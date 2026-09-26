package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// resumeFrom reads cdc.position per target table; the minimum across tables
// is the resume point, tables without one need the snapshot — unless the
// source cannot snapshot at all (Kafka: Capabilities.Snapshot is false),
// in which case a table with no committed position simply starts streaming
// from the source's default, matching the collapsed runner's "skip when the
// source does not support snapshot" guard (internal/runner/runner.go). The
// coordinator has no equivalent guard otherwise: a fresh Kafka table would be
// routed into the snapshot phase and dereference the nil c.qsrc a
// non-snapshotting source boots without (issue #394).
func (c *Coordinator) resumeFrom(ctx context.Context, refs []source.TableRef) (position.Position, []source.TableRef, error) {
	caps, err := driver.CapsForKind(c.cfg.Spec.Source.Kind)
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: %w", err)
	}
	var positions []position.Position
	var needsSnapshot []source.TableRef
	byTarget := make(map[string]position.Position, len(refs))
	// bootCommitted gets its own parse of each position, never an object
	// that may reach the source reader as its start: a reader advances its
	// start in place, which raced coveredAtBoot and moved the "committed"
	// position forward with the stream.
	boot := make(map[string]position.Position, len(refs))
	for _, ref := range refs {
		// The expected partition count travels to the sink: a per-partition
		// Position() must not return a MinSafe over an incomplete owner set,
		// or an owner with no committed position yet is resumed past (§2.6).
		ref.OwnerCount = len(c.loadRouting().owners[ref.Target])
		pos, err := c.snk.Position(ctx, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("coordinator: %s: %w", ref.Target, err)
		}
		if pos != "" {
			p, err := c.src.ParsePosition(pos)
			if err != nil {
				return nil, nil, fmt.Errorf("coordinator: %s cdc.position %q: %w", ref.Target, pos, err)
			}
			positions = append(positions, p)
			byTarget[ref.Target] = p
			if own, err := c.src.ParsePosition(pos); err == nil {
				boot[ref.Target] = own
			}
			// A position does not prove the snapshot finished: the stream
			// commits to a table before and during its snapshot (#428).
			if caps.Snapshot {
				interrupted, err := c.snapshotInterrupted(ctx, core.TableRef{Source: ref.Source, Target: ref.Target})
				if err != nil {
					return nil, nil, err
				}
				if interrupted {
					c.log.Warn("coordinator: snapshot interrupted by an earlier run; snapshotting again", "table", ref.Target, "position", pos)
					needsSnapshot = append(needsSnapshot, ref)
				}
			}
		} else if caps.Snapshot {
			needsSnapshot = append(needsSnapshot, ref)
		}
	}
	if len(positions) == 0 {
		return nil, needsSnapshot, nil
	}
	c.bootCommitted = boot
	// MinSafe: an incomparable pair (should not happen for one source) is an
	// error — guessing a minimum could resume past uncommitted data (P1).
	best, err := position.MinSafe(positions)
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: %w", err)
	}
	// Streams ahead of the resume point replay from it (idempotent under
	// upsert); naming them makes a crash-recovery replay observable (#155).
	if recovery := position.Ahead(best, byTarget); len(recovery) > 0 {
		c.log.Info("crash recovery: streams ahead of the resume point replay from it",
			"from", best.String(), "streams", recovery)
	}
	return best, needsSnapshot, nil
}
