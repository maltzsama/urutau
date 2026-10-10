package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/resume"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// resumeFrom resolves the run's resume point and the tables that must be
// snapshotted through the shared resume resolver (internal/resume, #718
// step 3). It reads cdc.position per target table; the minimum across tables
// is the resume point, tables without one need the snapshot — unless the
// source cannot snapshot at all (Kafka: Capabilities.Snapshot is false), in
// which case a table with no committed position simply starts streaming from
// the source's default, matching the collapsed runner's "skip when the source
// does not support snapshot" guard. Without that guard a fresh Kafka table
// would be routed into the snapshot phase and dereference the nil c.qsrc a
// non-snapshotting source boots without (issue #394).
//
// The expected partition count and the current owner NAMES travel to the sink
// on each ref: a per-partition Position() must not return a MinSafe over an
// incomplete owner set, or an owner with no committed position yet is resumed
// past — and an entry left by a retired owner (a scale-in) is ignored rather
// than pinning the minimum (§2.6).
func (c *Coordinator) resumeFrom(ctx context.Context, refs []source.TableRef) (position.Position, []source.TableRef, error) {
	caps, err := driver.CapsForKind(c.cfg.Spec.Source.Kind)
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: %w", err)
	}
	res, err := resume.Resolve(ctx, c.src, c.snk, refs, resume.Options{
		Caps: caps,
		Owners: func(target string) []string {
			ws := c.loadRouting().owners[target]
			names := make([]string, 0, len(ws))
			for _, w := range ws {
				names = append(names, w.name)
			}
			return names
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("coordinator: %w", err)
	}
	// bootCommitted is each table's committed position read at boot, for the
	// coveredAtBoot replay guard. Replaced only on a run with at least one
	// committed position; a fresh boot leaves the zero value (read-only after
	// boot, so a data race on the pump's covered check is impossible).
	if res.Resume != nil {
		c.bootCommitted = res.BootCommitted
	}
	// Streams ahead of the resume point replay from it (idempotent under
	// upsert); naming them makes a crash-recovery replay observable (#155).
	if len(res.Recovery) > 0 {
		c.log.Info("crash recovery: streams ahead of the resume point replay from it",
			"from", res.Resume.String(), "streams", res.Recovery)
	}
	return res.Resume, res.Snapshot, nil
}
