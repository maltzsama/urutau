// Package resume resolves where one pipeline resumes from and which tables it
// must snapshot or adopt — the shared boot step of the collapsed runner
// (internal/runner) and the distributed coordinator (internal/coordinator). It
// is step 3 of the shared collapsed/distributed core (issue #718, design #404).
//
// The resolver is mode-agnostic: the source reader and the sink are passed in
// as small interfaces, and the coordinator's partition-owner metadata and the
// runner's bootstrap blocks arrive as options. It may not import
// internal/runner or internal/coordinator — it is the shared leaf both depend
// on. It may import source, core, spec, position, driver (via its callers) and
// internal/snapshot.
package resume

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Reader is the source surface the resolver needs: decoding a stored
// cdc.position string.
type Reader interface {
	ParsePosition(s string) (position.Position, error)
}

// Sink is the sink surface the resolver needs: the per-table committed
// position and the table's snapshot state properties.
type Sink interface {
	Position(ctx context.Context, ref core.TableRef) (string, error)
	Properties(ctx context.Context, ref core.TableRef) (map[string]string, error)
}

// Options tunes one resolution.
type Options struct {
	// Caps is the source's declared capabilities. Snapshot gates whether a
	// table without a finished snapshot is routed into the snapshot phase: a
	// non-snapshotting source (Kafka) must never be, or it would boot with no
	// query connection and be dereferenced (issue #394).
	Caps source.Capabilities
	// Owners returns the current partition owner names for a target. Non-nil
	// in distributed mode: each ref's OwnerCount/Owners are set before the
	// per-partition Position read, so an entry left by a retired owner (a
	// scale-in) is ignored and an owner with no committed position is not
	// resumed past (WK-001 §2.6). Nil in the collapsed runner (single owner).
	Owners func(target string) []string
	// Bootstrap is the per-target bootstrap config (collapsed mode only). A
	// table whose Mode is adopt or adopt-verify is reported in Adopt rather
	// than Snapshot; a startAt: explicit position is parsed into Explicit. Nil
	// leaves every table on the snapshot path.
	Bootstrap map[string]spec.Bootstrap
}

// Result is the resolved boot layout both orchestration modes consume.
type Result struct {
	// Resume is the resume point: the MinSafe over every table's committed
	// position, nil on a fresh boot.
	Resume position.Position
	// Start is the source stream's start: Resume, or the minimum explicit
	// bootstrap position when Resume is nil. Nil means the caller falls back
	// to the source's InitialPosition.
	Start position.Position
	// Snapshot lists the tables that must run the snapshot loop, in ref
	// order: a table with no committed position, or one an earlier run left
	// unfinished (a position does not prove the snapshot finished, #428).
	Snapshot []source.TableRef
	// Adopt lists the tables whose existing data is adopted: a table that
	// would need a snapshot but declares bootstrap mode adopt/adopt-verify,
	// so its snapshot is marked complete without reading data.
	Adopt []source.TableRef
	// Explicit are the bootstrap startAt: explicit positions, already parsed.
	Explicit []position.Position
	// BootCommitted is each table's committed position parsed fresh at boot.
	// The MinSafe uses a separate parse, so a reader advancing a start in
	// place cannot move this "committed" position forward with the stream;
	// the coordinator's coveredAtBoot replays a table ahead of the minimum
	// using it.
	BootCommitted map[string]position.Position
	// Recovery names the streams ahead of the resume point: they replay from
	// it (idempotent under upsert), made observable for crash recovery
	// (#155).
	Recovery []string
}

// Resolve reads each target's committed cdc.position; the minimum across
// tables is the resume point. A table without a committed position — or one an
// earlier run left unfinished (#428) — needs the snapshot, unless the source
// cannot snapshot at all (Caps.Snapshot is false), in which case it is left
// out so it is never routed into the snapshot phase (issue #394). A table with
// bootstrap mode adopt/adopt-verify is reported in Adopt instead: its snapshot
// is marked complete without reading. An incomparable pair (should not happen
// for one source) is an error — guessing a minimum could resume past
// uncommitted data (P1).
func Resolve(ctx context.Context, src Reader, snk Sink, refs []source.TableRef, opts Options) (*Result, error) {
	res := &Result{BootCommitted: make(map[string]position.Position, len(refs))}
	adopt := adoptTargets(opts.Bootstrap)
	var positions []position.Position
	byTarget := make(map[string]position.Position, len(refs))
	for _, ref := range refs {
		if opts.Owners != nil {
			names := opts.Owners(ref.Target)
			ref.OwnerCount = len(names)
			ref.Owners = append([]string(nil), names...)
		}
		pos, err := snk.Position(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ref.Target, err)
		}
		if pos == "" {
			res.need(ref, opts.Caps, adopt)
			continue
		}
		p, err := src.ParsePosition(pos)
		if err != nil {
			return nil, fmt.Errorf("%s cdc.position %q: %w", ref.Target, pos, err)
		}
		positions = append(positions, p)
		byTarget[ref.Target] = p
		// bootCommitted gets its own parse, never an object that may reach the
		// source reader as its start: a reader advances its start in place,
		// which would race the coordinator's coveredAtBoot and move the
		// "committed" position forward with the stream.
		if own, err := src.ParsePosition(pos); err == nil {
			res.BootCommitted[ref.Target] = own
		}
		// A position does not prove the snapshot finished: the stream commits
		// to a table before and during its snapshot (#428).
		if !opts.Caps.Snapshot {
			continue
		}
		props, err := snk.Properties(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("%s: snapshot state: %w", ref.Target, err)
		}
		if snapshot.Unfinished(props) {
			res.need(ref, opts.Caps, adopt)
		}
	}
	if len(positions) == 0 {
		// A fresh boot: parse the explicit bootstrap positions and let them
		// seed the stream start; the caller falls back to InitialPosition
		// when there are none.
		if err := parseExplicit(src, opts.Bootstrap, res); err != nil {
			return nil, err
		}
		res.Start = explicitStart(res.Explicit)
		return res, nil
	}
	best, err := position.MinSafe(positions)
	if err != nil {
		return nil, err
	}
	res.Resume = best
	res.Start = best
	res.Recovery = position.Ahead(best, byTarget)
	if err := parseExplicit(src, opts.Bootstrap, res); err != nil {
		return nil, err
	}
	return res, nil
}

// adoptTargets is the set of targets whose bootstrap mode adopts existing
// data (adopt or adopt-verify).
func adoptTargets(bootstrap map[string]spec.Bootstrap) map[string]bool {
	adopt := make(map[string]bool, len(bootstrap))
	for target, b := range bootstrap {
		if b.Mode == spec.Adopt || b.Mode == spec.AdoptVerify {
			adopt[target] = true
		}
	}
	return adopt
}

// parseExplicit parses each table's bootstrap startAt: explicit position into
// res.Explicit. Table order is deterministic so the first malformed position
// reports a stable error.
func parseExplicit(src Reader, bootstrap map[string]spec.Bootstrap, res *Result) error {
	for _, target := range slices.Sorted(maps.Keys(bootstrap)) {
		b := bootstrap[target]
		if b.StartAt != spec.StartAtExplicit || b.Position == "" {
			continue
		}
		p, err := src.ParsePosition(b.Position)
		if err != nil {
			return fmt.Errorf("%s bootstrap.position %q: %w", target, b.Position, err)
		}
		res.Explicit = append(res.Explicit, p)
	}
	return nil
}

// need files ref into the snapshot or adopt list, gated on the source's
// snapshot capability.
func (r *Result) need(ref source.TableRef, caps source.Capabilities, adopt map[string]bool) {
	if !caps.Snapshot {
		return
	}
	if adopt[ref.Target] {
		r.Adopt = append(r.Adopt, ref)
		return
	}
	r.Snapshot = append(r.Snapshot, ref)
}

// explicitStart is the minimum explicit bootstrap position, or nil when there
// are none. The stream is one per source, so starting too late would skip
// data. Only consulted when there is no resume point.
func explicitStart(explicit []position.Position) position.Position {
	if len(explicit) == 0 {
		return nil
	}
	return position.Min(explicit)
}
