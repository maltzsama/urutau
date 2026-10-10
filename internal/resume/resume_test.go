package resume

// The shared resume resolver's coverage, moved from the collapsed runner
// (internal/runner) and extended for the distributed coordinator's inputs:
// the snapshot-vs-adopt split, the Kafka capability guard (issue #394), the
// bootstrap adopt/startAt handling, and the partition-owner metadata.

import (
	"context"
	"errors"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

type parseSource struct{}

func (parseSource) ParsePosition(s string) (position.Position, error) {
	return position.MustLSN(s), nil
}

type badSource struct{}

func (badSource) ParsePosition(string) (position.Position, error) {
	return nil, errors.New("bad lsn")
}

type mapSink struct {
	positions map[string]string
	props     map[string]map[string]string
	posErr    error
	propsErr  error
	seen      []core.TableRef
}

func (s *mapSink) Position(_ context.Context, ref core.TableRef) (string, error) {
	s.seen = append(s.seen, ref)
	if s.posErr != nil {
		return "", s.posErr
	}
	return s.positions[ref.Target], nil
}

func (s *mapSink) Properties(_ context.Context, ref core.TableRef) (map[string]string, error) {
	if s.propsErr != nil {
		return nil, s.propsErr
	}
	return s.props[ref.Target], nil
}

var snapCap = Options{Caps: source.Capabilities{Snapshot: true}}

func targets(refs []source.TableRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Target)
	}
	return out
}

func TestResolve(t *testing.T) {
	ctx := context.Background()
	refs := []source.TableRef{{Target: "a"}, {Target: "b"}}

	// All committed: MinSafe of the two, nothing to snapshot. Stream b is
	// ahead of the resume point, so it is the crash-recovery replay set.
	snk := &mapSink{positions: map[string]string{"a": "0/10", "b": "0/20"}}
	res, err := Resolve(ctx, parseSource{}, snk, refs, snapCap)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Snapshot) != 0 {
		t.Fatalf("Snapshot = %v, want none", targets(res.Snapshot))
	}
	if res.Resume == nil || res.Resume.String() != "0/10" {
		t.Fatalf("Resume = %v, want 0/10", res.Resume)
	}
	if len(res.Recovery) != 1 || res.Recovery[0] != "b" {
		t.Fatalf("Recovery = %v, want [b]", res.Recovery)
	}
	if len(res.BootCommitted) != 2 || res.BootCommitted["a"].String() != "0/10" {
		t.Fatalf("BootCommitted = %v, want both parsed", res.BootCommitted)
	}

	// One uncommitted: it needs a snapshot, the other sets the resume.
	snk = &mapSink{positions: map[string]string{"a": "0/10"}}
	res, err = Resolve(ctx, parseSource{}, snk, refs, snapCap)
	if err != nil {
		t.Fatalf("Resolve(partial): %v", err)
	}
	if got := targets(res.Snapshot); len(got) != 1 || got[0] != "b" {
		t.Fatalf("Snapshot = %v, want [b]", got)
	}
	if res.Resume == nil || res.Resume.String() != "0/10" {
		t.Fatalf("Resume = %v, want 0/10", res.Resume)
	}

	// Nothing committed: no resume, everything needs a snapshot.
	snk = &mapSink{positions: map[string]string{}}
	res, err = Resolve(ctx, parseSource{}, snk, refs, snapCap)
	if err != nil {
		t.Fatalf("Resolve(fresh): %v", err)
	}
	if res.Resume != nil || len(res.Snapshot) != 2 {
		t.Fatalf("resume/snapshot = %v, %v; want nil, both", res.Resume, targets(res.Snapshot))
	}

	// A sink read error propagates.
	snk = &mapSink{posErr: errors.New("catalog down")}
	if _, err := Resolve(ctx, parseSource{}, snk, refs, snapCap); err == nil {
		t.Fatal("a Position error must propagate")
	}

	// A bad stored position propagates.
	snk = &mapSink{positions: map[string]string{"a": "0/10", "b": "0/20"}}
	if _, err := Resolve(ctx, badSource{}, snk, refs, snapCap); err == nil {
		t.Fatal("a ParsePosition error must propagate")
	}
}

// A table the stream committed to while its snapshot was unfinished holds a
// position; a crash there must not take it for a finished snapshot (#428).
func TestResolveSnapshotsUnfinished(t *testing.T) {
	snk := &mapSink{
		positions: map[string]string{"queued": "0/10", "left": "0/20", "done": "0/30", "legacy": "0/40"},
		props: map[string]map[string]string{
			"queued": {snapshot.PropSnapshotState: string(snapshot.StateNotStarted)},
			"left":   {snapshot.PropSnapshotState: string(snapshot.StateInProgress)},
			"done":   {snapshot.PropSnapshotState: string(snapshot.StateComplete)},
		},
	}
	refs := []source.TableRef{{Target: "queued"}, {Target: "left"}, {Target: "done"}, {Target: "legacy"}, {Target: "fresh"}}
	res, err := Resolve(context.Background(), parseSource{}, snk, refs, snapCap)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := targets(res.Snapshot); len(got) != 3 || got[0] != "queued" || got[1] != "left" || got[2] != "fresh" {
		t.Fatalf("tables to snapshot = %v, want [queued left fresh]", got)
	}
	if res.Resume == nil || res.Resume.String() != "0/10" {
		t.Fatalf("Resume = %v, want 0/10", res.Resume)
	}
}

// A non-snapshotting source never routes a table into the snapshot phase
// (issue #394), so the coordinator cannot dereference the nil query
// connection a Kafka source boots with. It also never reads the snapshot
// state properties.
func TestResolveGatesSnapshotOnCapability(t *testing.T) {
	refs := []source.TableRef{{Target: "fresh"}, {Target: "committed"}}
	snk := &mapSink{
		positions: map[string]string{"committed": "0/10"},
		propsErr:  errors.New("must not be read"),
	}
	res, err := Resolve(context.Background(), parseSource{}, snk, refs, Options{})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res.Snapshot) != 0 || len(res.Adopt) != 0 {
		t.Fatalf("snapshot/adopt = %v/%v, want none for a non-snapshotting source", targets(res.Snapshot), targets(res.Adopt))
	}
	if res.Resume == nil || res.Resume.String() != "0/10" {
		t.Fatalf("Resume = %v, want 0/10", res.Resume)
	}
}

// A table whose bootstrap mode adopts existing data is reported in Adopt, not
// Snapshot; an explicit start position is parsed for the stream start.
func TestResolveAdoptAndExplicitStart(t *testing.T) {
	refs := []source.TableRef{{Target: "adopt"}, {Target: "verify"}, {Target: "fresh"}}
	res, err := Resolve(context.Background(), parseSource{}, &mapSink{positions: map[string]string{}}, refs, Options{
		Caps: source.Capabilities{Snapshot: true},
		Bootstrap: map[string]spec.Bootstrap{
			"adopt":  {Mode: spec.Adopt},
			"verify": {Mode: spec.AdoptVerify, StartAt: spec.StartAtExplicit, Position: "0/5"},
		},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got := targets(res.Adopt); len(got) != 2 || got[0] != "adopt" || got[1] != "verify" {
		t.Fatalf("Adopt = %v, want [adopt verify]", got)
	}
	if got := targets(res.Snapshot); len(got) != 1 || got[0] != "fresh" {
		t.Fatalf("Snapshot = %v, want [fresh]", got)
	}
	if res.Resume != nil {
		t.Fatalf("Resume = %v, want nil on a fresh boot", res.Resume)
	}
	if res.Start == nil || res.Start.String() != "0/5" {
		t.Fatalf("Start = %v, want the explicit 0/5", res.Start)
	}
	if len(res.Explicit) != 1 || res.Explicit[0].String() != "0/5" {
		t.Fatalf("Explicit = %v, want [0/5]", res.Explicit)
	}

	// A malformed explicit position is an error.
	_, err = Resolve(context.Background(), badSource{}, &mapSink{positions: map[string]string{}}, refs, Options{
		Bootstrap: map[string]spec.Bootstrap{"adopt": {Mode: spec.Adopt, StartAt: spec.StartAtExplicit, Position: "x"}},
	})
	if err == nil {
		t.Fatal("a bad bootstrap.position must error")
	}
}

// The coordinator's partition metadata reaches the sink's per-partition
// Position read on each ref.
func TestResolveSetsOwnerMetadata(t *testing.T) {
	refs := []source.TableRef{{Target: "orders"}}
	snk := &mapSink{positions: map[string]string{"orders": "0/10"}}
	owners := func(string) []string { return []string{"w0", "w1"} }
	if _, err := Resolve(context.Background(), parseSource{}, snk, refs, Options{
		Caps:   source.Capabilities{Snapshot: true},
		Owners: owners,
	}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(snk.seen) != 1 {
		t.Fatalf("Position called %d times, want 1", len(snk.seen))
	}
	ref := snk.seen[0]
	if ref.OwnerCount != 2 || len(ref.Owners) != 2 || ref.Owners[0] != "w0" || ref.Owners[1] != "w1" {
		t.Fatalf("owner metadata = count %d names %v, want 2 [w0 w1]", ref.OwnerCount, ref.Owners)
	}
}
