package snapshot

import (
	"context"

	"github.com/maltzsama/urutau/core"
)

// This file is the mode-agnostic prelude of the snapshot/adopt loop — step 4
// of the shared collapsed/distributed core (#718, design #404). The per-chunk
// sequence itself already lives in one place: SnapshotTable drives it over the
// Relay/reader interfaces, in process by the collapsed runner and over the
// wire by the coordinator's rendezvous-hash fan-out. What remains common to
// every path is the prelude: reading persisted progress, the "resume from the
// persisted chunk cursor" decision, and marking a snapshot (or an adopted
// table) complete.
//
// The distributed coordinator's range path keeps its own per-chunk loop: there
// the chunk SELECT runs on the worker and streams WindowOpen/ChunkReady back,
// a shape SnapshotTable's synchronous, single-batch-per-chunk body does not
// drive (see website/docs/architecture/orchestration.md, step 4). The prelude
// below is shared by all of them so the resume decision cannot drift.
//
// It depends on none of the sink library: Properties and PropsWriter are
// satisfied by sink.Sink, keeping this leaf inside the architecture wall
// (TestSourcesNeverKnowSinks).

// Properties reads a table's sink properties. Implemented by sink.Sink.
type Properties interface {
	Properties(ctx context.Context, ref core.TableRef) (map[string]string, error)
}

// PropsWriter writes a table's sink properties. Implemented by sink.Sink.
type PropsWriter interface {
	SetProperties(ctx context.Context, ref core.TableRef, props map[string]string) error
}

// Resumable is the one "resume from the persisted chunk cursor" decision:
// progress names an interrupted snapshot that carries the deterministic bounds
// it was chunked to, so its pending chunks are resumed instead of the bounds
// being recalculated over a table that has since changed. A caller with a
// stricter requirement (a matching partition layout, say) layers its own guard
// on top. Keeping the predicate here stops the collapsed runner and the
// coordinator's paths from drifting apart.
func Resumable(sp *SnapshotProgress) bool {
	return sp != nil && sp.State == StateInProgress && len(sp.Bounds) > 0
}

// ReadProgress reads a table's persisted snapshot progress from its
// properties.
func ReadProgress(ctx context.Context, p Properties, ref core.TableRef) (*SnapshotProgress, error) {
	props, err := p.Properties(ctx, ref)
	if err != nil {
		return nil, err
	}
	return ReadSnapshotProgress(props)
}

// MarkComplete records that a table's snapshot is complete: the adopt path,
// and the completion marker for a snapshot that had no rows to copy. It writes
// the same single property every mode writes today.
func MarkComplete(ctx context.Context, w PropsWriter, ref core.TableRef) error {
	props, err := EncodeSnapshotProgress(&SnapshotProgress{State: StateComplete})
	if err != nil {
		return err
	}
	return w.SetProperties(ctx, ref, props)
}
