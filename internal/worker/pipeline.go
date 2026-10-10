package worker

import (
	"context"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
)

// Enricher rewrites a columnar batch with reference-table columns before it
// is buffered. The seam is columnar (CR-069 §3.4): *dataplane.Batch in,
// *dataplane.Batch out. A nil output means every row was dropped by an inner
// join. Implemented by internal/enrich.Stage; the interface keeps the worker
// free of the reference-join machinery.
type Enricher interface {
	EnrichBatch(ctx context.Context, b *dataplane.Batch, primaryKey []string) (*dataplane.Batch, error)
}

// SetEnricher installs the enrichment stage for a target table. Nil (the
// default) keeps the pass-through path byte-identical to a pipeline
// without enrich.
func (w *Worker) SetEnricher(target string, e Enricher) {
	if p := w.tables[target]; p != nil {
		p.enricher = e
	}
}

// EnrichDropped reports how many events the enrichment stage discarded
// (inner-join misses).
func (w *Worker) EnrichDropped(target string) int64 {
	if p := w.tables[target]; p != nil {
		return p.enrichDropped.Load()
	}
	return 0
}

// SetDropDeletes implements onDelete: skip — deletes in an append-only
// table are dropped and counted, never appended from a before image.
func (w *Worker) SetDropDeletes(target string, drop bool) {
	if p := w.tables[target]; p != nil {
		p.appendDropDeletes = drop
	}
}

// DroppedDeletes reports how many deletes append-only dropped for a table.
func (w *Worker) DroppedDeletes(target string) int64 {
	if p := w.tables[target]; p != nil {
		return p.droppedDeletes.Load()
	}
	return 0
}

func newTablePipeline(target string, c sink.TableWriter, mode dataplane.WriteMode) *tablePipeline {
	// bootstrapGuard is created lazily by SetSnapshotState(in_progress): a
	// discovery pipeline with a thousand tables would otherwise allocate
	// ~120 MB of bloom filters no snapshot ever uses (issue #578).
	return &tablePipeline{
		target:        target,
		committer:     c,
		mode:          mode,
		ch:            make(chan Ingest, 1024),
		readyCh:       make(chan readyBatch, 1),
		windows:       map[uint64]*snapshotWindow{},
		winClosed:     make(chan struct{}, 1),
		driftReported: map[string]bool{},
	}
}

// DroppedByWindow reports how many snapshot rows the DBLog window discarded
// because a live event won. It is the evidence the window worked.
func (w *Worker) DroppedByWindow(target string) int64 {
	p := w.tables[target]
	if p == nil {
		return 0
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	return p.dropped
}

// SetSnapshotState sets the snapshot progress for a target table. The
// batcher includes this state in every batch commit so it is persisted
// atomically with position. Snapshot completion releases the bloom filter
// to free memory; a later transition back to in_progress (a second
// snapshot run in the same process) recreates it, so the batcher never
// dereferences a nil guard.
func (w *Worker) SetSnapshotState(target string, state string, pending []uint32) {
	p := w.tables[target]
	if p == nil {
		return
	}
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshotState = state
	p.snapshotPending = pending
	switch state {
	case string(snapshot.StateComplete):
		p.bootstrapGuard = nil
		p.snapshotResumed = false
	case string(snapshot.StateInProgress):
		if p.bootstrapGuard == nil {
			p.bootstrapGuard = bloom.NewWithEstimates(100_000, 0.01)
		}
	}
}

// MarkSnapshotResumed tells the worker that the snapshot it is about to run
// resumed from persisted state rather than starting fresh. The bloom guard
// is recreated empty on resume, so the pure-append optimization is unsafe —
// keys live events touched before the crash would be duplicated by pending
// chunks. The batcher writes every snapshot row through the upsert path
// instead. Set by the runner when it detects in_progress state at boot.
func (w *Worker) MarkSnapshotResumed(target string) {
	p := w.tables[target]
	if p == nil {
		return
	}
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshotResumed = true
	p.bootstrapGuard = nil
}

// SetKnownSchema sets the canonical schema known at introspection time. The
// batcher uses it to detect schema drift (ADD COLUMN, DROP COLUMN) — an
// empty schema disables the check.
func (w *Worker) SetKnownSchema(target string, schema core.Schema) {
	p := w.tables[target]
	if p == nil {
		return
	}
	p.knownSchema = schema
}

// KnownSchema returns the canonical schema registered for a target table
// (empty when unset or unknown). Snapshot batch builders use it so window
// rows encode against the introspected shape, never a per-batch inference.
func (w *Worker) KnownSchema(target string) core.Schema {
	p := w.tables[target]
	if p == nil {
		return core.Schema{}
	}
	return p.knownSchema
}

// Metrics exposes the worker's registry so the remote layer
// (internal/worker/remote) can render and ship its series.
func (w *Worker) Metrics() *observability.Metrics { return w.metrics }
