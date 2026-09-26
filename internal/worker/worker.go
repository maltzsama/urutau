package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
)

// Config tunes batch accumulation.
type Config struct {
	// MaxRows flushes the batch once this many changes are buffered.
	// Byte-based triggers arrive with arrow sizing.
	MaxRows int
	// MaxInterval flushes whatever is buffered on this cadence.
	MaxInterval time.Duration
	// MetricsAddr serves /metrics (Prometheus); empty disables it.
	MetricsAddr string
}

// OnCommit observes successful commits (bookkeeping, tests).
type OnCommit func(b *dataplane.Batch, rows int)

// OnDroppedDelete observes a DELETE that append-only dropped: either the
// table declared onDelete: skip, or onDelete: record could not because the
// source carried no before image for that message.
type OnDroppedDelete func(table, pos string)

// OnStaged ships a staged write's descriptor (WK-001 C5): the batch's data
// files were written but not committed, and the descriptor must reach the
// coordinator, which commits the whole cycle. seq is the cycle key (0 for a
// worker-generated snapshot/window batch, committed on arrival). The error
// matters: a dropped descriptor leaves the cycle uncommittable, so the
// worker must fail rather than report a successful stage.
type OnStaged func(table string, seq uint64, descriptor []byte, pos, state string, pending []uint32) error

type Worker struct {
	cfg             Config
	onCommit        OnCommit
	onStaged        OnStaged
	onDroppedDelete OnDroppedDelete
	schemaDrift     func(SchemaDrift)
	tables          map[string]*tablePipeline
	metrics         *observability.Metrics
}

type tablePipeline struct {
	target    string
	committer sink.TableWriter
	mode      dataplane.WriteMode
	// staged is true when the coordinator owns this table's commit: the
	// worker stages each batch's data files and ships the descriptor, and
	// the coordinator commits the cycle (WK-001 C5). Requires committer to
	// implement sink.StagingWriter.
	staged bool
	ch     chan Ingest

	// appendDropDeletes implements onDelete: skip — every DELETE in an
	// append-only table is dropped (counted) instead of appended from its
	// before image.
	appendDropDeletes bool
	// droppedDeletes counts deletes dropped in append mode (skip or a
	// record that had no before image). Read cross-goroutine.
	droppedDeletes atomic.Int64

	// enricher rewrites rows with reference-table columns before buffering
	// (nil when the table declares no enrich). enrichDropped counts
	// inner-join misses the stage discarded. Read cross-goroutine.
	enricher      Enricher
	enrichDropped atomic.Int64

	// readyCh carries collapsed batches from the batcher to the committer.
	// This decouples collapse (CPU) from commit (I/O): while batch N is
	// committing, batch N+1 collapses concurrently.
	readyCh chan readyBatch

	// DBLog snapshot windows, per design: each chunk's SELECT rows land in
	// their own window (AddWindowRows); live events tagged InWindow mark
	// their key touched in every open window; the chunk's Closes marker
	// emits the stored batch minus touched rows. Windows are keyed by
	// chunkID because the orchestrator may populate chunk N+1 while the
	// batcher is still draining chunk N's buffered release. Guarded by
	// winMu. The window OWNS its batch; Closes consumes and releases it.
	winMu   sync.Mutex
	windows map[uint32]*snapshotWindow
	dropped int64

	// Snapshot state for resumable backfill. The snapshot state machine
	// (not_started -> in_progress -> complete) and the pending chunk list
	// are persisted atomically with position via the batch properties.
	snapshotMu      sync.Mutex
	snapshotState   string   // "not_started", "in_progress", "complete"
	snapshotPending []uint32 // chunk IDs still to process

	// bootstrapGuard tracks PKs touched by live events during the snapshot
	// phase. Snapshot lines whose PK was never touched are written as pure
	// appends (no equality delete), halving the write volume for initial
	// backfill. Released when snapshot completes.
	//
	// snapshotResumed marks a snapshot that resumed from persisted state.
	// The guard is recreated empty on resume, so it no longer knows which
	// keys live events touched before the crash — keys in chunks that are
	// still pending would otherwise be pure-appended on top of already
	// committed live rows. A resumed snapshot therefore disables the
	// optimization and writes every snapshot row through the safe upsert
	// path; only a fresh bootstrap pays nothing.
	snapshotResumed bool
	bootstrapGuard  *bloom.BloomFilter

	// knownSchema is the canonical schema known at introspection time. The
	// batcher checks every incoming change against it to detect schema
	// drift (ADD COLUMN, DROP COLUMN) — descending into struct columns so a
	// field added inside a nested payload pauses the table the same way a
	// top-level column would. An empty schema disables the check.
	knownSchema core.Schema
	// driftReported deduplicates schema-drift reports per column path, so
	// the callback fires once even when many changes carry the unknown
	// column.
	driftReported map[string]bool
}

// Ingest is one unit the worker consumes: a columnar batch plus optional
// window routing tags set by the demux (four-readers rule: routing lives
// at the port, consumed here, never on the record). A Closes marker carries
// no batch — only Win and Position (window rows adopt the position).
type Ingest struct {
	Table    string
	Batch    *dataplane.Batch
	Win      *rowchange.Window
	Position string
	// Seq and Staged are a Closes marker's cycle in the coordinator's send
	// order and its commit mode: the window's rows are delivered as that
	// cycle (zero for a marker that carries none).
	Seq    uint64
	Staged bool
	// SnapshotDone marks the end of the table's snapshot: every window was
	// sent ahead of it. The worker commits cdc.snapshot.state=complete after
	// them, at Position, as the marker's cycle on a staged table (#428).
	SnapshotDone bool
}

// New builds a worker; register tables before Run.
func New(cfg Config) *Worker {
	w := &Worker{
		cfg:    cfg,
		tables: make(map[string]*tablePipeline),
	}
	// The registry always exists: the coordinator records the worker's series
	// via the metrics report, even when this worker serves no /metrics endpoint.
	w.metrics = observability.New()
	if cfg.MetricsAddr != "" {
		go func() { _ = w.metrics.Serve(cfg.MetricsAddr, nil) }()
	}
	return w
}

// OnCommit installs the commit observer.
func (w *Worker) OnCommit(f OnCommit) { w.onCommit = f }

// OnStaged installs the staged-delivery observer (WK-001 C5). The remote
// layer must install one for a staged assignment: with no observer the
// descriptor is dropped and the cycle never commits.
func (w *Worker) OnStaged(f OnStaged) { w.onStaged = f }

// stage reports whether batch b is committed by the coordinator's staged
// cycle rather than directly. A live batch carries the coordinator's per-batch
// decision (BatchMeta.staged), which can flip under a re-slice: the surviving
// owner of a 1→N scale attached when the table was unpartitioned, so its
// p.staged is false, yet the coordinator now expects a staged delivery from
// it — and if it committed directly, the cycle it owes would stay open and
// block every cycle behind it in the table's send order (issue #312). A
// worker-generated snapshot/window batch (Seq 0) has no BatchMeta and keeps
// the attach-time mode, which never changes after boot because a re-slice
// does not re-snapshot.
func (p *tablePipeline) stage(b *dataplane.Batch) bool {
	if b.Seq == 0 {
		return p.staged
	}
	return b.Staged
}

// SetStaged marks a table's pipeline as staged (WK-001 C5). It refuses when
// the writer cannot stage, so a misconfigured assignment fails loudly instead
// of committing data the coordinator will never see.
func (w *Worker) SetStaged(target string) error {
	p := w.tables[target]
	if p == nil {
		return fmt.Errorf("worker: staged table %s not registered", target)
	}
	if _, ok := p.committer.(sink.StagingWriter); !ok {
		return fmt.Errorf("worker: staged table %s: writer does not implement StagingWriter", target)
	}
	// The staged delivery callback is required: without it the first batch
	// fails at stageBatch, not at boot (issue #269).
	if w.onStaged == nil {
		return fmt.Errorf("worker: staged table %s: no staged delivery callback set (OnStaged)", target)
	}
	p.staged = true
	return nil
}

// Register wires a per-table writer to a target table. The writer is any
// sink.TableWriter implementation — the worker knows nothing about the sink.
// The mode controls whether batches are collapsed (upsert) or
// passed through (append).
func (w *Worker) Register(target string, c sink.TableWriter, mode dataplane.WriteMode) {
	w.tables[target] = newTablePipeline(target, c, mode)
}

// RegisterCommitter wires a writer to a target table (test helper; same
// contract as Register).
func (w *Worker) RegisterCommitter(target string, c sink.TableWriter, mode dataplane.WriteMode) {
	w.tables[target] = newTablePipeline(target, c, mode)
}

// OnDroppedDelete installs the observer for deletes that append-only
// dropped (declared skip, or a record with no before image).
func (w *Worker) OnDroppedDelete(f OnDroppedDelete) { w.onDroppedDelete = f }

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
	return &tablePipeline{
		target:         target,
		committer:      c,
		mode:           mode,
		ch:             make(chan Ingest, 1024),
		readyCh:        make(chan readyBatch, 1),
		windows:        map[uint32]*snapshotWindow{},
		bootstrapGuard: bloom.NewWithEstimates(100_000, 0.01),
		driftReported:  map[string]bool{},
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

// OnSchemaDrift installs a callback for schema drift detection.
func (w *Worker) OnSchemaDrift(f func(SchemaDrift)) {
	w.schemaDrift = f
}

// Run routes ingest to the per-table pipelines until the channel closes and
// every pipeline has flushed its remainder. A commit failure is terminal:
// the worker stops — a failed batch is never skipped, because skipping
// would let later batches advance the position past uncommitted data.
func (w *Worker) Run(ctx context.Context, ingest <-chan Ingest) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, len(w.tables))
	var wg sync.WaitGroup
	for _, p := range w.tables {
		wg.Add(1)
		go func(p *tablePipeline) {
			defer wg.Done()
			errCh <- w.runPipeline(ctx, p)
		}(p)
	}

	// Router: dispatch by target table, then close pipelines so they flush.
	go func() {
		for ing := range ingest {
			table := ing.Table
			if ing.Batch != nil {
				table = ing.Batch.Table
			}
			p, ok := w.tables[table]
			if !ok {
				// A change for an unregistered table is a routing bug
				// upstream; dropping it silently would lose data. Cancel the
				// world — the pipelines surface their errors.
				cancel()
				return
			}
			select {
			case p.ch <- ing:
			case <-ctx.Done():
				return
			}
		}
		for _, p := range w.tables {
			close(p.ch)
		}
	}()

	var errs []error
	for range w.tables {
		if err := <-errCh; err != nil {
			errs = append(errs, err)
			cancel()
		}
	}
	return errors.Join(errs...)
}

func (w *Worker) runPipeline(ctx context.Context, p *tablePipeline) error {
	// The committer goroutine is the serialization point: it reads
	// prepared batches from readyCh and commits them one at a time.
	// While batch N commits (catalog round-trips, S3 writes), batch
	// N+1 collapses concurrently in the batcher goroutine below.
	//
	// A committer failure cancels the pipeline: the committer stops
	// reading readyCh, so without the cancel the batcher would block on
	// its next send forever, and the worker would go silent (no ack, no
	// error) until the coordinator declared it stalled.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errCh := make(chan error, 1)
	go func() {
		err := w.runCommitter(ctx, p)
		if err != nil {
			cancel(err)
		}
		errCh <- err
	}()

	// Batcher goroutine: collects changes, collapses, sends to readyCh.
	err := w.runBatcher(ctx, p)
	close(p.readyCh) // signal committer to drain and exit
	if err != nil {
		// A batcher failure cancels too: a commit in flight that only
		// returns on cancellation must not keep the pipeline from ending.
		cancel(err)
	}
	cerr := <-errCh
	if cerr != nil {
		// Release what the committer never took.
		for rb := range p.readyCh {
			rb.batch.Release()
		}
	}
	if err != nil && cerr != nil {
		// Both sides failed; one is the other's context-canceled echo.
		// The cancel cause is whichever failed first.
		return context.Cause(ctx)
	}
	if cerr != nil {
		return cerr
	}
	return err
}
