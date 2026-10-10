package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/sink"
)

// Config tunes batch accumulation.
type Config struct {
	// MaxRows flushes the batch once this many changes are buffered.
	MaxRows int
	// MaxBytes flushes once the buffered rows hold this many bytes (zero:
	// 32 MiB): rows carry payloads, and a row count alone OOM-killed (#437).
	MaxBytes int64
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
	onMarker        func(table string, id uint64)
	onStaged        OnStaged
	onDroppedDelete OnDroppedDelete
	schemaDrift     func(SchemaDrift)
	tables          map[string]*tablePipeline
	metrics         *observability.Metrics
	// metricsSrv is the /metrics server when MetricsAddr is set; stopped on
	// shutdown instead of leaked (issue #559).
	metricsSrv *http.Server
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
	windows map[uint64]*snapshotWindow
	dropped int64
	// winClosed signals (non-blocking) when a window closes, waking the chunk
	// reader's backpressure wait (issue #622).
	winClosed chan struct{}

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

// readyBatch is a collapsed batch ready for commit.
type readyBatch struct {
	batch   *dataplane.Batch
	rows    int // rows fed into the batcher for this flush
	upserts int // surviving upsert rows
	deletes int // equality-delete rows
	// ackPos, when set, is the position acked once the batch is committed,
	// in place of its watermark: a snapshot-done batch commits no position
	// but acks the marker's (#428).
	ackPos  []byte
	markers []uint64 // a batch-less item: see queueMarkers
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
	// SnapshotPending, on a Closes marker, is the table's snapshot chunks
	// still to do after this window: the window's rows commit with them as
	// cdc.snapshot.pending, state in_progress (issue #461).
	SnapshotPending []uint32
	// MarkerID, on a Closes marker, is the marker's batch id: the worker acks
	// it by this id once the window's rows are committed (#468).
	MarkerID uint64
}

// New builds a worker; register tables before Run.
func New(cfg Config) *Worker {
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 32 << 20
	}
	// A non-positive interval would panic time.NewTicker in runPipeline
	// (issue #559).
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 2 * time.Second
	}
	w := &Worker{
		cfg:    cfg,
		tables: make(map[string]*tablePipeline),
	}
	// The registry always exists: the coordinator records the worker's series
	// via the metrics report, even when this worker serves no /metrics endpoint.
	w.metrics = observability.New()
	w.startMetrics()
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

// Run routes ingest to the per-table pipelines until the channel closes and
// every pipeline has flushed its remainder. A commit failure is terminal:
// the worker stops — a failed batch is never skipped, because skipping
// would let later batches advance the position past uncommitted data.
func (w *Worker) Run(ctx context.Context, ingest <-chan Ingest) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer w.stopMetrics()

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
	routerErr := make(chan error, 1)
	go func() {
		for ing := range ingest {
			table := ing.Table
			if ing.Batch != nil {
				table = ing.Batch.Table
			}
			p, ok := w.tables[table]
			if !ok {
				// A change for an unregistered table is a routing bug upstream;
				// dropping it silently would lose data. Fail with the table
				// named (not a bare context canceled) and release the batch
				// (issue #559).
				if ing.Batch != nil {
					ing.Batch.Release()
				}
				routerErr <- fmt.Errorf("worker: change for unregistered table %q", table)
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
	select {
	case err := <-routerErr:
		errs = append(errs, err)
	default:
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
			rb.release()
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
