package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
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

type Worker struct {
	cfg             Config
	onCommit        OnCommit
	onDroppedDelete OnDroppedDelete
	schemaDrift     func(SchemaDrift)
	tables          map[string]*tablePipeline
	metrics         *observability.Metrics
}

type tablePipeline struct {
	target    string
	committer sink.TableWriter
	mode      dataplane.WriteMode
	ch        chan Ingest

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

// readyBatch is a collapsed batch ready for commit.
type readyBatch struct {
	batch   *dataplane.Batch
	rows    int // rows fed into the batcher for this flush
	upserts int // surviving upsert rows
	deletes int // equality-delete rows
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
}

// New builds a worker; register tables before Run.
func New(cfg Config) *Worker {
	w := &Worker{
		cfg:    cfg,
		tables: make(map[string]*tablePipeline),
	}
	if cfg.MetricsAddr != "" {
		w.metrics = observability.New()
		go func() { _ = w.metrics.Serve(cfg.MetricsAddr, nil) }()
	}
	return w
}

// OnCommit installs the commit observer.
func (w *Worker) OnCommit(f OnCommit) { w.onCommit = f }

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
	EnrichBatch(b *dataplane.Batch, primaryKey []string) (*dataplane.Batch, error)
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

// snapshotWindow is one DBLog chunk's SELECT rows, stored as the batch the
// snapshot source produced (no row decode). Live InWindow events mark keys
// in touched; Closes emits the batch minus the touched rows.
type snapshotWindow struct {
	batch   *dataplane.Batch
	touched map[string]struct{}
}

// AddWindowRows stores one chunk's SELECT batch for the snapshot window.
// The window TAKES OWNERSHIP of the batch; the Closes handler releases it.
func (w *Worker) AddWindowRows(target string, chunkID uint32, batch *dataplane.Batch) error {
	p, ok := w.tables[target]
	if !ok {
		batch.Release()
		return fmt.Errorf("worker: window rows for unregistered table %s", target)
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	if _, dup := p.windows[chunkID]; dup {
		batch.Release()
		return fmt.Errorf("worker: window rows: duplicate chunk %d for %s", chunkID, target)
	}
	p.windows[chunkID] = &snapshotWindow{batch: batch, touched: make(map[string]struct{})}
	return nil
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

// checkDrift compares an incoming row's columns against the known schema,
// recursing into struct columns. It returns the first drifted path (with its
// full dotted name, e.g. "address.complement") and whether drift was found.
func checkDrift(after map[string]any, schema core.Schema) (SchemaDrift, bool) {
	for name, v := range after {
		col, ok := schema.Column(name)
		if !ok {
			return SchemaDrift{Column: name, Kind: "added"}, true
		}
		if col.Type.Kind == core.KindStruct {
			if nested, isMap := v.(map[string]any); isMap {
				if d, hit := checkDriftNested(name, nested, col.Type.Fields); hit {
					return d, true
				}
			}
		}
	}
	return SchemaDrift{}, false
}

// checkDriftNested descends one level into a struct value, comparing its
// keys against the struct's declared fields.
func checkDriftNested(path string, m map[string]any, fields []core.Column) (SchemaDrift, bool) {
	for name, v := range m {
		full := path + "." + name
		var f *core.Column
		for i := range fields {
			if fields[i].Name == name {
				f = &fields[i]
				break
			}
		}
		if f == nil {
			return SchemaDrift{Column: full, Kind: "added"}, true
		}
		if f.Type.Kind == core.KindStruct {
			if nested, isMap := v.(map[string]any); isMap {
				return checkDriftNested(full, nested, f.Type.Fields)
			}
		}
	}
	return SchemaDrift{}, false
}

// SchemaDrift is emitted when the batcher detects a column in the change
// stream that was not present at introspection time.
type SchemaDrift struct {
	Table  string
	Column string
	Kind   string // "added", "removed"
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
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.runCommitter(ctx, p)
	}()

	// Batcher goroutine: collects changes, collapses, sends to readyCh.
	err := w.runBatcher(ctx, p)
	close(p.readyCh) // signal committer to drain and exit
	if err != nil {
		return err
	}
	return <-errCh
}

// runCommitter reads prepared batches and commits them serially.
// The batch is already columnar; the committer hands it straight to the
// sink (commit 3 — the worker's main path produces dataplane.Batch).
func (w *Worker) runCommitter(ctx context.Context, p *tablePipeline) error {
	for rb := range p.readyCh {
		start := time.Now()
		// A zero-value WriteMode would silently default to upsert semantics.
		// Every batch must carry an explicit mode; reject the unset one.
		if rb.batch.Mode == dataplane.ModeUnset {
			rb.batch.Release()
			return fmt.Errorf("worker: table %s: batch has no write mode set", p.target)
		}
		if w.onCommit != nil {
			w.onCommit(rb.batch, rb.rows)
		}
		if err := p.committer.Commit(ctx, rb.batch); err != nil {
			rb.batch.Release()
			if w.metrics != nil {
				w.metrics.CommitFailures.WithLabelValues(p.target).Inc()
			}
			return fmt.Errorf("worker: table %s: commit: %w", p.target, err)
		}
		rb.batch.Release()
		if w.metrics != nil {
			w.metrics.CommitDuration.WithLabelValues(p.target).Observe(time.Since(start).Seconds())
			w.metrics.RowsWritten.WithLabelValues(p.target, "upsert").Add(float64(rb.upserts))
			w.metrics.EqualityDeletes.WithLabelValues(p.target).Add(float64(rb.deletes))
		}
	}
	return nil
}

// runBatcher collects changes, collapses them, and sends ready batches to
// the committer. When the channel closes, the committer drains and exits.
func (w *Worker) runBatcher(ctx context.Context, p *tablePipeline) error {
	// The batcher is COLUMNAR (G2): pending holds owned wire batches, never
	// decoded rows. Per-row decisions (bootstrap marking, window dedup,
	// drift, append delete-image) read the record through a BatchReader and
	// mutate only side state (guard, windows, counters); the batches flow
	// through to the columnar flush untouched.
	var pending []*dataplane.Batch
	pendingRows := 0
	ticker := time.NewTicker(w.cfg.MaxInterval)
	defer ticker.Stop()

	// freePending releases every buffered batch (ownership returns here on
	// error paths).
	freePending := func() {
		for _, b := range pending {
			b.Release()
		}
		pending = nil
		pendingRows = 0
	}

	// ready sends a prepared batch to the committer (ownership transfers).
	ready := func(b *dataplane.Batch, rows, upserts, deletes int) error {
		select {
		case p.readyCh <- readyBatch{batch: b, rows: rows, upserts: upserts, deletes: deletes}:
			return nil
		case <-ctx.Done():
			b.Release()
			return ctx.Err()
		}
	}

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		merged, err := concatBatches(pending)
		if err != nil {
			freePending()
			return fmt.Errorf("worker: table %s: concat: %w", p.target, err)
		}
		rows := int(merged.Record.NumRows())
		pos := lastRowPos(merged)

		p.snapshotMu.Lock()
		inSnapshot := p.snapshotState == string(snapshot.StateInProgress)
		guard := p.bootstrapGuard
		resumed := p.snapshotResumed
		snapState := p.snapshotState
		snapPending := p.snapshotPending
		p.snapshotMu.Unlock()

		defer func() {
			merged.Release()
			freePending()
		}()

		// Snapshot partition: untouched snapshot PKs are pure-appended (no
		// equality delete); everything else collapses columnar.
		if inSnapshot && !resumed && p.mode == dataplane.UpsertMode {
			untouchedIdx, restIdx, err := partitionSnapshotRows(merged, guard, p.knownSchema.PrimaryKey)
			if err != nil {
				return err
			}
			appendPos := pos
			delCount := 0
			if len(restIdx) > 0 {
				appendPos = ""
			}
			if len(untouchedIdx) > 0 {
				ab, err := selectRows(merged, untouchedIdx, appendPos, dataplane.AppendMode, snapState, snapPending)
				if err != nil {
					return fmt.Errorf("worker: table %s: select append: %w", p.target, err)
				}
				if err := ready(ab, len(untouchedIdx), len(untouchedIdx), 0); err != nil {
					return err
				}
			}
			if len(restIdx) > 0 {
				rest, err := selectRows(merged, restIdx, pos, dataplane.UpsertMode, snapState, snapPending)
				if err != nil {
					return fmt.Errorf("worker: table %s: select rest: %w", p.target, err)
				}
				upCount, dCount, err := collapseAndSend(ctx, p, rest, rows, pos, ready)
				delCount = dCount
				if err != nil {
					return err
				}
				_ = upCount
				_ = delCount
			}
			return nil
		}

		switch p.mode {
		case dataplane.AppendMode:
			// Append: keep non-delete rows; delete rows are dropped when
			// onDelete is skip, or when they carry no image (a key-only
			// tombstone must never write an all-null row). A delete WITH an
			// image is kept — its row is the before image (DELETE IMAGE
			// CONTRACT: on the wire the image already sits in the data
			// columns) and the append sink writes it.
			keepIdx, err := appendRowsToKeep(ctx, p, w, merged)
			if err != nil {
				return err
			}
			out, err := selectRows(merged, keepIdx, pos, dataplane.AppendMode, snapState, snapPending)
			if err != nil {
				return fmt.Errorf("worker: table %s: select append: %w", p.target, err)
			}
			return ready(out, rows, len(keepIdx), 0)
		default:
			// Upsert: collapse the whole buffer columnar, merge survivors
			// into one batch (the position never separates from its data).
			upCount, delCount, err := collapseAndSend(ctx, p, merged, rows, pos, ready)
			if err != nil {
				return err
			}
			_ = upCount
			_ = delCount
			return nil
		}
	}

	for {
		select {
		case ing, ok := <-p.ch:
			if !ok {
				return flush()
			}
			// Closes marker: emit the chunk's remaining window rows (the
			// stored snapshot batch minus the keys live events touched),
			// adopting the marker's position.
			if ing.Win != nil && ing.Win.Closes {
				if err := closeWindow(p, ing, &pending, &pendingRows); err != nil {
					return err
				}
				continue
			}
			if ing.Batch == nil {
				continue
			}
			batch := ing.Batch
			if batch.Record == nil || batch.Record.NumRows() == 0 {
				batch.Release()
				continue
			}
			// Schema drift, columnar: any data column the batch carries that
			// the known schema lacks is a spec violation — report once and
			// go terminal. Runs on the SOURCE batch before enrich (enrich
			// adds reference columns that must not trip drift).
			if len(p.knownSchema.Columns) > 0 {
				d, hit, err := schemaDrift(batch, p.knownSchema)
				if err != nil {
					batch.Release()
					return fmt.Errorf("worker: table %s: %w", p.target, err)
				}
				if hit {
					p.snapshotMu.Lock()
					first := !p.driftReported[d.Column]
					p.driftReported[d.Column] = true
					p.snapshotMu.Unlock()
					if first && w.schemaDrift != nil {
						w.schemaDrift(SchemaDrift{Table: p.target, Column: d.Column, Kind: d.Kind})
					}
					batch.Release()
					return fmt.Errorf("worker: table %s: schema drift: column %q is not in the spec — declare it and resume", p.target, d.Column)
				}
			}
			// Enrich the whole batch (columnar seam). Deletes bypass the
			// join, so no single-row special case is needed.
			origRows := int(batch.Record.NumRows())
			if p.enricher != nil {
				enriched, err := p.enricher.EnrichBatch(batch, p.knownSchema.PrimaryKey)
				batch.Release()
				if err != nil {
					return fmt.Errorf("worker: table %s: enrich: %w", p.target, err)
				}
				if enriched == nil {
					p.enrichDropped.Add(int64(origRows))
					continue
				}
				p.enrichDropped.Add(int64(origRows - int(enriched.Record.NumRows())))
				batch = enriched
			}

			// Per-row side effects, read columnar: bootstrap marking for
			// live keys, InWindow dedup against the open windows.
			if err := markBatchSideEffects(p, batch, ing); err != nil {
				batch.Release()
				return err
			}

			pending = append(pending, batch)
			pendingRows += int(batch.Record.NumRows())
			if w.cfg.MaxRows > 0 && pendingRows >= w.cfg.MaxRows {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			freePending()
			return ctx.Err()
		}
	}
}

// collapseAndSend collapses one batch columnar and sends the merged single
// batch to the committer.
func collapseAndSend(ctx context.Context, p *tablePipeline, b *dataplane.Batch, rows int, pos string, ready func(*dataplane.Batch, int, int, int) error) (upCount, delCount int, err error) {
	p.snapshotMu.Lock()
	snapState := p.snapshotState
	snapPending := p.snapshotPending
	p.snapshotMu.Unlock()
	upserts, deletes, cerr := dpint.Collapse(ctx, nil, b, p.knownSchema.PrimaryKey)
	if cerr != nil {
		return 0, 0, fmt.Errorf("worker: table %s: collapse: %w", p.target, cerr)
	}
	defer func() {
		if upserts != nil {
			upserts.Release()
		}
		if deletes != nil {
			deletes.Release()
		}
	}()
	if upserts != nil {
		upCount = int(upserts.Record.NumRows())
	}
	if deletes != nil {
		delCount = int(deletes.Record.NumRows())
	}
	combined, err := mergeBatches(upserts, deletes, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("worker: table %s: merge: %w", p.target, err)
	}
	if combined == nil {
		return 0, 0, nil
	}
	combined.Watermark = []byte(pos)
	combined.Mode = p.mode
	combined.SnapshotState = snapState
	combined.SnapshotPending = snapPending
	if err := ready(combined, rows, upCount, delCount); err != nil {
		return 0, 0, err
	}
	return upCount, delCount, nil
}

// closeWindow emits the stored chunk batch minus the keys live InWindow
// events touched, with every row's __pos adopted to the marker position.
func closeWindow(p *tablePipeline, ing Ingest, pending *[]*dataplane.Batch, pendingRows *int) error {
	p.winMu.Lock()
	win := p.windows[ing.Win.ChunkID]
	if win == nil {
		p.winMu.Unlock()
		return nil
	}
	delete(p.windows, ing.Win.ChunkID)
	p.winMu.Unlock()

	if win.batch.Record == nil || win.batch.Record.NumRows() == 0 {
		win.batch.Release()
		return nil
	}
	reader, err := transport.NewBatchReader(win.batch.Record, p.knownSchema.PrimaryKey)
	if err != nil {
		win.batch.Release()
		return fmt.Errorf("worker: window %d: %w", ing.Win.ChunkID, err)
	}
	// Keep every row whose key was not touched by a live InWindow event.
	var keepIdx []int32
	for i := range reader.NumRows() {
		k := rowchange.KeyString(reader.Key(i))
		if _, hit := win.touched[k]; !hit {
			keepIdx = append(keepIdx, int32(i))
		}
	}
	if len(keepIdx) == 0 {
		win.batch.Release()
		return nil
	}
	sel, err := selectRows(win.batch, keepIdx, "", dataplane.AppendMode, "", nil)
	win.batch.Release() // the window is consumed; selectRows retained its columns
	if err != nil {
		return err
	}
	out, err := adoptWindowPos(sel, ing.Position)
	sel.Release()
	if err != nil {
		return err
	}
	*pending = append(*pending, out)
	*pendingRows += int(out.Record.NumRows())
	return nil
}

// adoptWindowPos rebuilds a batch with __pos replaced by a constant — the
// Closes rows adopt the marker's position (the safe resume point).
func adoptWindowPos(b *dataplane.Batch, pos string) (*dataplane.Batch, error) {
	rec := b.Record
	posIdx := -1
	for i := range rec.Schema().NumFields() {
		if rec.Schema().Field(i).Name == "__pos" {
			posIdx = i
			break
		}
	}
	if posIdx < 0 {
		return nil, fmt.Errorf("worker: window batch has no __pos column")
	}
	bld := array.NewStringBuilder(memory.DefaultAllocator)
	defer bld.Release()
	for range int(rec.NumRows()) {
		bld.Append(pos)
	}
	posArr := bld.NewStringArray()
	defer posArr.Release()

	cols := make([]arrow.Array, rec.NumCols())
	for i := range int(rec.NumCols()) {
		if i == posIdx {
			posArr.Retain()
			cols[i] = posArr
		} else {
			rec.Column(i).Retain()
			cols[i] = rec.Column(i)
		}
	}
	newRec := array.NewRecordBatch(rec.Schema(), cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return &dataplane.Batch{Table: b.Table, Record: newRec, Watermark: []byte(pos), Mode: dataplane.AppendMode}, nil
}

// markBatchSideEffects applies the per-row, side-effect-only decisions for a
// live batch: bootstrap-guard marking (a live key during snapshot takes the
// upsert path) and InWindow dedup (a live key removes its snapshot row from
// every open window). Pure reads of the record; the batch itself is not
// modified.
func markBatchSideEffects(p *tablePipeline, batch *dataplane.Batch, ing Ingest) error {
	reader, err := transport.NewBatchReader(batch.Record, p.knownSchema.PrimaryKey)
	if err != nil {
		return fmt.Errorf("worker: table %s: %w", p.target, err)
	}
	inWindow := ing.Win != nil && ing.Win.InWindow
	for i := range reader.NumRows() {
		key := reader.Key(i)
		if len(key) == 0 {
			continue
		}
		k := rowchange.KeyString(key)
		if !reader.Snapshot(i) {
			p.snapshotMu.Lock()
			if p.snapshotState == string(snapshot.StateInProgress) && p.bootstrapGuard != nil {
				p.bootstrapGuard.AddString(k)
			}
			p.snapshotMu.Unlock()
		}
		if inWindow {
			p.winMu.Lock()
			for _, win := range p.windows {
				if _, hit := win.touched[k]; hit {
					continue
				}
				// Only touch a key the window actually holds.
				if rowHoldsKey(win, k, p.knownSchema.PrimaryKey) {
					win.touched[k] = struct{}{}
					p.dropped++
				}
			}
			p.winMu.Unlock()
		}
	}
	return nil
}

// rowHoldsKey reports whether the stored window batch holds a row with the
// given key string.
func rowHoldsKey(win *snapshotWindow, key string, pk []string) bool {
	reader, err := transport.NewBatchReader(win.batch.Record, pk)
	if err != nil {
		return false
	}
	for i := range reader.NumRows() {
		if rowchange.KeyString(reader.Key(i)) == key {
			return true
		}
	}
	return false
}

// partitionSnapshotRows splits a merged batch's row indices into untouched
// snapshot PKs (pure append) and the rest.
func partitionSnapshotRows(b *dataplane.Batch, guard *bloom.BloomFilter, pk []string) (untouched, rest []int32, err error) {
	reader, err := transport.NewBatchReader(b.Record, pk)
	if err != nil {
		return nil, nil, err
	}
	for i := range reader.NumRows() {
		if reader.Snapshot(i) {
			key := reader.Key(i)
			if len(key) > 0 && !guard.TestString(rowchange.KeyString(key)) {
				untouched = append(untouched, int32(i))
				continue
			}
		}
		rest = append(rest, int32(i))
	}
	return untouched, rest, nil
}

// appendRowsToKeep returns the row indices an append-mode flush keeps:
// every non-delete row, plus delete rows that carry a real before image (a
// non-PK data column set). Dropped deletes are counted and reported.
func appendRowsToKeep(ctx context.Context, p *tablePipeline, w *Worker, b *dataplane.Batch) ([]int32, error) {
	reader, err := transport.NewBatchReader(b.Record, p.knownSchema.PrimaryKey)
	if err != nil {
		return nil, fmt.Errorf("worker: table %s: %w", p.target, err)
	}
	nonPK := make([]string, 0, len(reader.DataColumns()))
	for _, name := range reader.DataColumns() {
		isPK := false
		for _, pkn := range p.knownSchema.PrimaryKey {
			if pkn == name {
				isPK = true
				break
			}
		}
		if !isPK {
			nonPK = append(nonPK, name)
		}
	}
	var keep []int32
	for i := range reader.NumRows() {
		if reader.Op(i) != rowchange.OpDelete {
			keep = append(keep, int32(i))
			continue
		}
		// A delete: drop on skip; else keep only if it carries an image.
		if p.appendDropDeletes || !rowHasImage(reader, nonPK, i) {
			p.droppedDeletes.Add(1)
			if w.metrics != nil {
				w.metrics.DeletesDropped.WithLabelValues(p.target).Inc()
			}
			if w.onDroppedDelete != nil {
				w.onDroppedDelete(p.target, reader.Position(i))
			}
			continue
		}
		keep = append(keep, int32(i))
	}
	return keep, nil
}

// rowHasImage reports whether the row carries a value in a non-PK column —
// a delete with an image is appendable; a key-only tombstone is not.
func rowHasImage(r *transport.BatchReader, nonPK []string, i int) bool {
	for _, name := range nonPK {
		v, ok := r.Value(name, i)
		if ok && v != nil {
			return true
		}
	}
	return false
}

// schemaDrift returns the first data column the batch carries a VALUE in
// that the known schema lacks. An all-null extra column is a padding
// artifact of schema merging (e.g. a reference column declared but not yet
// populated), not a source column; only a column the batch actually
// populates counts as drift.
func schemaDrift(b *dataplane.Batch, schema core.Schema) (SchemaDrift, bool, error) {
	rec := b.Record
	for i := range rec.Schema().NumFields() {
		name := rec.Schema().Field(i).Name
		if isMetadataName(name) {
			continue
		}
		if _, ok := schema.Column(name); ok {
			continue
		}
		col := rec.Column(i)
		if col.IsNull(0) {
			continue // padding artifact — no source value, no drift
		}
		return SchemaDrift{Column: name, Kind: "added"}, true, nil
	}
	return SchemaDrift{}, false, nil
}

// isMetadataName reports the reserved wire metadata columns.
func isMetadataName(name string) bool {
	switch name {
	case "__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot", "__phase":
		return true
	}
	return false
}

// lastRowPos returns the __pos of the batch's last row (its commit
// coordinate), or "" for an empty batch.
func lastRowPos(b *dataplane.Batch) string {
	if b.Record == nil || b.Record.NumRows() == 0 {
		return ""
	}
	reader, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		return ""
	}
	return reader.Position(reader.NumRows() - 1)
}

// concatBatches concatenates the row lists of the given batches, in order,
// into one owned batch. The input batches are NOT released. All batches must
// share the same record schema (columns in the same order) — a source that
// emits schema-varying batches (per-drain inference) fails loud here instead
// of corrupting column alignment.
func concatBatches(bs []*dataplane.Batch) (*dataplane.Batch, error) {
	if len(bs) == 0 {
		return nil, nil
	}
	first := bs[0].Record.Schema()
	for _, b := range bs[1:] {
		sch := b.Record.Schema()
		if !sameSchema(sch, first) {
			return nil, fmt.Errorf("worker: concat: schema mismatch: %s vs %s (source batches must share a stable schema)", colsOf(sch), colsOf(first))
		}
	}
	acc, err := mergeBatches(bs[0], nil, nil)
	if err != nil {
		return nil, err
	}
	for _, b := range bs[1:] {
		next, err := mergeBatches(acc, b, nil)
		acc.Release()
		if err != nil {
			return nil, err
		}
		acc = next
	}
	return acc, nil
}

// sameSchema reports field-for-field equality (names, types, order).
func sameSchema(a, b *arrow.Schema) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i := range a.NumFields() {
		af, bf := a.Field(i), b.Field(i)
		if af.Name != bf.Name || !arrow.TypeEqual(af.Type, bf.Type) {
			return false
		}
	}
	return true
}

func colsOf(s *arrow.Schema) string {
	names := make([]string, s.NumFields())
	for i := range s.NumFields() {
		names[i] = s.Field(i).Name
	}
	return strings.Join(names, ",")
}

// selectRows returns a new owned batch holding the rows at the given
// indices, with the given mode and position. The input is NOT released.
func selectRows(b *dataplane.Batch, idx []int32, pos string, mode dataplane.WriteMode, snapState string, snapPending []uint32) (*dataplane.Batch, error) {
	if len(idx) == 0 {
		return nil, nil
	}
	rec := b.Record
	ib := array.NewInt32Builder(memory.DefaultAllocator)
	for _, v := range idx {
		ib.Append(v)
	}
	idxArr := ib.NewInt32Array()
	defer idxArr.Release()

	cols := make([]arrow.Array, rec.NumCols())
	for i := range int(rec.NumCols()) {
		t, err := compute.TakeArray(context.Background(), rec.Column(i), idxArr)
		if err != nil {
			for j := range i {
				cols[j].Release()
			}
			return nil, err
		}
		cols[i] = t
	}
	newRec := array.NewRecordBatch(rec.Schema(), cols, int64(len(idx)))
	for _, c := range cols {
		c.Release()
	}
	return &dataplane.Batch{
		Table:           b.Table,
		Record:          newRec,
		Watermark:       []byte(pos),
		Mode:            mode,
		SnapshotState:   snapState,
		SnapshotPending: snapPending,
	}, nil
}

// countOps returns the number of upsert and delete rows in a batch by
// scanning its __op column. Used by OnCommit observers for bookkeeping.
func CountOps(b *dataplane.Batch) (upserts, deletes int) {
	if b == nil || b.Record == nil {
		return 0, 0
	}
	opIdx := -1
	schema := b.Record.Schema()
	for i := range schema.NumFields() {
		if schema.Field(i).Name == "__op" {
			opIdx = i
			break
		}
	}
	if opIdx < 0 {
		return int(b.Record.NumRows()), 0
	}
	opCol, ok := b.Record.Column(opIdx).(*array.Uint8)
	if !ok {
		return int(b.Record.NumRows()), 0
	}
	for i := range opCol.Len() {
		if opCol.Value(i) == uint8(rowchange.OpDelete) {
			deletes++
		} else {
			upserts++
		}
	}
	return upserts, deletes
}

// mergeBatches concatenates two same-schema batches (upserts + deletes)
// into a single batch. Returns nil when both inputs are empty. The caller
// owns the input batches; they are NOT released here.
func mergeBatches(a, b *dataplane.Batch, alloc memory.Allocator) (*dataplane.Batch, error) {
	// Ownership: the returned batch ALWAYS holds its own retained refs.
	// The caller owns a and b and releases them after this call. Never
	// return an input pointer directly — the caller's Release would free
	// the returned batch's record.
	if a == nil || a.Record == nil || a.Record.NumRows() == 0 {
		if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
			return nil, nil
		}
		b.Record.Retain()
		return &dataplane.Batch{Table: b.Table, Record: b.Record, Watermark: b.Watermark,
			Mode: b.Mode, SnapshotState: b.SnapshotState, SnapshotPending: b.SnapshotPending}, nil
	}
	if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		a.Record.Retain()
		return &dataplane.Batch{Table: a.Table, Record: a.Record, Watermark: a.Watermark,
			Mode: a.Mode, SnapshotState: a.SnapshotState, SnapshotPending: a.SnapshotPending}, nil
	}
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	schema := a.Record.Schema()
	ncols := int(schema.NumFields())
	nrows := a.Record.NumRows() + b.Record.NumRows()
	cols := make([]arrow.Array, ncols)
	for i := range ncols {
		cat, err := array.Concatenate([]arrow.Array{a.Record.Column(i), b.Record.Column(i)}, alloc)
		if err != nil {
			for j := range i {
				cols[j].Release()
			}
			return nil, err
		}
		cols[i] = cat
	}
	rec := array.NewRecordBatch(schema, cols, nrows)
	for _, c := range cols {
		c.Release()
	}
	return &dataplane.Batch{
		Table:           a.Table,
		Record:          rec,
		Watermark:       a.Watermark,
		Mode:            a.Mode,
		SnapshotState:   a.SnapshotState,
		SnapshotPending: a.SnapshotPending,
	}, nil
}
