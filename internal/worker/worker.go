package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	// their own window (AddWindowRows); live events tagged InWindow remove
	// their key from that chunk's window; the chunk's Closes marker flushes
	// what remains. Windows are keyed by chunkID because the orchestrator
	// may populate chunk N+1 while the batcher is still draining chunk N's
	// buffered release. Guarded by winMu: the runner (snapshot orchestrator)
	// populates windows while the batcher goroutine consumes events.
	winMu   sync.Mutex
	windows map[uint32]map[string]rowchange.Change
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
	EnrichBatch(b *dataplane.Batch) (*dataplane.Batch, error)
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
		windows:        map[uint32]map[string]rowchange.Change{},
		bootstrapGuard: bloom.NewWithEstimates(100_000, 0.01),
		driftReported:  map[string]bool{},
	}
}

// AddWindowRows feeds one chunk's SELECT result of a DBLog snapshot window
// into the table's batcher. The rows are held in that chunk's window until
// its Closes marker: a live event tagged InWindow discards its key (the live
// version wins). Chunks are independent — a previous chunk may still be
// draining while a new one opens.
//
// QUARANTINE: the batch is decoded to rows for the window map; dies when the
// worker consumes Batch directly (M4).
func (w *Worker) AddWindowRows(target string, chunkID uint32, batch *dataplane.Batch) error {
	p, ok := w.tables[target]
	if !ok {
		return fmt.Errorf("worker: window rows for unregistered table %s", target)
	}
	rows, err := decodeToChanges(batch, p.knownSchema.PrimaryKey)
	if err != nil {
		return fmt.Errorf("worker: window rows %s: %w", target, err)
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	win, ok := p.windows[chunkID]
	if !ok {
		win = make(map[string]rowchange.Change, len(rows))
		p.windows[chunkID] = win
	}
	for _, r := range rows {
		win[rowchange.KeyString(r.Key)] = r
	}
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
	var buf []rowchange.Change
	ticker := time.NewTicker(w.cfg.MaxInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		pos := buf[len(buf)-1].Position
		rows := len(buf)

		// During snapshot phase, separate snapshot lines into two groups:
		// untouched PKs (pure append, no delete) and touched PKs (upsert
		// with delete). This eliminates equality deletes for the initial
		// backfill on an empty table. A resumed snapshot skips this path
		// entirely (see snapshotResumed).
		// QUARANTINE: this partition path stays row-based until the sources
		// produce Arrow (M4); only the normal upsert path is columnar.
		p.snapshotMu.Lock()
		inSnapshot := p.snapshotState == string(snapshot.StateInProgress)
		guard := p.bootstrapGuard
		resumed := p.snapshotResumed
		p.snapshotMu.Unlock()

		if inSnapshot && !resumed && p.mode == dataplane.UpsertMode {
			var untouched []rowchange.Change
			var rest []rowchange.Change
			for _, c := range buf {
				if c.Snapshot && !guard.TestAndAddString(rowchange.KeyString(c.Key)) {
					untouched = append(untouched, c)
				} else {
					rest = append(rest, c)
				}
			}
			appendPos := pos
			if len(rest) > 0 {
				appendPos = ""
			}
			if len(untouched) > 0 {
				ab := rowchange.Batch{Table: p.target, Upserts: untouched, Position: appendPos, Mode: rowchange.ToRowMode(dataplane.AppendMode)}
				p.snapshotMu.Lock()
				ab.SnapshotState = p.snapshotState
				ab.SnapshotPending = p.snapshotPending
				p.snapshotMu.Unlock()
				dpb, err := dpint.BatchFromChangeBatch(ab, p.knownSchema)
				if err != nil {
					return fmt.Errorf("worker: table %s: bridge append: %w", p.target, err)
				}
				dpb.SnapshotState = ab.SnapshotState
				dpb.SnapshotPending = ab.SnapshotPending
				select {
				case p.readyCh <- readyBatch{batch: dpb, rows: len(untouched), upserts: len(untouched)}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if len(rest) > 0 {
				collapsed := rowchange.Collapse(rest)
				b := rowchange.Batch{Table: p.target, Upserts: collapsed.Upserts, Deletes: collapsed.Deletes, Position: pos, Mode: rowchange.ToRowMode(dataplane.UpsertMode)}
				p.snapshotMu.Lock()
				b.SnapshotState = p.snapshotState
				b.SnapshotPending = p.snapshotPending
				p.snapshotMu.Unlock()
				dpb, err := dpint.BatchFromChangeBatch(b, p.knownSchema)
				if err != nil {
					return fmt.Errorf("worker: table %s: bridge rest: %w", p.target, err)
				}
				dpb.SnapshotState = b.SnapshotState
				dpb.SnapshotPending = b.SnapshotPending
				select {
				case p.readyCh <- readyBatch{batch: dpb, rows: len(rest), upserts: len(collapsed.Upserts), deletes: len(collapsed.Deletes)}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			buf = buf[:0]
			return nil
		}

		switch p.mode {
		case dataplane.AppendMode:
			upserts := make([]rowchange.Change, 0, len(buf))
			for _, c := range buf {
				if c.Op == rowchange.OpDelete {
					// onDelete: skip — the delete is a fact only its absence
					// records. Drop and count.
					if p.appendDropDeletes {
						p.droppedDeletes.Add(1)
						if w.metrics != nil {
							w.metrics.DeletesDropped.WithLabelValues(p.target).Inc()
						}
						if w.onDroppedDelete != nil {
							w.onDroppedDelete(p.target, c.Position)
						}
						continue
					}
					// onDelete: record — appends the deleted row from its
					// before image. A delete with NO before image (Kafka
					// tombstone, source without the image) must never write
					// an all-null row: it is dropped and counted instead.
					// QUARANTINE: after the bridge round-trip the image lives in
					// After (the wire has no __before_* columns yet); dies in M4.
					// Only treat it as an image when it carries non-PK columns —
					// an image-less delete projects only its key onto the wire.
					if len(c.Before) == 0 && hasNonPKValue(c.After, p.knownSchema.PrimaryKey) {
						c.Before = c.After
					}
					if len(c.Before) == 0 {
						p.droppedDeletes.Add(1)
						if w.metrics != nil {
							w.metrics.DeletesDropped.WithLabelValues(p.target).Inc()
						}
						if w.onDroppedDelete != nil {
							w.onDroppedDelete(p.target, c.Position)
						}
						continue
					}
					c.After = c.Before
					if p.enricher != nil {
						// QUARANTINE: enrich the rewritten delete as a single-row
						// batch through the columnar seam; dies when the join
						// becomes columnar.
						dpb, err := dpint.BatchFromChangeBatch(rowchange.Batch{Table: p.target, Upserts: []rowchange.Change{c}, Mode: rowchange.ToRowMode(dataplane.AppendMode)}, p.knownSchema)
						if err != nil {
							return fmt.Errorf("worker: table %s: bridge: %w", p.target, err)
						}
						enriched, err := p.enricher.EnrichBatch(dpb)
						dpb.Release()
						if err != nil {
							return fmt.Errorf("worker: table %s: enrich: %w", p.target, err)
						}
						if enriched == nil {
							p.enrichDropped.Add(1)
							continue
						}
						enrichedRows, derr := decodeToChanges(enriched, p.knownSchema.PrimaryKey)
						enriched.Release()
						if derr != nil {
							return fmt.Errorf("worker: table %s: decode: %w", p.target, derr)
						}
						p.enrichDropped.Add(int64(1 - len(enrichedRows)))
						upserts = append(upserts, enrichedRows...)
						continue
					}
				}
				upserts = append(upserts, c)
			}
			b := rowchange.Batch{Table: p.target, Upserts: upserts, Position: pos, Mode: rowchange.ToRowMode(dataplane.AppendMode)}
			p.snapshotMu.Lock()
			b.SnapshotState = p.snapshotState
			b.SnapshotPending = p.snapshotPending
			p.snapshotMu.Unlock()
			dpb, err := dpint.BatchFromChangeBatch(b, p.knownSchema)
			if err != nil {
				return fmt.Errorf("worker: table %s: bridge append: %w", p.target, err)
			}
			dpb.SnapshotState = b.SnapshotState
			dpb.SnapshotPending = b.SnapshotPending
			select {
			case p.readyCh <- readyBatch{batch: dpb, rows: rows, upserts: len(upserts)}:
			case <-ctx.Done():
				return ctx.Err()
			}
			buf = buf[:0]
			return nil
		default:
			// Columnar upsert path: bridge the accumulated changes to a
			// dataplane.Batch and collapse columnar (CR-069 §3.2). The
			// collapsed upserts/deletes are merged back into ONE batch —
			// the sink commits a single batch per flush, and the position
			// must never separate from its data (two commits would advance
			// past uncommitted upserts on a crash between them).
			cb := rowchange.Batch{Table: p.target, Upserts: buf, Position: pos, Mode: rowchange.ToRowMode(dataplane.UpsertMode)}
			dpb, err := dpint.BatchFromChangeBatch(cb, p.knownSchema)
			if err != nil {
				return fmt.Errorf("worker: table %s: bridge upsert: %w", p.target, err)
			}
			p.snapshotMu.Lock()
			dpb.SnapshotState = p.snapshotState
			dpb.SnapshotPending = p.snapshotPending
			p.snapshotMu.Unlock()
			upCount, delCount := 0, 0
			upserts, deletes, err := dpint.Collapse(ctx, nil, dpb, p.knownSchema.PrimaryKey)
			dpb.Release()
			if err != nil {
				return fmt.Errorf("worker: table %s: collapse: %w", p.target, err)
			}
			if upserts != nil {
				upCount = int(upserts.Record.NumRows())
			}
			if deletes != nil {
				delCount = int(deletes.Record.NumRows())
			}
			combined, err := mergeBatches(upserts, deletes, nil)
			if upserts != nil {
				upserts.Release()
			}
			if deletes != nil {
				deletes.Release()
			}
			if err != nil {
				return fmt.Errorf("worker: table %s: merge: %w", p.target, err)
			}
			if combined != nil {
				select {
				case p.readyCh <- readyBatch{batch: combined, rows: rows, upserts: upCount, deletes: delCount}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			buf = buf[:0]
			return nil
		}
	}

	for {
		select {
		case ing, ok := <-p.ch:
			if !ok {
				return flush()
			}
			// Closes marker: emit the chunk's remaining window rows.
			if ing.Win != nil && ing.Win.Closes {
				p.winMu.Lock()
				win := p.windows[ing.Win.ChunkID]
				if win != nil {
					for _, row := range win {
						row.Position = ing.Position
						buf = append(buf, row)
					}
					delete(p.windows, ing.Win.ChunkID)
				}
				p.winMu.Unlock()
				continue // the Closes marker is not a data row
			}
			if ing.Batch == nil {
				continue
			}
			// Drift check runs on the SOURCE rows first — enrich adds reference
			// columns that must not trip drift. QUARANTINE: decodes the batch;
			// dies when the worker consumes Batch directly (M4).
			if len(p.knownSchema.Columns) > 0 {
				srcRows, derr := decodeToChanges(ing.Batch, p.knownSchema.PrimaryKey)
				if derr != nil {
					return fmt.Errorf("worker: table %s: decode: %w", p.target, derr)
				}
				for i := range srcRows {
					c := &srcRows[i]
					if c.After != nil {
						if d, hit := checkDrift(c.After, p.knownSchema); hit {
							p.snapshotMu.Lock()
							first := !p.driftReported[d.Column]
							p.driftReported[d.Column] = true
							p.snapshotMu.Unlock()
							if first && w.schemaDrift != nil {
								w.schemaDrift(SchemaDrift{Table: p.target, Column: d.Column, Kind: d.Kind})
							}
							return fmt.Errorf("worker: table %s: schema drift: column %q is not in the spec — declare it and resume", p.target, d.Column)
						}
					}
				}
			}
			// Enrich the whole batch (columnar seam), then decode.
			batch := ing.Batch
			if p.enricher != nil {
				enriched, err := p.enricher.EnrichBatch(batch)
				if err != nil {
					return fmt.Errorf("worker: table %s: enrich: %w", p.target, err)
				}
				if enriched == nil {
					// Every row was dropped by an inner join.
					p.enrichDropped.Add(int64(batch.Record.NumRows()))
					batch.Release()
					continue
				}
				batch = enriched
			}
			// Decode the columnar batch to rows for per-row processing.
			// QUARANTINE: dies when the worker consumes Batch directly (M4).
			rows, err := decodeToChanges(batch, p.knownSchema.PrimaryKey)
			if err != nil {
				batch.Release()
				return fmt.Errorf("worker: table %s: decode: %w", p.target, err)
			}
			if p.enricher != nil && len(rows) < int(batch.Record.NumRows()) {
				p.enrichDropped.Add(int64(batch.Record.NumRows() - int64(len(rows))))
			}

			for i := range rows {
				c := &rows[i]
				if ing.Win != nil && ing.Win.InWindow {
					c.Window = &rowchange.Window{ChunkID: ing.Win.ChunkID, InWindow: true}
				}
				// A live event during the snapshot marks its PK as touched:
				// the snapshot row for that key must take the safe upsert path,
				// never a pure append. The DBLog window only deduplicates events
				// that arrive while the covering chunk is open — events that
				// landed before their chunk was read would otherwise duplicate
				// the row. Closes markers carry no key and skip this.
				if !c.Snapshot && c.Key != nil {
					p.snapshotMu.Lock()
					if p.snapshotState == string(snapshot.StateInProgress) && p.bootstrapGuard != nil {
						p.bootstrapGuard.AddString(rowchange.KeyString(c.Key))
					}
					p.snapshotMu.Unlock()
				}
				// DBLog window application, design 3.4: an InWindow event is itself a
				// real change — it removes its snapshot row from the owning
				// chunk's window (the live version wins) and is then appended
				// normally. The coordinator tags gated live events with the
				// chunk that was draining when they were released, which need
				// not be the chunk that contains the row, so the delete scans
				// every open window.
				if c.Window != nil && c.Window.InWindow {
					p.winMu.Lock()
					k := rowchange.KeyString(c.Key)
					for _, win := range p.windows {
						if _, hit := win[k]; hit {
							delete(win, k)
							p.dropped++
						}
					}
					p.winMu.Unlock()
				}
				buf = append(buf, *c)
				if w.cfg.MaxRows > 0 && len(buf) >= w.cfg.MaxRows {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			// The demux handed us the batch; we own it and decoded it. The
			// enriched batch (if any) is separate and also owned.
			batch.Release()
			if batch != ing.Batch {
				ing.Batch.Release()
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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

// decodeToChanges decodes a columnar batch back to row-oriented changes.
// QUARANTINE: dies when the worker consumes Batch directly (M4).
func decodeToChanges(b *dataplane.Batch, pk []string) ([]rowchange.Change, error) {
	if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		return nil, nil
	}
	rows, _, err := transport.DecodeBatch(b.Record, nil, pk)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// IngestFromChanges wraps a change-oriented channel into Ingest batches,
// bridging accumulated changes via the transport. Buffers per table so a
// batch never mixes tables (the wire schema is per-table). Window markers
// pass through as Ingest with Win set.
// QUARANTINE: dies when sources produce Arrow directly (M4).
func IngestFromChanges(ctx context.Context, changes <-chan rowchange.Change, schema core.Schema) <-chan Ingest {
	out := make(chan Ingest, 64)
	go func() {
		defer close(out)
		bufs := map[string][]rowchange.Change{}
		flush := func() {
			for table, buf := range bufs {
				if len(buf) == 0 {
					continue
				}
				cb := rowchange.Batch{Table: table, Upserts: buf, Mode: rowchange.ToRowMode(dataplane.UpsertMode)}
				dpb, err := dpint.BatchFromChangeBatch(cb, schema)
				if err != nil {
					bufs[table] = bufs[table][:0] // QUARANTINE: drop on bridge error; dies in M4
					continue
				}
				select {
				case out <- Ingest{Table: table, Batch: dpb}:
				case <-ctx.Done():
					return
				}
				bufs[table] = bufs[table][:0]
			}
		}
		// A short ticker keeps data flowing to the worker so its MaxInterval
		// cadence still governs commits; the 100-change threshold covers high
		// volume. QUARANTINE: dies when sources produce Arrow directly (M4).
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case c, ok := <-changes:
				if !ok {
					flush()
					return
				}
				if c.Window != nil {
					flush()
					if c.Window.Closes {
						select {
						case out <- Ingest{Table: c.Table, Win: c.Window, Position: c.Position}:
						case <-ctx.Done():
							return
						}
						continue
					}
					// InWindow is DATA + a routing tag; the batch must survive.
					cb := rowchange.Batch{Table: c.Table, Upserts: []rowchange.Change{c}, Mode: rowchange.ToRowMode(dataplane.UpsertMode)}
					dpb, err := dpint.BatchFromChangeBatch(cb, schema)
					if err != nil {
						continue
					}
					select {
					case out <- Ingest{Table: c.Table, Batch: dpb, Win: c.Window}:
					case <-ctx.Done():
						return
					}
					continue
				}
				bufs[c.Table] = append(bufs[c.Table], c)
				if len(bufs[c.Table]) >= 100 {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// hasNonPKValue reports whether the row image carries a value in a non-PK
// column. Used to distinguish a real delete image from a key-only projection
// after the bridge round-trip (QUARANTINE).
func hasNonPKValue(row map[string]any, pk []string) bool {
	for name, v := range row {
		if v == nil {
			continue
		}
		isPK := false
		for _, p := range pk {
			if p == name {
				isPK = true
				break
			}
		}
		if !isPK {
			return true
		}
	}
	return false
}
