package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/observability"
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
type OnCommit func(b change.Batch, rows int)

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
	mode      change.WriteMode
	ch        chan change.Change

	// appendDropDeletes implements onDelete: skip — every DELETE in an
	// append-only table is dropped (counted) instead of appended from its
	// before image.
	appendDropDeletes bool
	// droppedDeletes counts deletes dropped in append mode (skip or a
	// record that had no before image). Read cross-goroutine.
	droppedDeletes atomic.Int64

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
	windows map[uint32]map[string]change.Change
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
	batch change.Batch
	rows  int
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
func (w *Worker) Register(target string, c sink.TableWriter, mode change.WriteMode) {
	w.tables[target] = newTablePipeline(target, c, mode)
}

// RegisterCommitter wires a writer to a target table (test helper; same
// contract as Register).
func (w *Worker) RegisterCommitter(target string, c sink.TableWriter, mode change.WriteMode) {
	w.tables[target] = newTablePipeline(target, c, mode)
}

// OnDroppedDelete installs the observer for deletes that append-only
// dropped (declared skip, or a record with no before image).
func (w *Worker) OnDroppedDelete(f OnDroppedDelete) { w.onDroppedDelete = f }

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

func newTablePipeline(target string, c sink.TableWriter, mode change.WriteMode) *tablePipeline {
	return &tablePipeline{
		target:         target,
		committer:      c,
		mode:           mode,
		ch:             make(chan change.Change, 1024),
		readyCh:        make(chan readyBatch, 1),
		windows:        map[uint32]map[string]change.Change{},
		bootstrapGuard: bloom.NewWithEstimates(100_000, 0.01),
		driftReported:  map[string]bool{},
	}
}

// AddWindowRows feeds one chunk's SELECT result of a DBLog snapshot window
// into the table's batcher. The rows are held in that chunk's window until
// its Closes marker: a live event tagged InWindow discards its key (the live
// version wins). Chunks are independent — a previous chunk may still be
// draining while a new one opens.
func (w *Worker) AddWindowRows(target string, chunkID uint32, rows []change.Change) error {
	p, ok := w.tables[target]
	if !ok {
		return fmt.Errorf("worker: window rows for unregistered table %s", target)
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	win, ok := p.windows[chunkID]
	if !ok {
		win = make(map[string]change.Change, len(rows))
		p.windows[chunkID] = win
	}
	for _, r := range rows {
		win[change.KeyString(r.Key)] = r
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
func (w *Worker) Run(ctx context.Context, ingest <-chan change.Change) error {
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
		for c := range ingest {
			p, ok := w.tables[c.Table]
			if !ok {
				// A change for an unregistered table is a routing bug
				// upstream; dropping it silently would lose data. Cancel the
				// world — the pipelines surface their errors.
				cancel()
				return
			}
			select {
			case p.ch <- c:
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
func (w *Worker) runCommitter(ctx context.Context, p *tablePipeline) error {
	for rb := range p.readyCh {
		start := time.Now()
		if err := p.committer.Commit(ctx, rb.batch); err != nil {
			if w.metrics != nil {
				w.metrics.CommitFailures.WithLabelValues(p.target).Inc()
			}
			return fmt.Errorf("worker: table %s: commit: %w", p.target, err)
		}
		if w.metrics != nil {
			w.metrics.CommitDuration.WithLabelValues(p.target).Observe(time.Since(start).Seconds())
			w.metrics.RowsWritten.WithLabelValues(p.target, "upsert").Add(float64(len(rb.batch.Upserts)))
			w.metrics.EqualityDeletes.WithLabelValues(p.target).Add(float64(len(rb.batch.Deletes)))
		}
		if w.onCommit != nil {
			w.onCommit(rb.batch, rb.rows)
		}
	}
	return nil
}

// runBatcher collects changes, collapses them, and sends ready batches to
// the committer. When the channel closes, the committer drains and exits.
func (w *Worker) runBatcher(ctx context.Context, p *tablePipeline) error {
	var buf []change.Change
	ticker := time.NewTicker(w.cfg.MaxInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		pos := buf[len(buf)-1].Position

		// During snapshot phase, separate snapshot lines into two groups:
		// untouched PKs (pure append, no delete) and touched PKs (upsert
		// with delete). This eliminates equality deletes for the initial
		// backfill on an empty table. A resumed snapshot skips this path
		// entirely (see snapshotResumed).
		p.snapshotMu.Lock()
		inSnapshot := p.snapshotState == string(snapshot.StateInProgress)
		guard := p.bootstrapGuard
		resumed := p.snapshotResumed
		p.snapshotMu.Unlock()

		if inSnapshot && !resumed && p.mode == change.UpsertMode {
			// Partition: untouched snapshot lines go as pure append;
			// everything else (live events + touched snapshot lines)
			// goes as normal upsert.
			var untouched []change.Change
			var rest []change.Change
			for _, c := range buf {
				if c.Snapshot && !guard.TestAndAddString(change.KeyString(c.Key)) {
					untouched = append(untouched, c)
				} else {
					rest = append(rest, c)
				}
			}
			// The position travels only on the LAST commit: when an upsert
			// batch follows, the append batch advances nothing — otherwise a
			// crash between the two commits would resume past the not-yet-
			// committed live events. When there is no upsert batch, the
			// append is the last commit and carries the position.
			appendPos := pos
			if len(rest) > 0 {
				appendPos = ""
			}
			if len(untouched) > 0 {
				ab := change.Batch{
					Table:    p.target,
					Upserts:  untouched,
					Position: appendPos,
					Mode:     change.AppendMode,
				}
				p.snapshotMu.Lock()
				ab.SnapshotState = p.snapshotState
				ab.SnapshotPending = p.snapshotPending
				p.snapshotMu.Unlock()
				select {
				case p.readyCh <- readyBatch{batch: ab, rows: len(untouched)}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			// Send rest as normal upsert batch.
			if len(rest) > 0 {
				collapsed := change.Collapse(rest)
				b := change.Batch{
					Table:    p.target,
					Upserts:  collapsed.Upserts,
					Deletes:  collapsed.Deletes,
					Position: pos,
					Mode:     change.UpsertMode,
				}
				p.snapshotMu.Lock()
				b.SnapshotState = p.snapshotState
				b.SnapshotPending = p.snapshotPending
				p.snapshotMu.Unlock()
				select {
				case p.readyCh <- readyBatch{batch: b, rows: len(rest)}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			buf = buf[:0]
			return nil
		}

		var b change.Batch
		switch p.mode {
		case change.AppendMode:
			upserts := make([]change.Change, 0, len(buf))
			for _, c := range buf {
				if c.Op == change.OpDelete {
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
				}
				upserts = append(upserts, c)
			}
			b = change.Batch{
				Table:    p.target,
				Upserts:  upserts,
				Position: pos,
				Mode:     change.AppendMode,
			}
		default:
			collapsed := change.Collapse(buf)
			b = change.Batch{
				Table:    p.target,
				Upserts:  collapsed.Upserts,
				Deletes:  collapsed.Deletes,
				Position: pos,
				Mode:     change.UpsertMode,
			}
		}
		// Attach snapshot state so it is persisted atomically with position.
		p.snapshotMu.Lock()
		b.SnapshotState = p.snapshotState
		b.SnapshotPending = p.snapshotPending
		p.snapshotMu.Unlock()
		rows := len(buf)
		buf = buf[:0]
		select {
		case p.readyCh <- readyBatch{batch: b, rows: rows}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	for {
		select {
		case c, ok := <-p.ch:
			if !ok {
				return flush()
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
					p.bootstrapGuard.AddString(change.KeyString(c.Key))
				}
				p.snapshotMu.Unlock()
			}
			// DBLog window application, design 3.4: an InWindow event is itself a
			// real change — it removes its snapshot row from the owning
			// chunk's window (the live version wins) and is then appended
			// normally. The coordinator tags gated live events with the
			// chunk that was draining when they were released, which need
			// not be the chunk that contains the row, so the delete scans
			// every open window. The chunk's Closes marker flushes what
			// remains as inserts; markers for unknown chunks are stale.
			if c.Window != nil {
				p.winMu.Lock()
				if c.Window.InWindow {
					k := change.KeyString(c.Key)
					for _, win := range p.windows {
						if _, hit := win[k]; hit {
							delete(win, k)
							p.dropped++
						}
					}
				}
				win := p.windows[c.Window.ChunkID]
				if c.Window.Closes {
					if win != nil {
						for _, row := range win {
							row.Position = c.Position
							buf = append(buf, row)
						}
						delete(p.windows, c.Window.ChunkID)
					}
					p.winMu.Unlock()
					continue // the Closes marker is not a data row
				}
				p.winMu.Unlock()
			}
			// Schema drift: a column that appears in the data but was not
			// known at introspection means the source schema moved under the
			// pipeline. Writing the row would corrupt the target (unknown
			// column) or silently drop data, so the change is refused and
			// the pipeline stops — restart only after the spec declares the
			// column. The check is recursive: a field added inside a struct
			// column is the same class of event as a top-level ADD COLUMN,
			// reported with its full path. Reported once per path.
			if len(p.knownSchema.Columns) > 0 && c.After != nil {
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
			buf = append(buf, c)
			if w.cfg.MaxRows > 0 && len(buf) >= w.cfg.MaxRows {
				if err := flush(); err != nil {
					return err
				}
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
