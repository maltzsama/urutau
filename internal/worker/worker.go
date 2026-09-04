package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/sink"
	"github.com/maltzsama/urutau/internal/snapshot"
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

type Worker struct {
	cfg      Config
	onCommit OnCommit
	tables   map[string]*tablePipeline
	metrics  *observability.Metrics
}

type tablePipeline struct {
	target    string
	committer sink.TableWriter
	mode      change.WriteMode
	ch        chan change.Change

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
	// (not_started → in_progress → complete) and the pending chunk list
	// are persisted atomically with position via the batch properties.
	snapshotMu      sync.Mutex
	snapshotState   string   // "not_started", "in_progress", "complete"
	snapshotPending []uint32 // chunk IDs still to process

	// bootstrapGuard tracks PKs touched by live events during the snapshot
	// phase. Snapshot lines whose PK was never touched are written as pure
	// appends (no equality delete), halving the write volume for initial
	// backfill. Released when snapshot completes.
	bootstrapGuard *bloom.BloomFilter
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
// sink.TableWriter implementation — the worker knows nothing about the sink
// (CR-012). The mode controls whether batches are collapsed (upsert) or
// passed through (append).
func (w *Worker) Register(target string, c sink.TableWriter, mode change.WriteMode) {
	w.tables[target] = newTablePipeline(target, c, mode)
}

// RegisterCommitter wires a writer to a target table (test helper; same
// contract as Register).
func (w *Worker) RegisterCommitter(target string, c sink.TableWriter, mode change.WriteMode) {
	w.tables[target] = newTablePipeline(target, c, mode)
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
// atomically with position. When snapshot completes, the bloom filter is
// released to free memory.
func (w *Worker) SetSnapshotState(target string, state string, pending []uint32) {
	p := w.tables[target]
	if p == nil {
		return
	}
	p.snapshotMu.Lock()
	defer p.snapshotMu.Unlock()
	p.snapshotState = state
	p.snapshotPending = pending
	if state == string(snapshot.StateComplete) {
		p.bootstrapGuard = nil
	}
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
		// backfill on an empty table.
		p.snapshotMu.Lock()
		inSnapshot := p.snapshotState == string(snapshot.StateInProgress)
		guard := p.bootstrapGuard
		p.snapshotMu.Unlock()

		if inSnapshot && p.mode == change.UpsertMode {
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
			// Send untouched as append-only batch (no equality delete).
			if len(untouched) > 0 {
				ab := change.Batch{
					Table:    p.target,
					Upserts:  untouched,
					Position: pos,
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
			// DBLog window application, design §3.4: an InWindow event is itself a
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
