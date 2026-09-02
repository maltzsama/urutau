package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/sink/iceberg"
)

// Config tunes batch accumulation.
type Config struct {
	// MaxRows flushes the batch once this many changes are buffered.
	// Byte-based triggers arrive with arrow sizing.
	MaxRows int
	// MaxInterval flushes whatever is buffered on this cadence.
	MaxInterval time.Duration
}

// Committer applies one collapsed batch of a single table. Implementations
// must be safe to call from the table's dedicated goroutine only — the
// serialization invariant is the caller's design.
type Committer interface {
	Commit(ctx context.Context, b change.Batch) error
}

// OnCommit observes successful commits (bookkeeping, tests).
type OnCommit func(b change.Batch, rows int)

type Worker struct {
	cfg      Config
	onCommit OnCommit
	tables   map[string]*tablePipeline
}

type tablePipeline struct {
	target    string
	committer Committer
	ch        chan change.Change

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
}

// New builds a worker; register tables before Run.
func New(cfg Config) *Worker {
	return &Worker{
		cfg:    cfg,
		tables: make(map[string]*tablePipeline),
	}
}

// OnCommit installs the commit observer.
func (w *Worker) OnCommit(f OnCommit) { w.onCommit = f }

// Register wires a committer to a target table.
func (w *Worker) Register(target string, c *iceberg.TableWriter) {
	w.tables[target] = newTablePipeline(target, c)
}

// RegisterCommitter wires a committer to a target table (for tests).
func (w *Worker) RegisterCommitter(target string, c Committer) {
	w.tables[target] = newTablePipeline(target, c)
}

func newTablePipeline(target string, c Committer) *tablePipeline {
	return &tablePipeline{
		target:    target,
		committer: c,
		ch:        make(chan change.Change, 1024),
		windows:   map[uint32]map[string]change.Change{},
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
	var buf []change.Change
	ticker := time.NewTicker(w.cfg.MaxInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		collapsed := change.Collapse(buf)
		b := change.Batch{
			Table:    p.target,
			Upserts:  collapsed.Upserts,
			Deletes:  collapsed.Deletes,
			Position: buf[len(buf)-1].Position,
		}
		rows := len(buf)
		buf = buf[:0]
		if err := p.committer.Commit(ctx, b); err != nil {
			return fmt.Errorf("worker: table %s: commit: %w", p.target, err)
		}
		if w.onCommit != nil {
			w.onCommit(b, rows)
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
