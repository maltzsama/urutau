package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/maltzsama/urutau/internal/change"
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

	// DBLog snapshot window, per design: the chunk SELECT rows land here
	// (AddWindowRows) and live events tagged InWindow remove their key; the
	// Closes marker flushes what remains. Guarded by winMu because the
	// runner (snapshot orchestrator) populates it while the batcher goroutine
	// consumes events.
	winMu         sync.Mutex
	windowOpen    bool
	windowChunkID uint32
	chunkWindow   map[string]change.Change
	dropped       int64
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
func (w *Worker) Register(target string, c Committer) {
	w.tables[target] = &tablePipeline{target: target, committer: c, ch: make(chan change.Change, 1024)}
}

// AddWindowRows feeds the chunk SELECT result of a DBLog snapshot window
// into the table's batcher. The rows are held until the window closes: a
// live event tagged InWindow discards its key (the live version wins), and
// the Closes marker emits whatever is left. The runner calls this between
// the chunk SELECT and releasing the buffered events of [low, high].
func (w *Worker) AddWindowRows(target string, chunkID uint32, rows []change.Change) error {
	p, ok := w.tables[target]
	if !ok {
		return fmt.Errorf("worker: window rows for unregistered table %s", target)
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	if p.windowOpen && p.windowChunkID != chunkID {
		return fmt.Errorf("worker: table %s: window for chunk %d already open", target, p.windowChunkID)
	}
	if !p.windowOpen {
		p.windowOpen = true
		p.windowChunkID = chunkID
		p.chunkWindow = make(map[string]change.Change, len(rows))
	}
	for _, r := range rows {
		p.chunkWindow[change.KeyString(r.Key)] = r
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
			// DBLog window application, design §3.4: an InWindow event is
			// itself a real change — it removes its snapshot row from the
			// window (the live version wins) and is then appended normally.
			// The Closes marker flushes what remains as inserts and is not a
			// row.
			if c.Window != nil {
				p.winMu.Lock()
				if c.Window.InWindow && p.windowOpen && c.Window.ChunkID == p.windowChunkID {
					if _, hit := p.chunkWindow[change.KeyString(c.Key)]; hit {
						delete(p.chunkWindow, change.KeyString(c.Key))
						p.dropped++
					}
				}
				if c.Window.Closes && p.windowOpen && c.Window.ChunkID == p.windowChunkID {
					for _, row := range p.chunkWindow {
						row.Position = c.Position
						buf = append(buf, row)
					}
					p.chunkWindow = nil
					p.windowOpen = false
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
