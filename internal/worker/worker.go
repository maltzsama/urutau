// Package worker implements the worker plane: one batcher per table that
// accumulates changes and flushes them collapsed to a committer, with
// commits strictly serialized per table. For now the process runs
// collapsed — changes arrive over an in-process channel; the gRPC and Arrow
// Flight planes arrive with multi-worker support.
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
