package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/source"
)

// waitWorkers blocks until every expected group has a session attached.
// Counting ready signals would miscount a flapping worker that attaches,
// dies, and reattaches inside the window (audit #4) — so the wait checks
// the attached flag directly, woken by each attach and the poll.
func (c *Coordinator) waitWorkers(ctx context.Context, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	allAttached := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, w := range c.workers {
			if !w.attached {
				return false
			}
		}
		return true
	}
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		if allAttached() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("coordinator: not all workers connected within %s", wait)
		}
		select {
		case <-c.ready:
		case <-poll.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pump encodes reader events into the data queue. While a DBLog window is
// open (gateOn), events of the gated table are buffered instead — released
// InWindow-tagged by flushWindow after the worker confirms ChunkReady. Other
// tables flow freely.
func (c *Coordinator) pump(ctx context.Context, out <-chan *dataplane.Batch) {
	for {
		select {
		case b, ok := <-out:
			if !ok {
				return
			}
			if b.Record != nil {
				c.log.Debug("coordinator: reader batch", "table", b.Table, "rows", b.Record.NumRows(), "mode", b.Mode)
			}
			if c.metrics != nil {
				c.metrics.EventsDecoded.Inc()
			}
			// A re-slice pauses this table so its drain can converge. The
			// batch is BUFFERED (not routed, not counted by the drain) until
			// the flip, then enqueued under the new layout — so no batch
			// spans the flip and ordering is preserved, and the pump keeps
			// draining the reader for every other table (issue #343).
			if c.pauseHold(b) {
				continue
			}
			if c.gateHold(ctx, b) {
				continue
			}
			// enqueueBatch takes ownership of b (serializes + releases).
			if err := c.enqueueBatch(ctx, b, nil); err != nil {
				c.pumpFail(ctx, err)
				return
			}
		case <-c.pauseWake:
			// A table resumed: route the batches held during its pause, in
			// order, under the new layout.
			if err := c.flushPaused(ctx); err != nil {
				c.pumpFail(ctx, err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// pumpFail surfaces a pump failure on the terminal plane: a pump death is a
// real failure — the reader stalls behind the closed out channel and the
// coordinator stays "alive" doing nothing (audit #9). On a cancelled pipeline
// run's select already owns the exit.
func (c *Coordinator) pumpFail(ctx context.Context, err error) {
	c.log.Warn("coordinator: enqueue failed", "err", err)
	if ctx.Err() == nil {
		select {
		case c.terminate <- fmt.Errorf("coordinator: pump: %w", err):
		default:
		}
	}
}

// sourceBatches forwards the reader's columnar batches to the pump — no
// decode, no per-row hop (G0/M4). Ownership: each batch moves to the pump,
// which gates, serializes and releases it.
func sourceBatches(ctx context.Context, rdr source.Reader) (<-chan *dataplane.Batch, <-chan error) {
	out := make(chan *dataplane.Batch, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		for {
			b, err := rdr.Next(ctx)
			if err != nil {
				errCh <- err
				return
			}
			if b == nil {
				errCh <- nil
				return
			}
			select {
			case out <- b:
			case <-ctx.Done():
				b.Release()
				return
			}
		}
	}()
	return out, errCh
}
