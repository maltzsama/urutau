package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/source"
)

// waitWorkers blocks until every expected group has had a session attached.
// Counting ready signals would miscount a flapping worker that attaches,
// dies, and reattaches inside the window (audit #4) — so the wait checks
// each worker's own state, woken by each attach and the poll. A worker that
// attached and was lost again counts: its loss is recovered like any other
// (awaited, bounded by the delivery timeout). Requiring all of them attached
// at one instant never held while workers were being killed, and the
// coordinator restarted in a loop (chaos-1M-521691a).
func (c *Coordinator) waitWorkers(ctx context.Context, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	allAttached := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, w := range c.workers {
			if !w.attached && !w.hadSession {
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
	tick := time.NewTicker(cycleCheckEvery)
	defer tick.Stop()
	defer c.releaseAccums()
	for {
		// Liveness heartbeat: the pump's select wakes at least every
		// cycleCheckEvery (the tick), so a stale lastPump means the loop is
		// wedged (issue #601).
		c.lastPump.Store(time.Now().UnixNano())
		select {
		case <-tick.C:
			if err := c.flushDueAccums(ctx); err != nil {
				c.pumpFail(ctx, err)
				return
			}
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
			if err := c.flushAccumBeforePause(ctx, b.Table); err != nil {
				c.pumpFail(ctx, err)
				return
			}
			if c.pauseHold(b) {
				continue
			}
			if c.gateHold(ctx, b) {
				continue
			}
			if taken, err := c.accumulate(ctx, b); err != nil {
				c.pumpFail(ctx, err)
				return
			} else if taken {
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

// streamTerminal runs the common teardown for a source-stream terminal signal:
// a clean end is a normal return, anything else is wrapped (issue #559).
func (c *Coordinator) streamTerminal(err error) error {
	c.gracefulShutdown()
	c.emitLog(eventlog.KindJobStopped, terminalFields("stream", err))
	if errors.Is(err, errStreamEnd) {
		return nil
	}
	return fmt.Errorf("coordinator: stream: %w", err)
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
				// A clean end, not an error: sending nil and wrapping it
				// with %w rendered "stream: %!w(<nil>)" (issue #559).
				errCh <- errStreamEnd
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

// errStreamEnd is sent on the coordinator's stream error channel for a clean
// source-stream end, so the consumer can tell it from a failure (issue #559).
var errStreamEnd = errors.New("coordinator: source stream ended")
