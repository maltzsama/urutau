package runner

import (
	"context"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/source"
)

// drainWaitTimeout is how long a Release drain waits for the puller to ACCEPT
// its request before deciding the reader is empty (the puller is blocked on
// Next) — there is then nothing to drain. Once accepted, the drain runs to
// completion with no deadline.
const drainWaitTimeout = 50 * time.Millisecond

// drainRequest is one Release drain handed to the puller goroutine. accepted
// closes when the puller takes it over; err carries the flush's error (nil on
// success), buffered so the puller never blocks reporting it, and doubles as
// the completion signal.
type drainRequest struct {
	accepted chan struct{}
	err      chan error
}

// flushDecoded drains batchCh and the reader's own buffer, so every event
// decoded ahead of a Closes marker is in ingest before the marker. The reader
// flush runs on the puller goroutine (the sole reader) via drainReq, so it is
// naturally ordered with Next; draining batchCh interleaved keeps a full
// batchCh from blocking that flush. The acceptance deadline covers a quiet
// source whose puller is blocked on Next (empty reader, nothing to drain); a
// failed drain is returned so the Closes marker is never emitted past it.
func (r *relay) flushDecoded(ctx context.Context, drainer source.Drainer, drainReq chan *drainRequest, batchCh <-chan *dataplane.Batch) error {
	drainBatchCh := func() bool {
		drained := false
		for {
			select {
			case b, ok := <-batchCh:
				if !ok {
					return drained
				}
				if r.gate(b) {
					drained = true
					continue
				}
				select {
				case r.ingest <- worker.Ingest{Table: b.Table, Batch: b}:
				case <-ctx.Done():
					return drained
				}
				drained = true
			default:
				return drained
			}
		}
	}
	// A reader without a Drain surface (an external plugin) has no puller
	// buffer to flush: its Next reads the Flight stream on demand, so the
	// relay's own batchCh is all there is to drain.
	if drainer == nil {
		drainBatchCh()
		return nil
	}
	req := &drainRequest{accepted: make(chan struct{}), err: make(chan error, 1)}
	// Drop a stale request left by a previous Release whose acceptance wait
	// timed out, so a full drainReq can never block this send.
	select {
	case <-drainReq:
	default:
	}
	select {
	case drainReq <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Wait for the puller to accept the request. A puller blocked on Next
	// holds an empty reader, so once this deadline passes there is nothing to
	// drain and Release may proceed.
	select {
	case <-req.accepted:
	case <-time.After(drainWaitTimeout):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	// Accepted: drain batchCh interleaved with waiting for the flush to
	// finish, so a full batchCh never blocks the puller's pushes.
	for {
		if drainBatchCh() {
			continue
		}
		select {
		case err := <-req.err:
			drainBatchCh()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
