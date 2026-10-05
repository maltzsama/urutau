package runner

import (
	"context"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/source"
)

// drainPollInterval is how often a Release drain re-checks the puller's flush
// while draining batchCh, so a full batchCh never blocks the flush.
const drainPollInterval = time.Millisecond

// drainWaitTimeout is how long a Release drain waits for the puller to flush
// the reader before deciding the reader is empty (the puller is blocked on
// Next) — there is then nothing to drain.
const drainWaitTimeout = 50 * time.Millisecond

// flushDecoded drains batchCh and the reader's own buffer, so every event
// decoded ahead of a Closes marker is in ingest before the marker. The reader
// flush runs on the puller goroutine (the sole reader) via drainReq, so it is
// naturally ordered with Next; draining batchCh interleaved keeps a full
// batchCh from blocking that flush. The deadline is the safety valve for a
// quiet source: a puller blocked on Next holds an empty reader, so there is
// nothing to drain.
func (r *relay) flushDecoded(ctx context.Context, drainer source.Drainer, drainReq chan chan struct{}, batchCh <-chan *dataplane.Batch) error {
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
	if drainer == nil {
		drainBatchCh()
		return nil
	}
	done := make(chan struct{})
	select {
	case drainReq <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	deadline := time.Now().Add(drainWaitTimeout)
	for {
		drainBatchCh()
		select {
		case <-done:
			drainBatchCh()
			return nil
		case <-time.After(drainPollInterval):
			if time.Now().After(deadline) {
				return nil // the puller is blocked on Next: the reader is empty
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
