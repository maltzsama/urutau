package coordinator

import (
	"context"
	"errors"
	"time"

	"github.com/maltzsama/urutau/internal/snapshot"
)

// A chunk's rows come from the worker's own SELECT and never pass through
// the flow budget, and the next chunk used to be requested as soon as the
// previous Closes marker was queued. Once chunk SELECTs were fast (#447) the
// events worker was handed a chunk every ~1.5 s against far slower commits:
// its heap held ~6 chunks in windows and ~6 more awaiting commit, and it was
// OOM-killed in every snapshot (#452).
//
// A chunk's Closes marker leaves the worker's in-flight index once the worker
// has committed it and everything queued before it, so the markers still in
// the index are the chunks not yet committed. A new chunk waits while
// maxUncommittedChunks of them are: one commits while the next is read.

// maxUncommittedChunks bounds a worker's snapshot chunks whose Closes marker
// is not yet committed.
const maxUncommittedChunks = 2

// chunkCommitPoll is the FALLBACK re-check cadence: the pacer now wakes on the
// ack notification (issue #587), and this bounds how long a missed signal
// could stall it.
const chunkCommitPoll = 20 * time.Millisecond

// errWorkerLost is a chunk round-trip cut short by its worker's loss.
var errWorkerLost = errors.New("worker lost")

// awaitChunkCommits returns once fewer than below of worker's chunk markers
// are uncommitted, errWorkerLost when lost closes first (its windows died),
// or when ctx ends (the chunk watchdog).
func (c *Coordinator) awaitChunkCommits(ctx context.Context, worker string, below int, lost <-chan struct{}) error {
	// One ticker for the whole wait: the safety-net poll below used to
	// allocate a timer on every pass of the loop.
	poll := time.NewTicker(chunkCommitPoll)
	defer poll.Stop()
	for {
		if c.uncommittedChunks(worker) < below {
			return nil
		}
		// Wait on the ack notification (issue #587): an ack that drops a
		// marker sends a coalesced signal, so the pacer wakes on the event
		// instead of polling. The buffered channel means a signal sent between
		// the check above and the select below is never lost; the slow ticker
		// is only a safety net.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-lost:
			return errWorkerLost
		case <-c.ackNotify:
		case <-poll.C:
		}
	}
}

// notifyAck wakes awaitChunkCommits waiters without blocking the ack path
// (issue #587). Buffered 1: coalesced, a burst of acks is one wake.
func (c *Coordinator) notifyAck() {
	select {
	case c.ackNotify <- struct{}{}:
	default:
	}
}

// uncommittedChunks drops worker's committed markers and counts the rest.
func (c *Coordinator) uncommittedChunks(worker string) int {
	return len(c.heldChunkMarkers(worker))
}

// heldChunkMarkers drops worker's committed markers and returns the rest,
// oldest first.
func (c *Coordinator) heldChunkMarkers(worker string) []snapshot.Marker {
	idx := c.index.Of(worker)
	return c.markers.Held(worker, func(id uint64) bool { return idx != nil && idx.Holds(id) })
}

// onMarkerAck releases a Closes marker the worker acked by id: the window's
// rows are committed (#468). It advances no position — the window's rows
// carry none.
func (c *Coordinator) onMarkerAck(worker string, id uint64) {
	idx := c.index.Of(worker)
	if idx == nil {
		return
	}
	// Record the chunk's resume cursor BEFORE releasing the marker: a
	// concurrent heldChunkMarkers prunes acked markers, so this must read the
	// marker while it is still held (issue #646).
	c.markers.RecordCursor(worker, id)
	freed, freedOversized, popped := idx.ReleaseMarker(id)
	c.notifyAck()
	if freed > 0 {
		c.budget.Release(worker, freed)
	}
	if freedOversized {
		c.budget.ClearOversized(worker)
	}
	if w := c.workerFor(worker); w != nil {
		w.dropSent(popped)
	}
}
