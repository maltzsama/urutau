package coordinator

import (
	"context"
	"errors"
	"time"
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

// chunkMarker is a queued Closes marker: its in-flight batch id, and the
// table and window it closes.
type chunkMarker struct {
	id       uint64
	target   string
	window   uint64
	chunkRef uint32
	highKey  []any // the window's last row's PK tuple (the redo cursor, #646)
}

// noteChunkMarker records a queued Closes marker of worker's snapshot.
func (c *Coordinator) noteChunkMarker(worker string, id uint64, target string, window uint64, chunkRef uint32, highKey []any) {
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	if c.chunkMarkers == nil {
		c.chunkMarkers = map[string][]chunkMarker{}
	}
	c.chunkMarkers[worker] = append(c.chunkMarkers[worker], chunkMarker{id: id, target: target, window: window, chunkRef: chunkRef, highKey: highKey})
}

// errWorkerLost is a chunk round-trip cut short by its worker's loss.
var errWorkerLost = errors.New("worker lost")

// awaitChunkCommits returns once fewer than below of worker's chunk markers
// are uncommitted, errWorkerLost when lost closes first (its windows died),
// or when ctx ends (the chunk watchdog).
func (c *Coordinator) awaitChunkCommits(ctx context.Context, worker string, below int, lost <-chan struct{}) error {
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
		case <-time.After(chunkCommitPoll):
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
func (c *Coordinator) heldChunkMarkers(worker string) []chunkMarker {
	idx := c.indexOf(worker)
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	kept := c.chunkMarkers[worker][:0]
	for _, m := range c.chunkMarkers[worker] {
		if idx != nil && idx.holds(m.id) {
			kept = append(kept, m)
		}
	}
	if len(kept) == 0 {
		delete(c.chunkMarkers, worker)
		return nil
	}
	clear(c.chunkMarkers[worker][len(kept):])
	c.chunkMarkers[worker] = kept
	return append([]chunkMarker(nil), kept...)
}

// holds reports whether batch id is still unacked.
func (p *positionIndex) holds(id uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.head {
		if h.id == id {
			return true
		}
	}
	return false
}

// minHeldID returns the oldest unacked batch id, or 0.
func (p *positionIndex) minHeldID() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var min uint64
	for _, h := range p.head {
		if min == 0 || h.id < min {
			min = h.id
		}
	}
	return min
}

// maxHeldID returns the newest unacked batch id, or 0.
func (p *positionIndex) maxHeldID() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var max uint64
	for _, h := range p.head {
		if h.id > max {
			max = h.id
		}
	}
	return max
}

// onMarkerAck releases a Closes marker the worker acked by id: the window's
// rows are committed (#468). It advances no position — the window's rows
// carry none.
func (c *Coordinator) onMarkerAck(worker string, id uint64) {
	idx := c.indexOf(worker)
	if idx == nil {
		return
	}
	// Record the chunk's resume cursor BEFORE releasing the marker: a
	// concurrent heldChunkMarkers prunes acked markers, so this must read the
	// marker while it is still held (issue #646).
	c.recordCursor(worker, id)
	freed, freedOversized, popped := idx.releaseMarker(id)
	c.notifyAck()
	if freed > 0 {
		c.budget.release(worker, freed)
	}
	if freedOversized {
		c.budget.clearOversized(worker)
	}
	if w := c.workerFor(worker); w != nil {
		w.dropSent(popped)
	}
}

// recordCursor stores a chunk's resume cursor — the high key of the last
// window whose Closes marker this ack committed — so a mid-chunk redo resumes
// the chunk from it instead of re-emitting already-committed windows (#646).
// A window whose Closes marker was never sent has no marker here, so it is
// never mistaken for committed.
func (c *Coordinator) recordCursor(worker string, id uint64) {
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	for _, m := range c.chunkMarkers[worker] {
		if m.id == id && m.highKey != nil {
			if c.chunkCursor == nil {
				c.chunkCursor = map[uint32][]any{}
			}
			c.chunkCursor[m.chunkRef] = m.highKey
			return
		}
	}
}

// takeCursor returns and forgets a chunk's resume cursor, or nil.
func (c *Coordinator) takeCursor(chunkRef uint32) []any {
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	cur := c.chunkCursor[chunkRef]
	delete(c.chunkCursor, chunkRef)
	return cur
}
