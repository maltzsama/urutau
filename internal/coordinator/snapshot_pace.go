package coordinator

import (
	"context"
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

// chunkCommitPoll is how often a waiting chunk re-checks the index.
const chunkCommitPoll = 20 * time.Millisecond

// noteChunkMarker records a queued Closes marker of worker's snapshot.
func (c *Coordinator) noteChunkMarker(worker string, id uint64) {
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	if c.chunkMarkers == nil {
		c.chunkMarkers = map[string][]uint64{}
	}
	c.chunkMarkers[worker] = append(c.chunkMarkers[worker], id)
}

// awaitChunkCommits returns once fewer than maxUncommittedChunks of worker's
// chunk markers are uncommitted, or when ctx ends (the chunk watchdog).
func (c *Coordinator) awaitChunkCommits(ctx context.Context, worker string) error {
	for {
		if c.uncommittedChunks(worker) < maxUncommittedChunks {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(chunkCommitPoll):
		}
	}
}

// uncommittedChunks drops worker's committed markers and counts the rest.
func (c *Coordinator) uncommittedChunks(worker string) int {
	idx := c.indexOf(worker)
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	if len(c.chunkMarkers[worker]) == 0 {
		return 0
	}
	ids := c.chunkMarkers[worker][:0]
	for _, id := range c.chunkMarkers[worker] {
		if idx != nil && idx.holds(id) {
			ids = append(ids, id)
		}
	}
	c.chunkMarkers[worker] = ids
	return len(ids)
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
