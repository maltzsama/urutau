package coordinator

import (
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/inflight"
)

// workerFor returns the worker registered under name, or nil. It reads under
// c.mu: registerOwner/retireOwner mutate the registry at runtime during a
// re-slice (issue #312), so an unlocked read races them (issue #479). The
// returned pointer stays valid even if the worker is retired concurrently (the
// GC keeps it alive), which is what the ack path needs — it only prunes that
// worker's own sent list.
func (c *Coordinator) workerFor(name string) *workerState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.workers[name]
}

// workersSnapshot returns the registered workers at one instant, for a reader
// that must touch several of them (shutdown, dashboard summary) without
// holding c.mu across the loop.
func (c *Coordinator) workersSnapshot() []*workerState {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*workerState, 0, len(c.workers))
	for _, w := range c.workers {
		out = append(out, w)
	}
	return out
}

// bootWorker returns the worker group named name, creating it on first use,
// and records that it owns one more target ref. It runs during boot, but the
// metrics server (and /statusz) is already serving concurrently, so the worker
// registry is written under c.mu — the same lock statusz reads it with. Left
// unlocked, the boot map write raced the statusz iteration (issue #556).
func (c *Coordinator) bootWorker(name string, ref core.TableRef) *workerState {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.workers[name]
	if !ok {
		// A 128-bit random ticket colliding is ~0, but the queue-lookup map
		// is keyed by it — a collision would silently orphan a worker's
		// stream, so regenerate rather than assume.
		for {
			ticket := randTicket()
			if _, taken := c.byTicket[string(ticket)]; taken {
				continue
			}
			w = &workerState{
				name:   name,
				queue:  make(chan queuedBatch, workerQueueCap),
				ticket: ticket,
			}
			c.byTicket[string(ticket)] = w
			break
		}
		c.workers[name] = w
		c.index.Set(name, inflight.NewPositionIndex(c.runID))
	}
	w.refs = append(w.refs, ref)
	return w
}
