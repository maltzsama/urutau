package coordinator

import (
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/spec"
)

// tablesOrSpec returns the expanded table list once boot has set c.tables, and
// otherwise the declared spec list (a bare Coordinator in tests, or a caller
// before boot enumerates). cfg.Spec.Tables is never mutated, so reading it is
// race-free. Callers in a concurrent path (statusz, dashboard) read it under
// c.mu, the lock the boot write takes.
func (c *Coordinator) tablesOrSpec() []spec.Table {
	if c.tables != nil {
		return c.tables
	}
	return c.cfg.Spec.Tables
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
		c.setIndex(name, newPositionIndex(c.runID))
	}
	w.refs = append(w.refs, ref)
	return w
}
