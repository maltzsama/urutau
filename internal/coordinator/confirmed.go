package coordinator

import (
	"github.com/maltzsama/urutau/position"
)

// workersOwing reports which workers hold data not yet durable: a non-empty
// queue, an in-flight (delivered-but-unacked) batch, or a table buffered in
// the snapshot gate, the re-slice pause buffer, or the staged accumulator. A
// worker that owes nothing must not constrain the confirmed position, or an
// idle table pins the source's retention forever.
func (c *Coordinator) workersOwing() map[string]bool {
	c.mu.Lock()
	owing := make(map[string]bool, len(c.workers))
	names := make([]string, 0, len(c.workers))
	for name, w := range c.workers {
		names = append(names, name)
		if len(w.queue) > 0 {
			owing[name] = true
		}
	}
	c.mu.Unlock()
	for _, name := range names {
		if !owing[name] && c.inFlight(name) > 0 {
			owing[name] = true
		}
	}
	rt := c.loadRouting()
	for table := range c.bufferedTables() {
		owners, _ := rt.OwnersOf(table)
		for _, w := range owners {
			owing[w.name] = true
		}
	}
	return owing
}

// bufferedTables returns the tables with decoded-but-unsent data in the
// snapshot gate, the re-slice pause buffer, or the staged accumulator.
func (c *Coordinator) bufferedTables() map[string]struct{} {
	seen := map[string]struct{}{}
	if c.gate != nil {
		c.gate.Lock()
		for _, target := range c.gate.BufferedTablesLocked() {
			seen[target] = struct{}{}
		}
		for table, acc := range c.accum {
			if acc != nil && len(acc.batches) > 0 {
				seen[table] = struct{}{}
			}
		}
		c.gate.Unlock()
	}
	c.pausedMu.Lock()
	for table, buf := range c.pauseBuf {
		if len(buf) > 0 {
			seen[table] = struct{}{}
		}
	}
	c.pausedMu.Unlock()
	return seen
}

// dispatchedPosition is the greatest position handed to a worker, the frontier
// the confirmed may advance to when nothing is in flight. It parses each
// table's last sent position and takes the greatest; nil before any dispatch.
//
// "The greatest" exists only when the positions can be ordered. Opaque plugin
// offsets, GTID sets of disjoint servers and offsets of disjoint topics
// cannot: there the frontier is undefined, and it is nil too, with a warning.
// Returning whichever position the map yields first would make the source's
// retention depend on iteration order; nil sends confirmedPosition to the
// minimum over what was confirmed, which holds retention when it has to.
func (c *Coordinator) dispatchedPosition() position.Position {
	// Copy under the lock, parse outside it: sentMu is taken on every enqueue
	// (noteSent), and this runs on every ack.
	c.sentMu.Lock()
	raw := make(map[string]string, len(c.lastSent))
	for table, s := range c.lastSent {
		if s != "" {
			raw[table] = s
		}
	}
	c.sentMu.Unlock()

	var best position.Position
	for table, s := range raw {
		p, err := c.src.ParsePosition(s)
		if err != nil {
			// deliverBatch parsed this very string before it was recorded,
			// so a failure here means the source's parser changed its mind.
			// The frontier is advisory, so the table is skipped, not the run
			// ended; but it is said.
			c.log.Debug("coordinator: dispatched position does not parse", "table", table, "pos", s, "err", err)
			continue
		}
		if best == nil {
			best = p
			continue
		}
		switch cmp := p.Compare(best); {
		case cmp == position.Incomparable:
			c.log.Warn("coordinator: incomparable dispatched positions; not advancing retention past what is confirmed",
				"have", best.String(), "candidate", p.String())
			return nil
		case cmp > 0:
			best = p
		}
	}
	return best
}
