package coordinator

import (
	"strings"

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
	for _, table := range c.bufferedTables() {
		for _, w := range c.loadRouting().owners[table] {
			owing[w.name] = true
		}
	}
	return owing
}

// bufferedTables returns the tables with decoded-but-unsent data in the
// snapshot gate, the re-slice pause buffer, or the staged accumulator.
func (c *Coordinator) bufferedTables() []string {
	seen := map[string]bool{}
	c.gateMu.Lock()
	for key, buf := range c.gateBuf {
		if len(buf) > 0 {
			if target, _, ok := strings.Cut(key, "#"); ok {
				seen[target] = true
			}
		}
	}
	for table, acc := range c.accum {
		if acc != nil && len(acc.batches) > 0 {
			seen[table] = true
		}
	}
	c.gateMu.Unlock()
	c.pausedMu.Lock()
	for table, buf := range c.pauseBuf {
		if len(buf) > 0 {
			seen[table] = true
		}
	}
	c.pausedMu.Unlock()
	out := make([]string, 0, len(seen))
	for table := range seen {
		out = append(out, table)
	}
	return out
}

// dispatchedPosition is the greatest position handed to a worker, the frontier
// the confirmed may advance to when nothing is in flight. It parses each
// table's last sent position and takes the greatest; nil before any dispatch.
func (c *Coordinator) dispatchedPosition() position.Position {
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	var best position.Position
	for _, s := range c.lastSent {
		if s == "" {
			continue
		}
		p, err := c.src.ParsePosition(s)
		if err != nil {
			continue
		}
		if best == nil {
			best = p
			continue
		}
		if cmp := p.Compare(best); cmp != position.Incomparable && cmp > 0 {
			best = p
		}
	}
	return best
}
