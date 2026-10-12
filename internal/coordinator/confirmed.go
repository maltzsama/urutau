package coordinator

import (
	"errors"

	"github.com/maltzsama/urutau/internal/confirmed"
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
			if acc.Holds() {
				seen[table] = struct{}{}
			}
		}
		c.gate.Unlock()
	}
	for _, table := range c.pause.HeldTables() {
		seen[table] = struct{}{}
	}
	return seen
}

// dispatchedPosition is the greatest position handed to a worker, the frontier
// the confirmed may advance to when nothing is in flight; nil before any
// dispatch, and nil with a warning when the tables' positions cannot be
// ordered (see confirmed.Sent.Frontier for the rule).
func (c *Coordinator) dispatchedPosition() position.Position {
	// The parser is reached only when something was sent: a coordinator with
	// no dispatch yet may have no source.
	parse := func(s string) (position.Position, error) { return c.src.ParsePosition(s) }
	best, err := c.sent.Frontier(parse, func(table, pos string, err error) {
		// deliverBatch parsed this very string before it was recorded, so a
		// failure here means the source's parser changed its mind. The
		// frontier is advisory, so the table is skipped, not the run ended;
		// but it is said.
		c.log.Debug("coordinator: dispatched position does not parse", "table", table, "pos", pos, "err", err)
	})
	var inc *confirmed.IncomparableError
	if errors.As(err, &inc) {
		c.log.Warn("coordinator: incomparable dispatched positions; not advancing retention past what is confirmed",
			"have", inc.Have.String(), "candidate", inc.Candidate.String())
		return nil
	}
	return best
}
