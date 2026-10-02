package runner

import (
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// updateCommitted records the position a target table durably committed and
// recomputes the pipeline-wide minimum — the value reported to the source so
// its retention never advances past uncommitted data. The minimum uses the
// position's own ordering: LSNs and GTID sets are not lexicographically
// ordered ("0/10" sorts before "0/2" as strings, 16 after 2 as positions),
// and a wrong minimum would advance the slot past data still in flight.
func (r *Runner) updateCommitted(table string, pos position.Position) {
	if pos == nil {
		return
	}
	r.posMu.Lock()
	defer r.posMu.Unlock()
	r.committedPositions[table] = pos
	vals := make([]position.Position, 0, len(r.committedPositions))
	for _, p := range r.committedPositions {
		vals = append(vals, p)
	}
	// MinSafe: an incomparable pair has no safe minimum — nil holds the
	// confirmed point back rather than advancing the slot past uncommitted
	// data (same direction as StringPosition's incomparable).
	best, err := position.MinSafe(vals)
	if err != nil {
		r.log.Warn("runner: incomparable committed positions; not advancing confirmed point", "err", err)
		r.minConfirmed = nil
		return
	}
	r.minConfirmed = best
}

// deliverFunc is the relay's dispatch callback: it parses a batch position
// string with the source and records it for the confirmed-point rule.
func (r *Runner) deliverFunc(src source.Source) func(table, pos string) {
	return func(table, pos string) {
		if pos == "" {
			return
		}
		if p, err := src.ParsePosition(pos); err == nil {
			r.noteDelivered(table, p)
		}
	}
}

// noteDelivered records the last position read from the source for a table as
// the relay dispatches it, before it is routed or gated. It is the
// "delivered" half of the confirmed-point rule.
func (r *Runner) noteDelivered(table string, pos position.Position) {
	if pos == nil {
		return
	}
	r.posMu.Lock()
	defer r.posMu.Unlock()
	if cur, ok := r.delivered[table]; !ok || cur == nil || pos.Compare(cur) > 0 {
		r.delivered[table] = pos
	}
	if r.dispatched == nil {
		r.dispatched = pos
	} else if cmp := pos.Compare(r.dispatched); cmp != position.Incomparable && cmp > 0 {
		r.dispatched = pos
	}
}

// confirmedPosition is the position the Postgres reader may advance the
// slot's confirmed_flush_lsn to. A table with data dispatched but not yet
// committed (delivered past committed) holds it at that table's committed
// position; a table with nothing in flight does not pin it, so an idle table
// cannot stall retention. When every dispatched batch is committed, it
// advances to the last dispatched position. nil means hold.
func (r *Runner) confirmedPosition() position.Position {
	r.posMu.Lock()
	defer r.posMu.Unlock()
	if r.delivered == nil {
		// No dispatch tracking (a bare Runner in tests): the legacy minimum.
		return r.minConfirmed
	}
	var vals []position.Position
	for table, d := range r.delivered {
		if d == nil {
			continue
		}
		c := r.committedPositions[table]
		if c == nil {
			// Dispatched but never committed: nothing provably durable.
			return nil
		}
		if cmp := d.Compare(c); cmp == position.Incomparable || cmp > 0 {
			vals = append(vals, c)
		}
	}
	if len(vals) == 0 {
		return r.dispatched
	}
	best, err := position.MinSafe(vals)
	if err != nil {
		r.log.Warn("runner: incomparable in-flight positions; not advancing confirmed point", "err", err)
		return nil
	}
	return best
}
