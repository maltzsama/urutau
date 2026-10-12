// Package confirmed keeps each worker's durably committed position and
// derives from them the position the source may be told is confirmed: what
// its retention (a replication slot, a binlog purge) may advance to.
package confirmed

import (
	"sync"
	"time"

	"github.com/maltzsama/urutau/position"
)

// Positions holds the latest durably committed position of each worker.
//
// Keyed by worker, not table: for a partitioned table the N workers commit
// disjoint key ranges and must each hold their own position; a per-table key
// would let the last acker overwrite the lagging partitions. The zero value
// is ready to use.
type Positions struct {
	mu       sync.Mutex
	byWorker map[string]position.Position
	// lastPos and lastAt track when the confirmed position last advanced.
	lastPos position.Position
	lastAt  time.Time
}

// set stores pos for worker. Callers hold mu.
func (p *Positions) set(worker string, pos position.Position) {
	if p.byWorker == nil {
		p.byWorker = map[string]position.Position{}
	}
	p.byWorker[worker] = pos
}

// Record stores a worker's latest durably committed position. A nil
// position is ignored: it says nothing was committed.
func (p *Positions) Record(worker string, pos position.Position) {
	if pos == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.set(worker, pos)
}

// Baseline registers worker at pos, which may be nil. A worker that has not
// committed yet must be registered: with a nil position it holds the
// confirmed position back while it owes work, where an absent worker would
// let it advance past data that worker has not committed.
func (p *Positions) Baseline(worker string, pos position.Position) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.set(worker, pos)
}

// Seed registers worker at pos unless it is registered already.
func (p *Positions) Seed(worker string, pos position.Position) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.byWorker[worker]; !ok {
		p.set(worker, pos)
	}
}

// Forget drops worker.
func (p *Positions) Forget(worker string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.byWorker, worker)
}

// Of returns worker's position, nil when it has none.
func (p *Positions) Of(worker string) position.Position {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.byWorker[worker]
}

// Has reports whether worker is registered, with a position or without.
func (p *Positions) Has(worker string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.byWorker[worker]
	return ok
}

// Len returns how many workers are registered.
func (p *Positions) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.byWorker)
}

// Min returns the minimum committed position across the workers that still
// owe work; nil while nothing is provably committed.
//
// A worker that owes nothing must NOT constrain the minimum: an idle table
// never acks, so its committed position would otherwise pin the source's
// retention forever and let the Postgres WAL grow without bound. When no
// worker owes anything, every dispatched batch is committed, so the result
// is dispatched, the frontier of what was sent.
//
// The caller must read dispatched BEFORE it computes owing: a batch is
// visible as owed before its position is recorded as dispatched, so a
// frontier read first can never name a batch the owing set missed. The
// other order could pair a stale "nothing owing" with a newer frontier and
// confirm a batch that has not been acked.
//
// MinSafe, not Min: incomparable committed positions have no safe minimum.
// They return nil and the error, which holds retention back rather than
// advancing past uncommitted data.
func (p *Positions) Min(owing map[string]bool, dispatched position.Position) (position.Position, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.byWorker) == 0 {
		return nil, nil
	}
	var vals []position.Position
	anyOwing := false
	for name, pos := range p.byWorker {
		if !owing[name] {
			continue // idle: does not constrain the minimum
		}
		anyOwing = true
		if pos == nil {
			// A worker that owes work but has no committed position yet: hold
			// retention back rather than advance over its data.
			return nil, nil
		}
		vals = append(vals, pos)
	}
	if !anyOwing && dispatched != nil {
		// Every dispatched batch is committed: advance to the frontier.
		return dispatched, nil
	}
	if !anyOwing {
		// Nothing dispatched yet this run: the boot baseline, min over all.
		for _, pos := range p.byWorker {
			if pos == nil {
				return nil, nil
			}
			vals = append(vals, pos)
		}
	}
	best, err := position.MinSafe(vals)
	if err != nil {
		return nil, err
	}
	return best, nil
}

// StalledSince returns the instant the confirmed position has been standing
// still from, given the confirmed position now and whether any worker owes
// work. The clock restarts when the position moves forward and whenever
// nothing is owed, so a later stall is measured from its onset; it starts
// when work is pending and nothing has committed yet.
func (p *Positions) StalledSince(pos position.Position, owing bool, now time.Time) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pos != nil {
		if p.lastPos == nil {
			p.lastPos = pos // first confirmed position
			p.lastAt = now
		} else if cmp := pos.Compare(p.lastPos); cmp != position.Incomparable && cmp > 0 {
			p.lastPos = pos // forward progress
			p.lastAt = now
		}
	}
	if !owing {
		p.lastAt = now
	} else if p.lastAt.IsZero() {
		p.lastAt = now
	}
	return p.lastAt
}
