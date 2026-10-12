package confirmed

import (
	"fmt"
	"sync"

	"github.com/maltzsama/urutau/position"
)

// Sent holds the latest position sent for each table: a table's batches go
// out in order, so the last one sent is its highest. The zero value is ready
// to use.
type Sent struct {
	mu      sync.Mutex
	byTable map[string]string
}

// Note records the latest position sent for a table. An empty position is
// ignored: the batch carried none.
func (s *Sent) Note(table, pos string) {
	if pos == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byTable == nil {
		s.byTable = map[string]string{}
	}
	s.byTable[table] = pos
}

// Of returns the latest position sent for a table, "" before any.
func (s *Sent) Of(table string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byTable[table]
}

// IncomparableError says two tables' last sent positions have no order.
type IncomparableError struct {
	Have, Candidate position.Position
}

func (e *IncomparableError) Error() string {
	return fmt.Sprintf("incomparable dispatched positions %s and %s", e.Have, e.Candidate)
}

// Frontier is the greatest position handed to a worker, the frontier the
// confirmed position may advance to when nothing is in flight. It parses each
// table's last sent position and takes the greatest; nil before any dispatch.
// A position parse refuses is skipped and handed to skipped, when set: the
// frontier is advisory, so one table's bad position does not end the run.
//
// "The greatest" exists only when the positions can be ordered. Opaque plugin
// offsets, GTID sets of disjoint servers and offsets of disjoint topics
// cannot: there the frontier is undefined, and it is nil too, with an
// IncomparableError. Returning whichever position the map yields first would
// make the source's retention depend on iteration order; nil sends the
// confirmed position to the minimum over what was confirmed, which holds
// retention when it has to.
func (s *Sent) Frontier(parse func(string) (position.Position, error), skipped func(table, pos string, err error)) (position.Position, error) {
	// Copy under the lock, parse outside it: the lock is taken on every
	// enqueue (Note), and this runs on every ack.
	s.mu.Lock()
	raw := make(map[string]string, len(s.byTable))
	for table, pos := range s.byTable {
		raw[table] = pos
	}
	s.mu.Unlock()

	var best position.Position
	for table, str := range raw {
		p, err := parse(str)
		if err != nil {
			if skipped != nil {
				skipped(table, str, err)
			}
			continue
		}
		if best == nil {
			best = p
			continue
		}
		switch cmp := p.Compare(best); {
		case cmp == position.Incomparable:
			return nil, &IncomparableError{Have: best, Candidate: p}
		case cmp > 0:
			best = p
		}
	}
	return best, nil
}
