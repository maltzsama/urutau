package staged

import (
	"sync"

	"github.com/maltzsama/urutau/position"
)

// TableLocks hands out the mutex that serializes one table's staged commits:
// two cycles of the same table must never race on the catalog, and a table's
// cycles commit in send order only while the take and the commit are one
// step. The zero value is ready to use.
type TableLocks struct {
	mu      sync.Mutex
	byTable map[string]*sync.Mutex
}

// For returns table's mutex, the same one on every call.
func (l *TableLocks) For(table string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()
	mu := l.byTable[table]
	if mu == nil {
		mu = &sync.Mutex{}
		if l.byTable == nil {
			l.byTable = map[string]*sync.Mutex{}
		}
		l.byTable[table] = mu
	}
	return mu
}

// MinSafePosition parses each delivery's watermark and returns the lowest one
// as the cycle's commit position. An empty position set yields "".
func MinSafePosition(parse func(string) (position.Position, error), positions []string) (string, error) {
	vals := make([]position.Position, 0, len(positions))
	for _, s := range positions {
		if s == "" {
			continue
		}
		p, err := parse(s)
		if err != nil {
			return "", err
		}
		vals = append(vals, p)
	}
	if len(vals) == 0 {
		return "", nil
	}
	best, err := position.MinSafe(vals)
	if err != nil {
		return "", err
	}
	return best.String(), nil
}

// CommitPosition is the position a complete cycle commits.
//
// A snapshot window's cycle commits none: its marker carries the reader's
// position, past stream cycles of the table not committed yet, and a crash
// would then skip them as covered (#468). Only the stream advances a table's
// position. A cycle of one source batch commits the batch's last position:
// every partition received its whole share, so the table is durable through
// it (#459). Any other cycle commits the minimum safe position over its
// deliveries.
func (c *Cycle) CommitPosition(parse func(string) (position.Position, error)) (string, error) {
	if c.Window {
		return "", nil
	}
	if c.BatchPos != "" {
		return c.BatchPos, nil
	}
	return MinSafePosition(parse, c.Positions)
}
