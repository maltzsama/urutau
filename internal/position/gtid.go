// Package position defines the replication position contract. This file
// implements the MySQL GTID-set position on top of go-mysql's MysqlGTIDSet.
package position

import (
	"fmt"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// GTID wraps a MySQL GTID set (a set of uuid:interval pairs) as a Position.
type GTID struct {
	set *gomysql.MysqlGTIDSet
}

// ParseGTID parses a GTID set string such as
// "uuid1:1-5,uuid2:7". Empty input yields an empty set.
func ParseGTID(s string) (*GTID, error) {
	if s == "" {
		set, err := gomysql.ParseMysqlGTIDSet("")
		if err != nil {
			return nil, err
		}
		return &GTID{set: set.(*gomysql.MysqlGTIDSet)}, nil
	}
	set, err := gomysql.ParseMysqlGTIDSet(s)
	if err != nil {
		return nil, fmt.Errorf("position: parse gtid %q: %w", s, err)
	}
	return &GTID{set: set.(*gomysql.MysqlGTIDSet)}, nil
}

// MustGTID is ParseGTID for configuration-time literals; it panics on error.
func MustGTID(s string) *GTID {
	g, err := ParseGTID(s)
	if err != nil {
		panic(err)
	}
	return g
}

// Raw returns the underlying go-mysql set, for the canal hooks.
func (g *GTID) Raw() *gomysql.MysqlGTIDSet { return g.set }

// String renders the set in the canonical text form.
func (g *GTID) String() string { return g.set.String() }

// Compare orders sets by their maximum transaction number per server uuid,
// then by total transaction count. It exists to support Min; cross-server
// comparison is only meaningful for sets sharing the same uuid universe,
// which is the single-source guarantee of this project.
func (g *GTID) Compare(other Position) int {
	o, ok := other.(*GTID)
	if !ok {
		panic(fmt.Sprintf("position: cannot compare GTID to %T", other))
	}
	myMax := g.maxInterval()
	otherMax := o.maxInterval()
	if myMax < otherMax {
		return -1
	}
	if myMax > otherMax {
		return 1
	}
	return 0
}

// maxInterval returns the largest transaction number across all server
// uuids in the set.
func (g *GTID) maxInterval() uint64 {
	var max uint64
	for _, intervals := range g.set.Sets {
		for _, iv := range intervals.Intervals {
			if iv.End > max {
				max = iv.End
			}
		}
	}
	return max
}

// Contains reports whether every transaction in other is also in g.
func (g *GTID) Contains(other Position) bool {
	o, ok := other.(*GTID)
	if !ok {
		panic(fmt.Sprintf("position: cannot contain-check GTID against %T", other))
	}
	return g.set.Contain(o.set)
}

// Add merges other's transactions into g.
func (g *GTID) Add(other *GTID) {
	// Update merges the string representation of other into g.
	_ = g.set.Update(other.set.String())
}

// Min returns the position all others start from: the set that is contained
// by every candidate, if such a set exists, otherwise the one with the
// smallest max interval. Under a single source every committed position
// extends the previous one, so the contained set is well defined.
func Min(positions []Position) Position {
	if len(positions) == 0 {
		return nil
	}
	best := positions[0].(*GTID)
	for _, p := range positions[1:] {
		c := p.(*GTID)
		if best.Contains(c) {
			best = c
		} else if c.Contains(best) {
			// c is strictly smaller; keep best.
		} else {
			if c.Compare(best) < 0 {
				best = c
			}
		}
	}
	return best
}