// Package position defines the replication position contract. This file
// implements the MySQL GTID-set position on top of go-mysql's MysqlGTIDSet.
package position

import (
	"fmt"
	"strconv"
	"strings"

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

// Compare orders sets. Containment is the natural order: a set containing
// another is greater. Incomparable sets (disjoint uuid universes) fall back
// to their maximum transaction number.
func (g *GTID) Compare(other Position) int {
	o, ok := other.(*GTID)
	if !ok {
		panic(fmt.Sprintf("position: cannot compare GTID to %T", other))
	}
	if g.Contains(o) {
		if o.Contains(g) {
			return 0
		}
		return 1
	}
	if o.Contains(g) {
		return -1
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
// uuids in the set, parsed from the canonical text form.
func (g *GTID) maxInterval() uint64 {
	var max uint64
	for _, part := range strings.Split(g.set.String(), ",") {
		// part is "uuid:1-5:8-10" or "uuid:7"; take everything after the uuid.
		colon := strings.Index(part, ":")
		if colon < 0 {
			continue
		}
		for _, iv := range strings.Split(part[colon+1:], ":") {
			end := iv
			if dash := strings.Index(end, "-"); dash >= 0 {
				end = end[dash+1:]
			}
			if n, err := strconv.ParseUint(end, 10, 64); err == nil && n > max {
				max = n
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
