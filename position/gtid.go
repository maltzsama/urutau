// Package position defines the replication position contract. This file
// implements the MySQL GTID-set position: a set of uuid:interval pairs with
// containment as the natural order. It is deliberately free of the go-mysql
// dependency — the mysql source converts to and from go-mysql's type at its
// own boundary.
package position

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// interval is a half-open transaction range [start, stop), the same shape
// MySQL uses internally (rpl_gtid.h). The text form is closed: "n" means
// [n, n+1), "n-m" means [n, m+1).
type interval struct {
	start uint64
	stop  uint64
}

func (iv interval) String() string {
	if iv.stop == iv.start+1 {
		return strconv.FormatUint(iv.start, 10)
	}
	return fmt.Sprintf("%d-%d", iv.start, iv.stop-1)
}

// parseInterval parses "n" or "n-m" into a half-open interval.
func parseInterval(s string) (interval, error) {
	p := strings.Split(s, "-")
	var start, stop uint64
	var err error
	switch len(p) {
	case 1:
		start, err = strconv.ParseUint(p[0], 10, 64)
		if err != nil {
			return interval{}, fmt.Errorf("invalid interval %q: %w", s, err)
		}
		stop = start + 1
	case 2:
		start, err = strconv.ParseUint(p[0], 10, 64)
		if err == nil {
			stop, err = strconv.ParseUint(p[1], 10, 64)
		}
		if err != nil {
			return interval{}, fmt.Errorf("invalid interval %q: %w", s, err)
		}
		stop++
	default:
		return interval{}, fmt.Errorf("invalid interval %q: must be n[-n]", s)
	}
	if stop <= start {
		return interval{}, fmt.Errorf("invalid interval %q: end must be >= start", s)
	}
	return interval{start: start, stop: stop}, nil
}

// normalize sorts and merges overlapping or adjacent intervals.
func normalize(ivs []interval) []interval {
	if len(ivs) < 2 {
		return ivs
	}
	slices.SortFunc(ivs, func(a, b interval) int {
		if a.start < b.start {
			return -1
		}
		if a.start > b.start {
			return 1
		}
		if a.stop < b.stop {
			return -1
		}
		if a.stop > b.stop {
			return 1
		}
		return 0
	})
	out := ivs[:1]
	for _, iv := range ivs[1:] {
		last := &out[len(out)-1]
		if iv.start > last.stop {
			out = append(out, iv)
			continue
		}
		if iv.stop > last.stop {
			last.stop = iv.stop
		}
	}
	return out
}

// containsIntervals reports whether every interval in sub is covered by some
// interval in s. Both are normalized (sorted, non-overlapping).
func containsIntervals(s, sub []interval) bool {
	for _, iv := range sub {
		covered := false
		for _, c := range s {
			if iv.start >= c.start && iv.stop <= c.stop {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

// validUUID reports whether s is a canonical lowercase/uppercase UUID.
func validUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHex(c) {
				return false
			}
		}
	}
	return true
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// GTID is a MySQL GTID set (a set of uuid:interval pairs) as a Position.
type GTID struct {
	sets map[string][]interval // uuid -> normalized, sorted intervals
}

// ParseGTID parses a GTID set string such as
// "uuid1:1-5,uuid2:7". Empty input yields an empty set.
func ParseGTID(s string) (*GTID, error) {
	g := &GTID{sets: map[string][]interval{}}
	if s == "" {
		return g, nil
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		segs := strings.Split(part, ":")
		if len(segs) < 2 {
			return nil, fmt.Errorf("position: parse gtid %q: invalid part %q", s, part)
		}
		uuid := segs[0]
		if !validUUID(uuid) {
			return nil, fmt.Errorf("position: parse gtid %q: invalid uuid %q", s, uuid)
		}
		var ivs []interval
		for _, iv := range segs[1:] {
			it, err := parseInterval(iv)
			if err != nil {
				return nil, fmt.Errorf("position: parse gtid %q: %w", s, err)
			}
			ivs = append(ivs, it)
		}
		g.sets[uuid] = normalize(append(g.sets[uuid], ivs...))
	}
	return g, nil
}

// MustGTID is ParseGTID for configuration-time literals; it panics on error.
func MustGTID(s string) *GTID {
	g, err := ParseGTID(s)
	if err != nil {
		panic(err)
	}
	return g
}

// String renders the set in the canonical text form.
func (g *GTID) String() string {
	uuids := make([]string, 0, len(g.sets))
	for u := range g.sets {
		uuids = append(uuids, u)
	}
	slices.Sort(uuids)
	var sb strings.Builder
	for i, u := range uuids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(u)
		for _, iv := range g.sets[u] {
			sb.WriteString(":")
			sb.WriteString(iv.String())
		}
	}
	return sb.String()
}

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
// uuids in the set.
func (g *GTID) maxInterval() uint64 {
	var max uint64
	for _, ivs := range g.sets {
		for _, iv := range ivs {
			if end := iv.stop - 1; end > max {
				max = end
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
	for u, oivs := range o.sets {
		ivs, ok := g.sets[u]
		if !ok {
			return false
		}
		if !containsIntervals(ivs, oivs) {
			return false
		}
	}
	return true
}

// Add merges other's transactions into g.
func (g *GTID) Add(other *GTID) {
	for u, oivs := range other.sets {
		g.sets[u] = normalize(append(g.sets[u], oivs...))
	}
}
