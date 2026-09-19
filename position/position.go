// Package position defines the replication position contract (GTID | LSN),
// including the containment that upholds the DBLog caught-up proof.
package position

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

var ErrNoPosition = errors.New("position: no known position")

// Incomparable is the Compare result for two positions with no defined order.
// Some positions are opaque cookies (plugin offsets, contract §8.2) — their
// only defined relation is identity. Callers MUST treat this as "cannot
// decide": never skip data, never advance the confirmed point past it.
const Incomparable = math.MaxInt

type Position interface {
	String() string
	Compare(other Position) int
	Contains(other Position) bool
}

// Meeter is implemented by positions with a PARTIAL order, where two values
// can diverge without either containing the other. Meet returns their
// greatest lower bound — the position every input contains — or false when
// there is none (different topics, different sources).
//
// MinSafe needs this because a partial order has no minimum among the inputs
// to select: for Kafka offsets {p0:100,p1:5} and {p0:5,p1:100}, neither is a
// safe resume point, and the only safe one ({p0:5,p1:5}) is not in the list.
// A totally ordered position (GTID, LSN) does not implement it — its minimum
// is always one of the inputs.
type Meeter interface {
	Meet(other Position) (Position, bool)
}

// Min returns the smallest position of a homogeneous list: the one every
// other position extends. Under a single source every committed position
// extends the previous one, so the order is well defined — containment for
// GTID sets, linearity for LSNs. Compare carries that order for both.
//
// An incomparable pair is folded with Meet when the type provides one, so a
// partially ordered position (Kafka offsets) yields the greatest lower bound
// rather than an arbitrary side. Without a meet the current best is kept:
// conservative when the order is undefined.
//
// P3: for a RESUME or RETENTION decision, prefer MinSafe — where Min keeps
// going, MinSafe reports the pair it cannot bound instead of returning a
// minimum that may be ahead of an uncommitted position.
func Min(positions []Position) Position {
	if len(positions) == 0 {
		return nil
	}
	best := positions[0]
	for _, p := range positions[1:] {
		switch c := p.Compare(best); {
		case c == Incomparable:
			if m, ok := meet(p, best); ok {
				best = m
			}
		case c < 0:
			best = p
		}
	}
	return best
}

// MinSafe is Min for a RESUME decision: it returns a position that every
// input contains, so resuming from it can never skip uncommitted data.
//
// For a totally ordered position (GTID, LSN) that is the smallest input.
// For a PARTIAL order (Kafka offsets, folded one-per-partition-owner by
// WK-001 C7) no input need be safe: {p0:100,p1:5} and {p0:5,p1:100} each
// sit ahead of the other on one partition. Selecting either resumes past
// the other's uncommitted range, so an incomparable pair is folded with
// Meet into their greatest lower bound ({p0:5,p1:5}) instead.
//
// An incomparable pair with no meet is an error, not a silent skip: there
// is no safe choice, so fail fast rather than resume past data (P1).
func MinSafe(positions []Position) (Position, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	best := positions[0]
	for _, p := range positions[1:] {
		switch c := p.Compare(best); {
		case c == Incomparable:
			m, ok := meet(p, best)
			if !ok {
				return nil, fmt.Errorf("position: incomparable positions %s and %s — no safe minimum to resume from", p, best)
			}
			best = m
		case c < 0:
			best = p
		}
	}
	return best, nil
}

// meet returns the greatest lower bound of two positions when their type
// provides one. Both sides are asked: Meet is symmetric, but only the
// receiver's type can decide compatibility.
func meet(a, b Position) (Position, bool) {
	if m, ok := a.(Meeter); ok {
		if got, ok := m.Meet(b); ok {
			return got, true
		}
	}
	if m, ok := b.(Meeter); ok {
		if got, ok := m.Meet(a); ok {
			return got, true
		}
	}
	return nil, false
}

// Ahead returns the keys of ps whose position is strictly after global,
// sorted for stable output. On resume from global, a stream that is ahead has
// already-committed data past the resume point, so it replays from global
// (idempotent under upsert) — the crash-recovery set. A nil global yields no
// keys.
func Ahead(global Position, ps map[string]Position) []string {
	if global == nil {
		return nil
	}
	var out []string
	for name, p := range ps {
		if p != nil && p.Compare(global) > 0 {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Parse decodes a committed position string using the parser for a source
// kind ("mysql", "postgres", "kafka"); an unknown or empty kind parses as a
// MySQL GTID set, the default. The position format is a property of the
// source, so a caller that stores opaque position strings but must compare
// them (a sink keeping one position per partition, WK-001 §2.6) carries the
// kind as a hint instead of reimplementing the format.
func Parse(kind, s string) (Position, error) {
	switch kind {
	case "postgres":
		return ParseLSN(s)
	case "kafka":
		return ParseOffsets(s)
	default:
		return ParseGTID(s)
	}
}
