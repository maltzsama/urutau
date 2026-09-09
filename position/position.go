// Package position defines the replication position contract (GTID | LSN),
// including the containment that upholds the DBLog caught-up proof.
package position

import (
	"errors"
	"math"
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

// Min returns the smallest position of a homogeneous list: the one every
// other position extends. Under a single source every committed position
// extends the previous one, so the order is well defined — containment for
// GTID sets, linearity for LSNs. Compare carries that order for both.
//
// A position that is Incomparable with the current best is never selected:
// keeping the first is the conservative choice when order is undefined.
func Min(positions []Position) Position {
	if len(positions) == 0 {
		return nil
	}
	best := positions[0]
	for _, p := range positions[1:] {
		if c := p.Compare(best); c != Incomparable && c < 0 {
			best = p
		}
	}
	return best
}
