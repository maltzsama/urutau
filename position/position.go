// Package position defines the replication position contract (GTID | LSN),
// including the containment that upholds the DBLog caught-up proof.
package position

import (
	"errors"
	"fmt"
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
//
// P3: for a RESUME or RETENTION decision, prefer MinSafe — Min silently
// skips an Incomparable entry, which could return a minimum that is ahead
// of an uncommitted position. It is safe here only because every caller
// feeds a single source's positions (one type, fully ordered).
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

// MinSafe is Min for a RESUME decision: an incomparable pair is an error,
// not a silent skip. Picking a minimum when the order is undefined could
// resume PAST uncommitted data (a skip) — there is no safe choice, so fail
// fast. A single-source pipeline has one position type, so this never fires
// in practice; it is a guard, not a path (P1).
func MinSafe(positions []Position) (Position, error) {
	if len(positions) == 0 {
		return nil, nil
	}
	best := positions[0]
	for _, p := range positions[1:] {
		switch c := p.Compare(best); {
		case c == Incomparable:
			return nil, fmt.Errorf("position: incomparable positions %s and %s — no safe minimum to resume from", p, best)
		case c < 0:
			best = p
		}
	}
	return best, nil
}
