// Package position defines the replication position contract (GTID | LSN),
// including the containment that upholds the DBLog caught-up proof.
package position

import "errors"

var ErrNoPosition = errors.New("position: no known position")

type Position interface {
	String() string
	Compare(other Position) int
	Contains(other Position) bool
}

// Min returns the smallest position of a homogeneous list: the one every
// other position extends. Under a single source every committed position
// extends the previous one, so the order is well defined — containment for
// GTID sets, linearity for LSNs. Compare carries that order for both.
func Min(positions []Position) Position {
	if len(positions) == 0 {
		return nil
	}
	best := positions[0]
	for _, p := range positions[1:] {
		if p.Compare(best) < 0 {
			best = p
		}
	}
	return best
}
