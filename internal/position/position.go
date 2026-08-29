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
