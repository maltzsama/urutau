package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// The confirmed position reported to the source must use the position's own
// ordering: "0/10" sorts before "0/2" lexicographically while 16 follows 2
// numerically, and a string min would advance the Postgres slot past data
// still in flight.
func TestConfirmedPositionUsesPositionOrdering(t *testing.T) {
	c := &Coordinator{confirmed: make(map[string]position.Position)}

	c.recordConfirmed("raw.a", position.MustLSN("0/10"))
	c.recordConfirmed("raw.b", position.MustLSN("0/2"))

	if got := c.confirmedPosition().String(); got != "0/2" {
		t.Fatalf("confirmed = %q, want 0/2 — the true minimum", got)
	}
}

// Nothing acked yet means nil: the source's retention must not advance.
func TestConfirmedPositionEmptyIsNil(t *testing.T) {
	c := &Coordinator{confirmed: make(map[string]position.Position)}
	if c.confirmedPosition() != nil {
		t.Fatal("confirmed = non-nil with no acks, want nil")
	}
}
