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

	c.recordConfirmed("w1", position.MustLSN("0/10"))
	c.recordConfirmed("w2", position.MustLSN("0/2"))

	if got := c.confirmedPosition().String(); got != "0/2" {
		t.Fatalf("confirmed = %q, want 0/2 — the true minimum", got)
	}
}

// WK-001 §2.2 (C6): two workers of the SAME partitioned table each hold
// their own position; the minimum is the lagging one, not the last acker.
// Before this fix the map was keyed by table, so the second ack overwrote
// the first and the Postgres slot advanced past WAL the lagging partition
// had not read. Fails on the pre-C6 code.
func TestConfirmedPositionMinAcrossWorkersOfSameTable(t *testing.T) {
	c := &Coordinator{confirmed: make(map[string]position.Position)}

	c.recordConfirmed("orders-raw.orders-0", position.MustLSN("0/100"))
	c.recordConfirmed("orders-raw.orders-1", position.MustLSN("0/2"))

	if got := c.confirmedPosition().String(); got != "0/2" {
		t.Fatalf("confirmed = %q, want 0/2 (the lagging worker)", got)
	}
}

// Nothing acked yet means nil: the source's retention must not advance.
func TestConfirmedPositionEmptyIsNil(t *testing.T) {
	c := &Coordinator{confirmed: make(map[string]position.Position)}
	if c.confirmedPosition() != nil {
		t.Fatal("confirmed = non-nil with no acks, want nil")
	}
}
