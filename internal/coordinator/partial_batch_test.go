package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// The boot replay guard mirrors the worker's rule.
func TestPartialBatchAtTheBootPositionIsExpected(t *testing.T) {
	c, _ := coordHarness()
	c.bootCommitted = map[string]position.Position{"raw.orders": position.MustLSN("0/10")}
	if c.coveredAtBoot("raw.orders", "0/10", true) {
		t.Fatal("a partial batch at the committed position passes for covered: its worker applies it, and the cycle must expect it")
	}
	if !c.coveredAtBoot("raw.orders", "0/10", false) {
		t.Fatal("a whole batch at the committed position is covered")
	}
	if !c.coveredAtBoot("raw.orders", "0/F", true) {
		t.Fatal("a partial batch strictly before the committed position is covered")
	}
}
