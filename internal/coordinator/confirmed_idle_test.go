package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// a worker with nothing in flight (idle table) must not pin the
// confirmed position; a worker that owes work holds it at its committed.
func TestConfirmedPositionIdleWorkerDoesNotPin(t *testing.T) {
	c, _ := coordHarness()
	idle := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = idle
	c.index["w1"] = newPositionIndex("run-1")
	c.confirmed["w0"] = position.MustLSN("0/100")
	c.confirmed["w1"] = position.MustLSN("0/10")

	// Both workers owe nothing and something has been dispatched: advance to
	// the dispatched frontier, not the idle worker's old 0/10.
	c.noteSent("raw.orders", "0/100")
	if got := c.confirmedPosition(); got == nil || got.String() != "0/100" {
		t.Fatalf("confirmed = %v, want 0/100 (idle worker must not pin it)", got)
	}

	// w1 owes a queued batch: it constrains the minimum at its committed.
	idle.queue <- queuedBatch{}
	if got := c.confirmedPosition(); got == nil || got.String() != "0/10" {
		t.Fatalf("confirmed = %v, want 0/10 (w1 owes work)", got)
	}
}

// Before anything is dispatched, the confirmed falls back to the boot
// baseline: the minimum over every committed position.
func TestConfirmedPositionBootBaseline(t *testing.T) {
	c, _ := coordHarness()
	c.confirmed["w0"] = position.MustLSN("0/50")
	c.confirmed["w1"] = position.MustLSN("0/30")
	if got := c.confirmedPosition(); got == nil || got.String() != "0/30" {
		t.Fatalf("confirmed = %v, want 0/30 (boot baseline)", got)
	}
}
