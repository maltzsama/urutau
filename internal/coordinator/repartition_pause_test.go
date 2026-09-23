package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/dataplane"
)

// #343: a paused table's batches are BUFFERED, not parked, so a batch for
// another table is not held behind them — the pump keeps draining the reader.
func TestPauseHoldBuffersOnlyThePausedTable(t *testing.T) {
	c, _ := scaleHarness(t)
	c.pauseTable("raw.orders")

	if !c.pauseHold(&dataplane.Batch{Table: "raw.orders"}) {
		t.Fatal("a paused table's batch must be held")
	}
	if c.pauseHold(&dataplane.Batch{Table: "raw.other"}) {
		t.Fatal("another table's batch must not be held")
	}

	// A batch for the resumed table that arrives before the flush still
	// queues behind the held ones, preserving order.
	c.resumeTable("raw.orders")
	if !c.pauseHold(&dataplane.Batch{Table: "raw.orders"}) {
		t.Fatal("a batch arriving before the flush must queue behind the held ones")
	}
	if got := len(c.pauseBuf["raw.orders"]); got != 2 {
		t.Fatalf("held batches = %d, want 2", got)
	}
}

// A batch for a table that was never paused is routed directly.
func TestPauseHoldPassesThroughUnpausedTable(t *testing.T) {
	c, _ := scaleHarness(t)
	if c.pauseHold(&dataplane.Batch{Table: "raw.orders"}) {
		t.Fatal("an unpaused table's batch must not be held")
	}
}
