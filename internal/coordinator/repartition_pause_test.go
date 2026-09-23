package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

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

// #343, end to end at the pump: a batch for a paused table must not PARK the
// pump. The pump is fed a paused table's batch and then a second table's batch
// that enqueueBatch rejects (no owner), which terminates the pump. With the
// old behaviour the pump parked on the first batch and never reached the
// second, so this would hang; with the buffering fix it reaches the second and
// terminates with the pump error.
func TestPumpDoesNotParkOnPausedTable(t *testing.T) {
	c, _ := scaleHarness(t)
	c.terminate = make(chan error, 1)
	c.pauseTable("raw.orders")

	out := make(chan *dataplane.Batch, 2)
	go c.pump(context.Background(), out)
	out <- &dataplane.Batch{Table: "raw.orders"}  // paused: buffered
	out <- &dataplane.Batch{Table: "raw.unknown"} // no owner: enqueueBatch fails

	select {
	case err := <-c.terminate:
		if err == nil || !strings.Contains(err.Error(), "pump") {
			t.Fatalf("want a pump error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pump parked on the paused table; it never processed the next table's batch")
	}
}
