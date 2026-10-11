package inflight

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// A chunk counts as committed once the worker's index no longer holds its
// Closes marker. Released by a position ack, the marker went with the ack of
// a batch that merely shared or passed its position, while the window's rows
// were still in the worker's memory (#468; chaos-race-cfe4699 lost 38 chunks
// of pr_events). A marker leaves the index only on the ack of its own id,
// sent once the window's rows are committed; the batches behind it then
// leave as their position is acked.
func TestAClosesMarkerLeavesTheIndexOnlyOnItsOwnAck(t *testing.T) {
	idx := NewPositionIndex("run-1")
	idx.Add(Batch{ID: 1, Table: "raw.orders", High: position.MustLSN("0/50")})
	idx.Add(Batch{ID: 2, Table: "raw.orders", High: position.MustLSN("0/40"), Marker: true})
	idx.Add(Batch{ID: 3, Table: "raw.orders", High: position.MustLSN("0/60")})

	idx.Truncate("raw.orders", position.MustLSN("0/90")) // a stream commit past all three
	if idx.Holds(1) {
		t.Fatal("the batch ahead of the marker was not released by the ack covering it")
	}
	if !idx.Holds(2) {
		t.Fatal("the Closes marker was released by a position ack")
	}
	if !idx.Holds(3) {
		t.Fatal("a batch behind the held marker was released out of order")
	}

	idx.ReleaseMarker(2) // the window's rows are committed
	if idx.Holds(2) || idx.Holds(3) {
		t.Fatalf("after the marker's own ack: holds(2)=%v holds(3)=%v, want both released", idx.Holds(2), idx.Holds(3))
	}
}
