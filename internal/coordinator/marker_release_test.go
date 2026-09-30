package coordinator

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
	idx := newPositionIndex("run-1")
	idx.add(inflightBatch{id: 1, table: "raw.orders", high: position.MustLSN("0/50")})
	idx.add(inflightBatch{id: 2, table: "raw.orders", high: position.MustLSN("0/40"), marker: true})
	idx.add(inflightBatch{id: 3, table: "raw.orders", high: position.MustLSN("0/60")})

	idx.truncate("raw.orders", position.MustLSN("0/90")) // a stream commit past all three
	if idx.holds(1) {
		t.Fatal("the batch ahead of the marker was not released by the ack covering it")
	}
	if !idx.holds(2) {
		t.Fatal("the Closes marker was released by a position ack")
	}
	if !idx.holds(3) {
		t.Fatal("a batch behind the held marker was released out of order")
	}

	idx.releaseMarker(2) // the window's rows are committed
	if idx.holds(2) || idx.holds(3) {
		t.Fatalf("after the marker's own ack: holds(2)=%v holds(3)=%v, want both released", idx.holds(2), idx.holds(3))
	}
}

// commitEverything is a fake worker committing everything it holds: its
// stream batches through pos, and every window, whose marker it acks by id.
func commitEverything(idx *positionIndex, table string, pos position.Position) {
	idx.truncate(table, pos)
	idx.mu.Lock()
	var markers []uint64
	for _, h := range idx.head {
		if h.marker {
			markers = append(markers, h.id)
		}
	}
	idx.mu.Unlock()
	for _, id := range markers {
		idx.releaseMarker(id)
	}
}
