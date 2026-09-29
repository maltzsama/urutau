package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// A chunk counts as committed once the worker's position index no longer
// holds its Closes marker, and an ack releases every held batch its position
// covers. A marker positioned at the last batch sent before it (#468) was
// released by that batch's own ack while the window's rows were still in the
// worker's memory: the worker was OOM-killed next, the chunk was taken for
// committed and never redone (chaos-race-cfe4699: 38 chunks, 271,112 rows of
// pr_events). A Closes marker must stay held past the ack of any batch sent
// before it.
func TestAClosesMarkerIsNotReleasedByTheAckOfABatchSentBeforeIt(t *testing.T) {
	at := closesMarkerPos(t, "0/50")
	idx := newPositionIndex("run-1")
	idx.add(inflightBatch{id: 1, table: "raw.orders", high: position.MustLSN("0/50")})
	idx.add(inflightBatch{id: 2, table: "raw.orders", high: position.MustLSN(at)})
	idx.truncate("raw.orders", position.MustLSN("0/50")) // the stream batch commits
	if !idx.holds(2) {
		t.Fatalf("Closes marker at %s released by the ack of the batch sent before it", at)
	}
}
