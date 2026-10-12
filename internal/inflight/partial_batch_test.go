package inflight

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// An ack at position N says the table is durable through N. A partial batch
// stamped N belongs to the transaction after it and may not be in that
// commit: it stays in flight, to be redelivered if the worker is lost, until
// the table is acked beyond N. Popped at N, a worker that died before
// committing it never got it again (#823).
func TestPartialBatchLeavesOnlyOnAnAckBeyondItsPosition(t *testing.T) {
	const gtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-"
	n, next := position.MustGTID(gtid+"5"), position.MustGTID(gtid+"6")

	idx := NewPositionIndex("run-1")
	idx.Add(Batch{ID: 1, Table: "t", High: n, Bytes: 10})                // the last batch of transaction 5
	idx.Add(Batch{ID: 2, Table: "t", High: n, Bytes: 20, Partial: true}) // transaction 6, cut
	idx.Add(Batch{ID: 3, Table: "t", High: n, Bytes: 30, Partial: true})
	idx.Add(Batch{ID: 4, Table: "t", High: next, Bytes: 40}) // its last batch

	freed, _, popped := idx.Truncate("t", n)
	if len(popped) != 1 || popped[0] != 1 || freed != 10 {
		t.Fatalf("ack at %s popped %v (%d bytes), want only batch 1: the partial batches are not known committed", n, popped, freed)
	}
	freed, _, popped = idx.Truncate("t", next)
	if len(popped) != 3 || freed != 90 {
		t.Fatalf("ack at %s popped %v (%d bytes), want batches 2, 3 and 4", next, popped, freed)
	}
	if idx.InFlight() != 0 {
		t.Fatalf("%d batches still in flight", idx.InFlight())
	}
}
