package coordinator

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

	idx := newPositionIndex("run-1")
	idx.add(inflightBatch{id: 1, table: "t", high: n, bytes: 10})                // the last batch of transaction 5
	idx.add(inflightBatch{id: 2, table: "t", high: n, bytes: 20, partial: true}) // transaction 6, cut
	idx.add(inflightBatch{id: 3, table: "t", high: n, bytes: 30, partial: true})
	idx.add(inflightBatch{id: 4, table: "t", high: next, bytes: 40}) // its last batch

	freed, _, popped := idx.truncate("t", n)
	if len(popped) != 1 || popped[0] != 1 || freed != 10 {
		t.Fatalf("ack at %s popped %v (%d bytes), want only batch 1: the partial batches are not known committed", n, popped, freed)
	}
	freed, _, popped = idx.truncate("t", next)
	if len(popped) != 3 || freed != 90 {
		t.Fatalf("ack at %s popped %v (%d bytes), want batches 2, 3 and 4", next, popped, freed)
	}
	if idx.InFlight() != 0 {
		t.Fatalf("%d batches still in flight", idx.InFlight())
	}
}

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
