package remote

import (
	"log/slog"
	"testing"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
)

// Issue #372: a batch the worker skips as "covered" (at or before its committed
// position) must still be acked. The coordinator counts it as in-flight until
// acked, so skipping without acking leaks the in-flight, the supervisor sees a
// "stalled" worker and terminates the run for a clean replay, which redelivers
// the same batch and crashloops.
func TestCoveredBatchIsAcked(t *testing.T) {
	pos := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:5")
	recv := &batchReceiver{
		committed: map[string]position.Position{"t": pos},
		parsePos:  parsePosition("mysql"),
		log:       slog.New(slog.DiscardHandler),
	}
	var acked []string
	recv.ack = func(table, p string) { acked = append(acked, table+"|"+p) }

	// Covered (high == committed): skipped AND acked.
	if !recv.skipCovered(&pb.BatchMeta{Table: "t", HighPos: pos.String()}) {
		t.Fatal("a covered batch must be skipped")
	}
	if len(acked) != 1 || acked[0] != "t|"+pos.String() {
		t.Fatalf("acked = %v, want the covered batch acked (issue #372)", acked)
	}

	// Ahead of the committed position: not skipped, not acked.
	hi := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:9")
	if recv.skipCovered(&pb.BatchMeta{Table: "t", HighPos: hi.String()}) {
		t.Fatal("a batch ahead of the committed position must not be skipped")
	}
	if len(acked) != 1 {
		t.Fatalf("an uncovered batch must not be acked, acked=%v", acked)
	}
}

// A partial batch carries the position of the transaction before its own. A
// commit that landed between two batches of that transaction recorded this
// same position whether or not this batch was in it, so at the committed
// position it must be applied again: skipped, the rows of the batches that
// were not in the commit were lost for good (#823).
func TestPartialBatchAtTheCommittedPositionIsNotSkipped(t *testing.T) {
	pos := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	recv := &batchReceiver{
		committed: map[string]position.Position{"t": pos},
		parsePos:  parsePosition("mysql"),
		log:       slog.New(slog.DiscardHandler),
	}
	var acked []string
	recv.ack = func(table, p string) { acked = append(acked, table+"|"+p) }

	if recv.skipCovered(&pb.BatchMeta{Table: "t", HighPos: pos.String(), Partial: true}) {
		t.Fatal("a partial batch at the committed position was skipped: its rows may never have been committed")
	}
	if len(acked) != 0 {
		t.Fatalf("a batch that is applied must not be acked on receipt, acked=%v", acked)
	}

	// Strictly before the committed position its transaction is durable:
	// skipped, and acked with the committed position, the only one that
	// takes a partial batch out of the coordinator's in-flight index.
	before := position.MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3")
	if !recv.skipCovered(&pb.BatchMeta{Table: "t", HighPos: before.String(), Partial: true}) {
		t.Fatal("a partial batch strictly before the committed position must be skipped")
	}
	if len(acked) != 1 || acked[0] != "t|"+pos.String() {
		t.Fatalf("acked = %v, want the committed position %s", acked, pos)
	}
}
