package mysql

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// The reader advances its resume set as transactions stream in. It must do
// so on its own copy: the start position is the caller's object, and the
// coordinator keeps and reads that same object (its per-table committed
// position at boot). Mutating it in place raced those reads and moved the
// caller's "committed" position forward with the stream.
func TestReaderDoesNotMutateItsStartPosition(t *testing.T) {
	start := position.MustGTID("4acc018a-b8ce-11f1-a7c2-be924e253b21:1-100")
	r := &Reader{}
	r.setStart(start)
	r.mergeGTID(position.MustGTID("4acc018a-b8ce-11f1-a7c2-be924e253b21:101"))

	if got := start.String(); got != "4acc018a-b8ce-11f1-a7c2-be924e253b21:1-100" {
		t.Fatalf("start position mutated to %s", got)
	}
	if got := r.curSet.String(); got != "4acc018a-b8ce-11f1-a7c2-be924e253b21:1-101" {
		t.Fatalf("reader's own set = %s, want it advanced to 1-101", got)
	}
}
