package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/maltzsama/urutau/source"
)

// A row whose JSON column arrived as a partial diff must be rejected: the
// full document is not in the binlog, so decoding it would silently corrupt.
func TestRejectPartialJSON(t *testing.T) {
	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders"}
	if err := rejectPartialJSON(ref, []any{"ok", int64(1), nil}); err != nil {
		t.Fatalf("a complete row image was rejected: %v", err)
	}
	diff := &replication.JsonDiff{}
	if err := rejectPartialJSON(ref, []any{"ok", diff}); err == nil {
		t.Fatal("a partial JSON diff was accepted")
	}
	if err := rejectPartialJSON(ref, []any{[]*replication.JsonDiff{diff}}); err == nil {
		t.Fatal("a partial JSON diff slice was accepted")
	}
}
