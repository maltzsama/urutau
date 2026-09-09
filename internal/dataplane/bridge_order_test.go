package dataplane_test

// T-13 (W-3 / H-7 / D-6): the bridge must preserve the batch's arrival
// order. With the old upserts+deletes buckets, the delete concatenated at
// the END, so the columnar collapse (last-write-wins) resurrected a deleted
// row: insert k → delete k → insert k won with the DELETE, killing a row
// that was legitimately re-inserted. With the single ordered slice, the
// final insert wins — the row stays.

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

func TestBridgeOrderInsertDeleteInsertWinsWithInsert(t *testing.T) {
	alloc := checkedAlloc(t)

	key := []any{int64(42)}
	cb := rowchange.Batch{
		Table: "t",
		Changes: []rowchange.Change{
			{Op: rowchange.OpInsert, Key: key, After: map[string]any{"id": int64(42), "v": "first"}},
			{Op: rowchange.OpDelete, Key: key},
			{Op: rowchange.OpInsert, Key: key, After: map[string]any{"id": int64(42), "v": "back"}},
		},
		Mode: rowchange.UpsertMode,
	}
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}

	dpb, err := dataplane.BatchFromChangeBatch(cb, cs)
	if err != nil {
		t.Fatalf("bridge: %v", err)
	}
	defer dpb.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), alloc, dpb, []string{"id"})
	if err != nil {
		t.Fatalf("collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()

	if dels != nil {
		t.Fatalf("re-inserted row must not be equality-deleted: %d delete rows", dels.Record.NumRows())
	}
	if ups == nil || ups.Record.NumRows() != 1 {
		t.Fatalf("want exactly 1 surviving row, got %v", ups)
	}
	// Winner must be the LAST insert (v = "back"), never the delete.
	vIdx := -1
	for i := range ups.Record.Schema().NumFields() {
		if ups.Record.Schema().Field(i).Name == "v" {
			vIdx = i
			break
		}
	}
	if vIdx < 0 {
		t.Fatal("v column missing")
	}
	if got := ups.Record.Column(vIdx).(*array.String).Value(0); got != "back" {
		t.Fatalf("winner value = %q, want %q — arrival order lost", got, "back")
	}
}
