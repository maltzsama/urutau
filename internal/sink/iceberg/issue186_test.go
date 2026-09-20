package iceberg

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// #186: in append mode WriteStaged must append the whole batch, including a
// delete row that carries its before-image. Before the fix, splitByOp routed
// that row into the delete bucket append mode never reads, and it was dropped.
func TestWriteStagedAppendKeepsDeleteRows(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)
	ctx := context.Background()

	w, err := NewTableWriter(ctx, s.cat, s.ident(ref.Target), ref.PrimaryKey, core.CastPolicy{}, nil, "shop.orders", 0)
	if err != nil {
		t.Fatalf("NewTableWriter: %v", err)
	}

	// A single delete row (carrying its before-image) in append mode.
	b := wireBatch(t, [3]any{int64(1), "kept", rowchange.OpDelete})
	b.Mode = dataplane.AppendMode
	defer b.Release()

	desc, err := w.WriteStaged(ctx, b)
	if err != nil {
		t.Fatalf("WriteStaged: %v", err)
	}

	tbl, err := s.cat.LoadTable(ctx, s.ident(ref.Target))
	if err != nil {
		t.Fatal(err)
	}
	p, err := decodeStaged(desc, tbl.Spec(), tbl.Schema(), tbl.Metadata().Version())
	if err != nil {
		t.Fatalf("decodeStaged: %v", err)
	}
	if len(p.appends) == 0 {
		t.Fatal("an append-mode delete row must be staged as an append, not dropped")
	}
	if len(p.deletes) != 0 {
		t.Fatalf("append mode must stage no equality deletes, got %d", len(p.deletes))
	}
}
