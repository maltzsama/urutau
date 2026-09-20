package iceberg

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// #200.1: Properties must return a copy, not the table's live property map.
func TestPropertiesReturnsCopy(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)
	ctx := context.Background()

	if err := s.SetProperties(ctx, ref, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	props, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	props["k"] = "mutated"
	again, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if again["k"] != "v" {
		t.Fatalf("mutating the returned map changed the table: k=%q", again["k"])
	}
}

// #200.2: splitByOp must carry every batch field (Mode, SnapshotState,
// SnapshotPending), not only Table/Watermark.
func TestSplitByOpCopiesBatchFields(t *testing.T) {
	b := wireBatch(t, [3]any{int64(1), "x", rowchange.OpInsert})
	b.Mode = dataplane.AppendMode
	b.SnapshotState = "in_progress"
	b.SnapshotPending = []uint32{1, 2}
	defer b.Release()

	up, del, err := splitByOp(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if del != nil {
		t.Fatalf("an all-insert batch must have no delete side, got %v", del)
	}
	if up == nil {
		t.Fatal("expected an upsert side")
	}
	if up.Mode != dataplane.AppendMode || up.SnapshotState != "in_progress" || len(up.SnapshotPending) != 2 {
		t.Fatalf("split batch lost fields: mode=%v state=%q pending=%v", up.Mode, up.SnapshotState, up.SnapshotPending)
	}
}

// #200.3: a required target column absent from the wire must error, not null.
func TestProjectRequiredMissingColumnErrors(t *testing.T) {
	w := testWriter() // dataSchema: id (required), v, _op
	// A wire schema without the required "id".
	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, data)
	defer bld.Release()
	rec := bld.NewRecordBatch()
	defer rec.Release()
	b := &dataplane.Batch{Table: "t", Record: rec, Mode: dataplane.UpsertMode}

	if _, err := w.projectRecord(context.Background(), b); err == nil {
		t.Fatal("a required column missing from the wire must error")
	}
}
