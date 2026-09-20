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

// #187: changeOpString uses the rowchange constants and rejects an unknown op.
func TestChangeOpString(t *testing.T) {
	for _, tc := range []struct {
		op   rowchange.Op
		want string
	}{
		{rowchange.OpInsert, "insert"},
		{rowchange.OpUpdate, "update"},
		{rowchange.OpDelete, "delete"},
	} {
		got, err := changeOpString(uint8(tc.op))
		if err != nil || got != tc.want {
			t.Fatalf("changeOpString(%d) = %q, %v; want %q", tc.op, got, err, tc.want)
		}
	}
	if _, err := changeOpString(99); err == nil {
		t.Fatal("an unknown op must error, not become delete")
	}
}

// #187: splitByOp rejects a NULL __op instead of routing it to the upsert side.
func TestSplitByOpNullOpErrors(t *testing.T) {
	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(int64(1))
	bld.Field(1).(*array.StringBuilder).Append("x")
	bld.Field(2).(*array.Uint8Builder).AppendNull()
	bld.Field(3).(*array.StringBuilder).Append("p")
	bld.Field(4).AppendNull()
	bld.Field(5).AppendNull()
	bld.Field(6).(*array.BooleanBuilder).Append(false)
	bld.Field(7).(*array.StringBuilder).Append("stream")
	rec := bld.NewRecordBatch()
	defer rec.Release()

	b := &dataplane.Batch{Table: "t", Record: rec, Mode: dataplane.UpsertMode}
	if _, _, err := splitByOp(context.Background(), b); err == nil {
		t.Fatal("a NULL __op must error")
	}
}
