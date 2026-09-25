package iceberg

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"
)

// A NULL for a required Iceberg column has no representation: Parquet writes
// the value slot instead, so a NULL landed as "" (or 0). A table created
// before the MySQL source reported nullability has every column required, so
// the write must fail loud and say how to fix the table.
func TestProjectRecordRejectsNullInRequiredColumn(t *testing.T) {
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "note", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}, PrimaryKey: []string{"id"}}
	wire, err := transport.CoreSchemaToArrow(cs)
	if err != nil {
		t.Fatalf("wire schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, wire)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2}, nil)
	bld.Field(1).(*array.StringBuilder).Append("x")
	bld.Field(1).AppendNull()
	for j := 2; j < int(wire.NumFields()); j++ {
		bld.Field(j).AppendNulls(2)
	}
	rec := bld.NewRecordBatch()
	defer rec.Release()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("p"), Mode: dataplane.UpsertMode}

	required := &TableWriter{dataSchema: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil), metaByName: map[string]core.MetadataColumn{}}
	if _, err := required.projectRecord(context.Background(), b); err == nil || !strings.Contains(err.Error(), `"note"`) {
		t.Fatalf("projectRecord = %v, want an error naming the required column", err)
	}

	optional := &TableWriter{dataSchema: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil), metaByName: map[string]core.MetadataColumn{}}
	out, err := optional.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord into an optional column: %v", err)
	}
	defer out.Release()
	if !out.Column(1).IsNull(1) {
		t.Fatal("the NULL must stay NULL")
	}
}
