package iceberg

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// testWriter builds a TableWriter directly with hand-constructed arrow
// schemas and a cast policy — no catalog, so the projection path
// (projectRecord) is testable in isolation.
func testWriter() *TableWriter {
	return &TableWriter{
		dataSchema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "v", Type: arrow.BinaryTypes.String},
			{Name: "_op", Type: arrow.BinaryTypes.String},
		}, nil),
		delSchema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
		delCols: []string{"id"},
		metaByName: map[string]core.MetadataColumn{
			"_op": {From: core.MetaOp, As: "_op"},
		},
		cast: core.CastPolicy{},
	}
}

// wireBatch builds a wire-schema batch from (id, v, op) triples.
func wireBatch(t *testing.T, rows ...[3]any) *dataplane.Batch {
	t.Helper()
	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, data)
	defer bld.Release()
	for _, r := range rows {
		id := r[0].(int64)
		bld.Field(0).(*array.Int64Builder).Append(id)
		if r[1] == nil {
			bld.Field(1).(*array.StringBuilder).AppendNull()
		} else {
			bld.Field(1).(*array.StringBuilder).Append(r[1].(string))
		}
		bld.Field(2).(*array.Uint8Builder).Append(uint8(r[2].(rowchange.Op)))
		bld.Field(3).(*array.StringBuilder).Append("p")
		bld.Field(4).AppendNull()
		bld.Field(5).AppendNull()
		bld.Field(6).(*array.BooleanBuilder).Append(false)
	}
	rec := bld.NewRecordBatch()
	return &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("p"), Mode: dataplane.UpsertMode}
}

// projectRecord fills source columns and metadata columns; a missing source
// column projects as NULL rather than failing.
func TestProjectColumnsAndMetadata(t *testing.T) {
	w := testWriter()
	b := wireBatch(t, [3]any{int64(7), "x", rowchange.OpUpdate}, [3]any{int64(1), nil, rowchange.OpInsert})
	defer b.Release()
	out, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
	}
	defer out.Release()

	ids := out.Column(0).(*array.Int64)
	if ids.Value(0) != 7 || ids.Value(1) != 1 {
		t.Fatalf("id column = %v", ids)
	}
	vs := out.Column(1).(*array.String)
	if vs.Value(0) != "x" {
		t.Fatalf("v column = %v", vs)
	}
	if !vs.IsNull(1) {
		t.Fatalf("missing column must project NULL, got %q", vs.Value(1))
	}
	ops := out.Column(2).(*array.String)
	if ops.Value(0) != "update" || ops.Value(1) != "insert" {
		t.Fatalf("_op = %q,%q, want update,insert", ops.Value(0), ops.Value(1))
	}
}

// projectRecord applies the declared cast to the source value — the
// regression guard for the columnar path silently skipping cast policies.
func TestProjectAppliesCast(t *testing.T) {
	cp, err := core.ParseCastPolicy(map[string]string{"v": "string(hex)"})
	if err != nil {
		t.Fatalf("cast: %v", err)
	}
	w := &TableWriter{
		dataSchema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "v", Type: arrow.BinaryTypes.String},
		}, nil),
		cast:       cp,
		metaByName: map[string]core.MetadataColumn{},
	}
	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindBinary, Nullable: true}},
	}})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	bld.Field(1).(*array.BinaryBuilder).Append([]byte{0xde, 0xad})
	for j := 2; j < int(data.NumFields()); j++ {
		bld.Field(j).AppendNull()
	}
	rec := bld.NewRecordBatch()
	defer rec.Release()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("p"), Mode: dataplane.UpsertMode}
	defer b.Release()

	out, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
	}
	defer out.Release()
	vs := out.Column(1).(*array.String)
	if vs.Value(0) != "dead" {
		t.Fatalf("v after cast = %v, want dead", vs.Value(0))
	}
}

// projectRecord materializes rows into a typed record, including metadata.
func TestProjectRecordRows(t *testing.T) {
	w := testWriter()
	b := wireBatch(t, [3]any{int64(1), "a", rowchange.OpInsert}, [3]any{int64(2), "b", rowchange.OpInsert})
	defer b.Release()
	rec, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", rec.NumRows())
	}
	ids := rec.Column(0).(*array.Int64)
	if ids.Value(0) != 1 || ids.Value(1) != 2 {
		t.Fatalf("id column = %v", ids)
	}
}

// deleteRecord projects key tuples onto the delete key columns and rejects
// a key with the wrong arity — the guard that stops a corrupted key from
// silently producing a wrong delete file.
func TestDeleteRecordArityGuard(t *testing.T) {
	w := testWriter()
	if _, err := w.deleteRecord([][]any{{int64(1)}, {}}); err == nil {
		t.Fatal("empty key tuple must be rejected")
	}
	if _, err := w.deleteRecord([][]any{{}}); err == nil {
		t.Fatal("arity-zero key must be rejected")
	}
	rec, err := w.deleteRecord([][]any{{int64(3)}, {int64(4)}})
	if err != nil {
		t.Fatalf("deleteRecord: %v", err)
	}
	defer rec.Release()
	col := rec.Column(0).(*array.Int64)
	if col.Value(0) != 3 || col.Value(1) != 4 {
		t.Fatalf("delete keys = %v", col)
	}
}

// appendColumn must refuse a value the column type cannot hold.
func TestAppendColumnTypeErrors(t *testing.T) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	defer b.Release()
	intField := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64}
	if err := appendColumn(b.Field(0), intField, []any{"not-a-number"}); err == nil {
		t.Fatal("string into int64 column must be rejected")
	}
	if err := appendColumn(b.Field(0), intField, []any{float64(1.5)}); err == nil {
		t.Fatal("fractional float into int64 column must be rejected")
	}
	if err := appendColumn(b.Field(0), intField, []any{nil, int64(1), int32(2)}); err != nil {
		t.Fatalf("valid ints rejected: %v", err)
	}
}
