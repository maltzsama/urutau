package iceberg

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
)

// testWriter builds a TableWriter directly with hand-constructed arrow
// schemas and a cast policy — no catalog, so the pure data path (project,
// dataRecord, deleteRecord) is testable in isolation.
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

// project fills source columns and metadata columns; a missing source
// column projects as NULL rather than failing.
func TestProjectColumnsAndMetadata(t *testing.T) {
	w := testWriter()
	c := change.Change{Op: change.OpUpdate, After: map[string]any{"id": int64(7), "v": "x"}}
	out, err := w.project(c)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if out["id"] != int64(7) || out["v"] != "x" {
		t.Fatalf("project = %v, want id=7 v=x", out)
	}
	if out["_op"] != "update" {
		t.Fatalf("_op = %v, want update", out["_op"])
	}
	// A change missing a declared column yields NULL for that column.
	sparse := change.Change{Op: change.OpInsert, After: map[string]any{"id": int64(1)}}
	out, err = w.project(sparse)
	if err != nil {
		t.Fatalf("project sparse: %v", err)
	}
	if out["v"] != nil {
		t.Fatalf("missing column = %v, want nil", out["v"])
	}
}

// project applies the declared cast to the source value.
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
	c := change.Change{After: map[string]any{"id": int64(1), "v": []byte{0xde, 0xad}}}
	out, err := w.project(c)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if out["v"] != "dead" {
		t.Fatalf("v after cast = %v, want dead", out["v"])
	}
}

// dataRecord materializes rows into a typed record, including metadata.
func TestDataRecord(t *testing.T) {
	w := testWriter()
	rows := []change.Change{
		{Op: change.OpInsert, After: map[string]any{"id": int64(1), "v": "a"}},
		{Op: change.OpInsert, After: map[string]any{"id": int64(2), "v": "b"}},
	}
	rec, err := w.dataRecord(rows)
	if err != nil {
		t.Fatalf("dataRecord: %v", err)
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
