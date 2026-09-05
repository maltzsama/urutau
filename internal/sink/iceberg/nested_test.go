package iceberg

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
)

func compositeDataSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cust", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		)},
		{Name: "tags", Type: arrow.ListOfNonNullable(arrow.BinaryTypes.String)},
		{Name: "attrs", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
	}, nil)
}

// dataRecord materializes composite values (struct/list/map as nested
// map[string]any / []any) into the typed arrow record.
func TestDataRecordComposite(t *testing.T) {
	w := &TableWriter{
		dataSchema: compositeDataSchema(),
		metaByName: map[string]core.MetadataColumn{},
		cast:       core.CastPolicy{},
	}
	rows := []change.Change{
		{Op: change.OpInsert, After: map[string]any{
			"id":    int64(1),
			"cust":  map[string]any{"name": "ana", "age": int64(30)},
			"tags":  []any{"a", "b"},
			"attrs": map[string]any{"x": int64(1)},
		}},
		{Op: change.OpInsert, After: map[string]any{
			"id":    int64(2),
			"cust":  map[string]any{"name": "bob", "age": int64(40)},
			"tags":  []any{},
			"attrs": map[string]any{},
		}},
	}
	rec, err := w.dataRecord(rows)
	if err != nil {
		t.Fatalf("dataRecord: %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", rec.NumRows())
	}

	// Struct column: read the two struct rows back.
	structCol := rec.Column(1).(*array.Struct)
	if structCol.Len() != 2 {
		t.Fatalf("struct rows = %d, want 2", structCol.Len())
	}
	nameCol := structCol.Field(0).(*array.String)
	if nameCol.Value(0) != "ana" || nameCol.Value(1) != "bob" {
		t.Fatalf("names = %v", nameCol)
	}
	ageCol := structCol.Field(1).(*array.Int64)
	if ageCol.Value(0) != 30 || ageCol.Value(1) != 40 {
		t.Fatalf("ages = %v", ageCol)
	}

	// List column: row 0 has 2 items, row 1 none (via offsets).
	listCol := rec.Column(2).(*array.List)
	s0, e0 := listCol.ValueOffsets(0)
	s1, e1 := listCol.ValueOffsets(1)
	if e0-s0 != 2 || e1-s1 != 0 {
		t.Fatalf("list lengths = %d/%d, want 2/0", e0-s0, e1-s1)
	}
	items := listCol.ListValues().(*array.String)
	if items.Value(0) != "a" || items.Value(1) != "b" {
		t.Fatalf("list items = %v", items)
	}

	// Map column: 2 map rows; row 0 has one entry, row 1 an empty map
	// (total keys = 1).
	mapCol := rec.Column(3).(*array.Map)
	if mapCol.Len() != 2 {
		t.Fatalf("map rows = %d, want 2", mapCol.Len())
	}
	if keys := mapCol.Keys(); keys.Len() != 1 {
		t.Fatalf("map total keys = %d, want 1", keys.Len())
	}
}

// A whole composite column may be null per row; only the non-null rows build
// children.
func TestDataRecordCompositeNulls(t *testing.T) {
	w := &TableWriter{dataSchema: compositeDataSchema(), metaByName: map[string]core.MetadataColumn{}, cast: core.CastPolicy{}}
	rows := []change.Change{
		{Op: change.OpInsert, After: map[string]any{"id": int64(1), "cust": nil, "tags": nil, "attrs": nil}},
	}
	rec, err := w.dataRecord(rows)
	if err != nil {
		t.Fatalf("dataRecord: %v", err)
	}
	defer rec.Release()
	if rec.Column(1).IsNull(0) != true {
		t.Fatal("null struct row must be null")
	}
	if rec.Column(2).IsNull(0) != true {
		t.Fatal("null list row must be null")
	}
}
