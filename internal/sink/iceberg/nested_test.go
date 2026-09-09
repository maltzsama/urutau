package iceberg

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"
)

func compositeDataSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cust", Type: arrow.StructOf(
			arrow.Field{Name: "name", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		)},
		{Name: "tags", Type: arrow.ListOfNonNullable(arrow.BinaryTypes.String)},
		// Non-nullable items: mirrors what CoreSchemaToArrow emits for a
		// map whose core ValueType is non-nullable, so projection retains
		// the wire column zero-copy.
		{Name: "attrs", Type: arrow.MapOfFields(
			arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		)},
	}, nil)
}

// compositeWireBatch builds a wire-schema batch carrying two composite
// rows (struct/list/map filled on row 0, empty on row 1).
func compositeWireBatch(t *testing.T) *dataplane.Batch {
	t.Helper()
	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "cust", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "age", Type: core.ColumnType{Kind: core.KindInt64}},
		}}},
		{Name: "tags", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindString}}},
		{Name: "attrs", Type: core.ColumnType{Kind: core.KindMap, KeyType: &core.ColumnType{Kind: core.KindString}, ValueType: &core.ColumnType{Kind: core.KindInt64}}},
	}})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, data)
	defer bld.Release()

	fillMeta := func() {
		for j := 5; j < int(data.NumFields()); j++ {
			bld.Field(j).AppendNull()
		}
	}
	// row 0: id=1, cust={ana,30}, tags=[a b], attrs={x:1}
	bld.Field(0).(*array.Int64Builder).Append(1)
	sb := bld.Field(1).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("ana")
	sb.FieldBuilder(1).(*array.Int64Builder).Append(30)
	lb := bld.Field(2).(*array.ListBuilder)
	lb.Append(true)
	for _, s := range []string{"a", "b"} {
		lb.ValueBuilder().(*array.StringBuilder).Append(s)
	}
	mb := bld.Field(3).(*array.MapBuilder)
	mb.Append(true)
	mb.KeyBuilder().(*array.StringBuilder).Append("x")
	mb.ItemBuilder().(*array.Int64Builder).Append(1)
	bld.Field(4).AppendNull()
	fillMeta()

	// row 1: id=2, cust={bob,40}, tags=[], attrs={}
	bld.Field(0).(*array.Int64Builder).Append(2)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.StringBuilder).Append("bob")
	sb.FieldBuilder(1).(*array.Int64Builder).Append(40)
	lb.Append(true)
	mb.Append(true)
	bld.Field(4).AppendNull()
	fillMeta()

	rec := bld.NewRecordBatch()
	return &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w"), Mode: dataplane.UpsertMode}
}

// Composite columns survive the columnar projection intact: matching types
// are retained zero-copy, nested structure and emptiness included.
func TestProjectRecordComposite(t *testing.T) {
	w := &TableWriter{
		dataSchema: compositeDataSchema(),
		metaByName: map[string]core.MetadataColumn{},
		cast:       core.CastPolicy{},
	}
	b := compositeWireBatch(t)
	defer b.Release()
	rec, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
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

// A whole composite column may be null per row; the projection preserves
// the row-level null.
func TestProjectRecordCompositeNulls(t *testing.T) {
	w := &TableWriter{dataSchema: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "cust", Type: arrow.StructOf(arrow.Field{Name: "name", Type: arrow.BinaryTypes.String})},
		{Name: "tags", Type: arrow.ListOfNonNullable(arrow.BinaryTypes.String)},
		{Name: "attrs", Type: arrow.MapOfFields(
			arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		)},
	}, nil), metaByName: map[string]core.MetadataColumn{}, cast: core.CastPolicy{}}
	data, err := transport.CoreSchemaToArrow(core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "cust", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
		}}},
		{Name: "tags", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindString}}},
		{Name: "attrs", Type: core.ColumnType{Kind: core.KindMap, KeyType: &core.ColumnType{Kind: core.KindString}, ValueType: &core.ColumnType{Kind: core.KindInt64}}},
	}})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, data)
	defer bld.Release()
	bld.Field(0).(*array.Int64Builder).Append(1)
	for j := 1; j < 4; j++ {
		bld.Field(j).AppendNull()
	}
	bld.Field(4).(*array.Uint8Builder).Append(0) // __op
	bld.Field(5).(*array.StringBuilder).Append("p")
	for j := 6; j < int(data.NumFields()); j++ {
		bld.Field(j).AppendNull()
	}
	rec2 := bld.NewRecordBatch()
	defer rec2.Release()
	b := &dataplane.Batch{Table: "t", Record: rec2, Watermark: []byte("w"), Mode: dataplane.UpsertMode}
	defer b.Release()
	rec, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
	}
	defer rec.Release()
	if rec.Column(1).IsNull(0) != true {
		t.Fatal("null struct row must be null")
	}
	if rec.Column(2).IsNull(0) != true {
		t.Fatal("null list row must be null")
	}
}
