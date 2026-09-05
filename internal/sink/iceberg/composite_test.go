package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/maltzsama/urutau/core"
)

// nestedSchema is a two-level struct with a list of structs and a map.
func nestedSchema() core.Schema {
	return core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "customer", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "address", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
				{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
			}}},
		}}},
		{Name: "orders", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "total", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 10, Scale: 2}},
		}}}},
		{Name: "attrs", Type: core.ColumnType{Kind: core.KindMap,
			KeyType:   &core.ColumnType{Kind: core.KindString},
			ValueType: &core.ColumnType{Kind: core.KindInt64}}},
	}}
}

func topByID(t *testing.T, sch *iceberg.Schema, id int) iceberg.NestedField {
	t.Helper()
	for _, f := range sch.Fields() {
		if f.ID == id {
			return f
		}
	}
	t.Fatalf("top field %d missing", id)
	return iceberg.NestedField{}
}

func nestedByID(t *testing.T, st *iceberg.StructType, id int) iceberg.NestedField {
	t.Helper()
	for _, f := range st.FieldList {
		if f.ID == id {
			return f
		}
	}
	t.Fatalf("nested field %d missing", id)
	return iceberg.NestedField{}
}

// 034 §7.2: field IDs are allocated sequentially through the whole tree, so
// a field inside a nested struct has a stable, deterministic ID — adding a
// nested field later is additive evolution that older readers map by ID.
func TestFromCanonicalCompositeFieldIDs(t *testing.T) {
	sch, err := FromCanonical(nestedSchema())
	if err != nil {
		t.Fatalf("from canonical: %v", err)
	}

	// Field IDs are assigned depth-first: each field takes its own id before
	// its children, so id=1, customer=2 (children 3,4,5), orders=6 (element
	// 7, its struct's total 8), attrs=9 (key 10, value 11).
	ids := map[string]int{}
	for _, f := range sch.Fields() {
		ids[f.Name] = f.ID
	}
	if ids["id"] != 1 || ids["customer"] != 2 || ids["orders"] != 6 || ids["attrs"] != 9 {
		t.Fatalf("top-level ids = %v, want id=1 customer=2 orders=6 attrs=9 (DFS)", ids)
	}

	// customer struct: name=3, address=4; address struct: city=5.
	customer := topByID(t, sch, 2).Type.(*iceberg.StructType)
	if f := nestedByID(t, customer, 3); f.Name != "name" {
		t.Fatalf("customer field 3 = %s, want name", f.Name)
	}
	addr := nestedByID(t, customer, 4).Type.(*iceberg.StructType)
	if f := nestedByID(t, addr, 5); f.Name != "city" {
		t.Fatalf("address field 5 = %s, want city — the nested counter must continue", f.Name)
	}

	// orders list element is a struct: element id 7, total field 8.
	orders := topByID(t, sch, 6).Type.(*iceberg.ListType)
	if orders.ElementID != 7 {
		t.Fatalf("orders element id = %d, want 7", orders.ElementID)
	}
	if f := nestedByID(t, orders.Element.(*iceberg.StructType), 8); f.Name != "total" {
		t.Fatalf("orders.total should be field 8, got %s", f.Name)
	}

	// attrs map: key 10, value 11.
	attrs := topByID(t, sch, 9).Type.(*iceberg.MapType)
	if attrs.KeyID != 10 || attrs.ValueID != 11 {
		t.Fatalf("attrs key/value ids = %d/%d, want 10/11", attrs.KeyID, attrs.ValueID)
	}
}
