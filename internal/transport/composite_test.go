package transport

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/internal/core"
)

// A two-level struct round-trips through the Arrow table schema with its
// shape intact (including a uuid nested inside).
func TestCompositeSchemaRoundTrip(t *testing.T) {
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "customer", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "uid", Type: core.ColumnType{Kind: core.KindUUID}},
			{Name: "address", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
				{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
			}}},
		}}},
		{Name: "tags", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindString}}},
		{Name: "attrs", Type: core.ColumnType{Kind: core.KindMap,
			KeyType:   &core.ColumnType{Kind: core.KindString},
			ValueType: &core.ColumnType{Kind: core.KindInt64}}},
	}}

	sch, err := CoreSchemaToArrow(cs)
	if err != nil {
		t.Fatalf("to arrow: %v", err)
	}
	// The struct child uuid keeps its extension label inside the struct.
	cust := fieldByName(t, sch, "customer")
	st := cust.Type.(*arrow.StructType)
	var uid arrow.Field
	for _, f := range st.Fields() {
		if f.Name == "uid" {
			uid = f
		}
	}
	if uid.Name == "" {
		t.Fatal("uid field not found in struct")
	}
	if v, ok := uid.Metadata.GetValue("ARROW:extension:name"); !ok || v != "arrow.uuid" {
		t.Fatalf("nested uid metadata = %v, want arrow.uuid", uid.Metadata)
	}

	// Encode/decode through IPC schema bytes.
	b, err := EncodeTableSchema(cs)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeTableSchema(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(got.Columns))
	}
	custGot := got.Columns[1]
	if custGot.Type.Kind != core.KindStruct || len(custGot.Type.Fields) != 3 {
		t.Fatalf("customer = %+v, want struct with 3 fields", custGot.Type)
	}
	nestedUID := custGot.Type.Fields[1].Type
	if nestedUID.Kind != core.KindUUID {
		t.Fatalf("nested uid kind = %v, want uuid (label survived inside the struct)", nestedUID.Kind)
	}
	addr := custGot.Type.Fields[2].Type
	if addr.Kind != core.KindStruct || len(addr.Fields) != 1 || addr.Fields[0].Name != "city" {
		t.Fatalf("address = %+v, want nested struct with city", addr)
	}
	if got.Columns[2].Type.Kind != core.KindList || got.Columns[2].Type.Elem.Kind != core.KindString {
		t.Fatalf("tags = %+v, want list of string", got.Columns[2].Type)
	}
	m := got.Columns[3].Type
	if m.Kind != core.KindMap || m.KeyType.Kind != core.KindString || m.ValueType.Kind != core.KindInt64 {
		t.Fatalf("attrs = %+v, want map<string,int64>", m)
	}
}
