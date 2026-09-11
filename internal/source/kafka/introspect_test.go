package kafka

import (
	"context"
	"slices"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/spec"
)

// The spec declares columns as a map with no order; introspection must
// resolve a deterministic column order, or two boots of the same spec
// produce differently ordered target tables and schemas.
func TestIntrospectDeterministicColumnOrder(t *testing.T) {
	s := Source{}
	tbl := spec.Table{
		Source:     "shop.users",
		Target:     "raw.users",
		PrimaryKey: []string{"id"},
		Columns: map[string]spec.ColumnDecl{
			"id":     {Scalar: "int64"},
			"name":   {Scalar: "string"},
			"uid":    {Scalar: "uuid"},
			"active": {Scalar: "bool"},
		},
	}

	var first []string
	for i := 0; i < 20; i++ {
		_, cs, _, err := s.Introspect(context.Background(), tbl)
		if err != nil {
			t.Fatalf("introspect %d: %v", i, err)
		}
		names := make([]string, 0, len(cs.Columns))
		for _, c := range cs.Columns {
			names = append(names, c.Name)
		}
		if first == nil {
			first = names
			continue
		}
		if !slices.Equal(first, names) {
			t.Fatalf("introspection %d: columns %v, want stable %v", i, names, first)
		}
	}
}

// A nested struct/list column declared in the spec resolves through
// Introspect into a canonical core.ColumnType with the composite Kind and
// its Fields/Elem populated — the wiring Decision 1 (issue #17) needed.
func TestIntrospectResolvesNestedColumns(t *testing.T) {
	s := Source{}
	tbl := spec.Table{
		Source:     "shop.orders",
		Target:     "raw.orders",
		PrimaryKey: []string{"id"},
		Columns: map[string]spec.ColumnDecl{
			"id": {Scalar: "int64"},
			"address": {Struct: map[string]spec.ColumnDecl{
				"street": {Scalar: "string"},
				"city":   {Scalar: "string"},
			}},
			"tags": {List: &spec.ColumnDecl{Scalar: "string"}},
		},
	}
	_, cs, _, err := s.Introspect(context.Background(), tbl)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	addr, ok := cs.Column("address")
	if !ok || addr.Type.Kind != core.KindStruct {
		t.Fatalf("address = %+v, ok=%v, want struct", addr, ok)
	}
	if len(addr.Type.Fields) != 2 {
		t.Fatalf("address fields = %d, want 2", len(addr.Type.Fields))
	}
	tags, ok := cs.Column("tags")
	if !ok || tags.Type.Kind != core.KindList {
		t.Fatalf("tags = %+v, ok=%v, want list", tags, ok)
	}
	if tags.Type.Elem == nil || tags.Type.Elem.Kind != core.KindString {
		t.Fatalf("tags elem = %+v", tags.Type.Elem)
	}
}
