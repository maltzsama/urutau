package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/maltzsama/urutau/internal/core"
)

// TestFromCanonicalRoundTrip maps a canonical schema into Iceberg and back
// via the primitive type, proving the canonical set survives the boundary.
func TestFromCanonicalRoundTrip(t *testing.T) {
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindFloat64}},
			{Name: "active", Type: core.ColumnType{Kind: core.KindBool}},
		},
		PrimaryKey: []string{"id"},
	}
	is, err := FromCanonical(cs)
	if err != nil {
		t.Fatalf("from canonical: %v", err)
	}
	if len(is.Fields()) != 4 {
		t.Fatalf("fields = %d, want 4", len(is.Fields()))
	}
	want := map[string]iceberg.Type{
		"id":     iceberg.PrimitiveTypes.Int64,
		"v":      iceberg.PrimitiveTypes.String,
		"amount": iceberg.PrimitiveTypes.Float64,
		"active": iceberg.PrimitiveTypes.Bool,
	}
	for _, f := range is.Fields() {
		if f.Type != want[f.Name] {
			t.Errorf("field %s type = %v, want %v", f.Name, f.Type, want[f.Name])
		}
	}
}

// TestUnmappableIsHardError asserts no silent coercion: a canonical kind the
// sink does not support is an error, not a string fallback.
func TestUnmappableIsHardError(t *testing.T) {
	cs := core.Schema{
		Columns: []core.Column{{Name: "x", Type: core.ColumnType{Kind: core.KindUnknown}}},
	}
	if _, err := FromCanonical(cs); err == nil {
		t.Fatal("unsupported canonical kind did not error")
	}
}
