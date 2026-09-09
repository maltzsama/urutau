package worker

// Nested schema-drift detection is M4-gated at the pipeline level (the
// known-schema bridge drops extra nested fields before the check can see
// them), so the logic is covered HERE against raw row maps — the exact
// shapes a source-native batch would deliver.

import (
	"testing"

	"github.com/maltzsama/urutau/core"
)

func nestedSchema() core.Schema {
	return core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "address", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "geo", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
				{Name: "lat", Type: core.ColumnType{Kind: core.KindFloat64}},
			}}},
		}}},
	}}
}

func TestCheckDriftNestedReportsDeepPath(t *testing.T) {
	after := map[string]any{
		"id": int64(1),
		"address": map[string]any{
			"city": "sp",
			"geo":  map[string]any{"lat": -23.5, "extra": true},
		},
	}
	d, hit := checkDrift(after, nestedSchema())
	if !hit {
		t.Fatal("deep drift must be detected")
	}
	if d.Column != "address.geo.extra" {
		t.Fatalf("column = %q, want the full dotted path address.geo.extra", d.Column)
	}
}

func TestCheckDriftNestedTopLevelStructField(t *testing.T) {
	after := map[string]any{
		"id":      int64(1),
		"address": map[string]any{"city": "sp", "complement": "apto 4"},
	}
	d, hit := checkDrift(after, nestedSchema())
	if !hit {
		t.Fatal("drift must be detected")
	}
	if d.Column != "address.complement" {
		t.Fatalf("column = %q, want address.complement", d.Column)
	}
}

func TestCheckDriftNestedConformingValuePasses(t *testing.T) {
	after := map[string]any{
		"id":      int64(1),
		"address": map[string]any{"city": "sp", "geo": map[string]any{"lat": -23.5}},
	}
	if _, hit := checkDrift(after, nestedSchema()); hit {
		t.Fatal("a conforming nested value must not report drift")
	}
}

func TestCheckDriftTopLevelAddedColumn(t *testing.T) {
	after := map[string]any{"id": int64(1), "surprise": "new"}
	d, hit := checkDrift(after, nestedSchema())
	if !hit || d.Column != "surprise" || d.Kind != "added" {
		t.Fatalf("drift = %+v, want top-level surprise/added", d)
	}
}
