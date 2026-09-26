package runner

import (
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/position"
)

func TestCanonicalForTargetFound(t *testing.T) {
	canonical := map[string]core.Schema{
		"src.orders": {Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}}},
	}
	refs := []core.TableRef{{Source: "src.orders", Target: "orders"}}
	got := canonicalForTarget(canonical, refs, "orders")
	if len(got.Columns) != 1 || got.Columns[0].Name != "id" {
		t.Errorf("canonicalForTarget = %v", got)
	}
}

func TestCanonicalForTargetNotFound(t *testing.T) {
	canonical := map[string]core.Schema{}
	refs := []core.TableRef{}
	got := canonicalForTarget(canonical, refs, "nonexistent")
	if len(got.Columns) != 0 {
		t.Errorf("not found should return empty schema, got %v", got)
	}
}

func TestResumeOrNoneWithNil(t *testing.T) {
	got := position.StringOrNone(nil)
	if got != "none" {
		t.Errorf("position.StringOrNone(nil) = %q, want none", got)
	}
}
