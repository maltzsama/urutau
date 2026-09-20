package postgres

import (
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/spec"
)

func TestProjectionKeepAndProject(t *testing.T) {
	p, err := newProjection([]string{"id", "name"}, &spec.Filter{
		Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	full := map[string]any{"id": int64(1), "name": "a", "status": "active", "secret": "x"}

	keep, err := p.keep(full)
	if err != nil {
		t.Fatal(err)
	}
	if !keep {
		t.Fatal("row must satisfy the filter")
	}
	got := p.project(full)
	if len(got) != 2 || got["id"] != int64(1) || got["name"] != "a" {
		t.Fatalf("project = %v, want only id+name", got)
	}
	if _, ok := got["secret"]; ok {
		t.Fatal("projection leaked an unselected column")
	}

	// A row outside the filter is dropped.
	full["status"] = "banned"
	keep, err = p.keep(full)
	if err != nil {
		t.Fatal(err)
	}
	if keep {
		t.Fatal("row outside the filter must be dropped")
	}
}

func TestProjectionEmptyColumnsKeepsAll(t *testing.T) {
	p := Projection{}
	full := map[string]any{"id": int64(1), "name": "a"}
	if got := p.project(full); len(got) != 2 {
		t.Fatalf("empty projection must keep all columns, got %v", got)
	}
	keep, err := p.keep(full)
	if err != nil || !keep {
		t.Fatalf("empty filter must keep the row: keep=%v err=%v", keep, err)
	}
}

func TestFilterSchemaColumns(t *testing.T) {
	cs := core.Schema{
		PrimaryKey: []string{"id"},
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "secret", Type: core.ColumnType{Kind: core.KindString}},
		},
	}
	got := core.FilterSchemaColumns(cs, []string{"id", "name"})
	if len(got.Columns) != 2 || got.Columns[0].Name != "id" || got.Columns[1].Name != "name" {
		t.Fatalf("filterSchemaColumns = %v", got.Columns)
	}
	// Empty filter keeps all.
	if len(core.FilterSchemaColumns(cs, nil).Columns) != 3 {
		t.Fatal("empty filter must keep every column")
	}
}

func TestCheckColumnFilterCoversPK(t *testing.T) {
	if err := checkColumnFilterCoversPK(nil, []string{"id"}); err != nil {
		t.Fatalf("no filter: %v", err)
	}
	if err := checkColumnFilterCoversPK([]string{"id", "name"}, []string{"id"}); err != nil {
		t.Fatalf("pk covered: %v", err)
	}
	if err := checkColumnFilterCoversPK([]string{"name"}, []string{"id"}); err == nil {
		t.Fatal("want an error when the key column is excluded")
	}
}

func TestCheckColumnFilterExists(t *testing.T) {
	st := &TableState{Columns: []Column{{Name: "id"}, {Name: "name"}}}
	if err := checkColumnFilterExists(st, []string{"id", "name"}); err != nil {
		t.Fatalf("known columns: %v", err)
	}
	if err := checkColumnFilterExists(st, nil); err != nil {
		t.Fatalf("no filter: %v", err)
	}
	if err := checkColumnFilterExists(st, []string{"id", "nope"}); err == nil {
		t.Fatal("want an error for an unknown projected column")
	}
}
