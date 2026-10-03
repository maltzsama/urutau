package postgres

import (
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/spec"
)

func TestProjectionKeep(t *testing.T) {
	st := &TableState{Columns: []Column{
		{Name: "id", DataType: "bigint"},
		{Name: "name", DataType: "text"},
		{Name: "status", DataType: "text"},
		{Name: "secret", DataType: "text"},
	}}
	p, err := newProjection([]string{"id", "name"}, &spec.Filter{
		Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"},
	}, st)
	if err != nil {
		t.Fatal(err)
	}
	row := rowPos(st, map[string]any{"id": int64(1), "name": "a", "status": "active", "secret": "x"})

	keep, err := p.keep(row, st)
	if err != nil {
		t.Fatal(err)
	}
	if !keep {
		t.Fatal("row must satisfy the filter")
	}

	// A row outside the filter is dropped.
	row = rowPos(st, map[string]any{"id": int64(1), "name": "a", "status": "banned"})
	keep, err = p.keep(row, st)
	if err != nil {
		t.Fatal(err)
	}
	if keep {
		t.Fatal("row outside the filter must be dropped")
	}
}

func TestProjectionEmptyFilterKeepsAll(t *testing.T) {
	p := Projection{}
	keep, err := p.keep(nil, nil)
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
