package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/spec"
)

func projectionTable() *schema.Table {
	return &schema.Table{
		Schema: "shop", Name: "orders",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "v", Type: schema.TYPE_STRING},
			{Name: "amount", Type: schema.TYPE_DECIMAL},
			{Name: "active", Type: schema.TYPE_NUMBER},
		},
		PKColumns: []int{0},
	}
}

func TestProjectionKeepNumeric(t *testing.T) {
	p, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "active", Op: spec.OpEq, Value: float64(1)},
	}, projectionTable())
	if err != nil {
		t.Fatal(err)
	}
	ok, err := p.keep(map[string]any{"active": int64(1)})
	if err != nil || !ok {
		t.Fatalf("keep(active=1) = %v, %v; want true", ok, err)
	}
	ok, err = p.keep(map[string]any{"active": int64(0)})
	if err != nil || ok {
		t.Fatalf("keep(active=0) = %v, %v; want false", ok, err)
	}
	// NULL never satisfies a comparison.
	ok, err = p.keep(map[string]any{"active": nil})
	if err != nil || ok {
		t.Fatalf("keep(active=NULL) = %v, %v; want false", ok, err)
	}
}

func TestProjectionKeepDecimalExact(t *testing.T) {
	// DECIMAL compares exactly: a value one ulp past 100.5 must pass, and the
	// boundary itself must not.
	p, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGt, Value: "100.5"},
	}, projectionTable())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		amount string
		want   bool
	}{
		{"100.6", true},
		{"100.5000001", true},
		{"100.50", false},
		{"100.4", false},
	} {
		ok, err := p.keep(map[string]any{"amount": tc.amount})
		if err != nil || ok != tc.want {
			t.Fatalf("keep(amount=%s) = %v, %v; want %v", tc.amount, ok, err, tc.want)
		}
	}
}

func TestProjectionProject(t *testing.T) {
	p, err := newProjection([]string{"id", "v"}, nil, projectionTable())
	if err != nil {
		t.Fatal(err)
	}
	out := p.project(map[string]any{"id": int64(1), "v": "a", "amount": "9.9", "active": int64(1)})
	if len(out) != 2 || out["id"] != int64(1) || out["v"] != "a" {
		t.Fatalf("project = %+v, want {id,v}", out)
	}
	// No projection: the row is unchanged.
	empty, _ := newProjection(nil, nil, projectionTable())
	full := map[string]any{"id": int64(1)}
	if got := empty.project(full); len(got) != 1 {
		t.Fatalf("empty projection = %+v, want unchanged", got)
	}
}

func TestProjectionRejectsUnknownFilterColumn(t *testing.T) {
	_, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "nope", Op: spec.OpEq, Value: 1},
	}, projectionTable())
	if err == nil {
		t.Fatal("a filter on an unknown column must error")
	}
}
