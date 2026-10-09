package filterexpr

import (
	"strings"
	"testing"

	"github.com/maltzsama/urutau/spec"
)

// stubBuilder renders each leaf as its column name.
type stubBuilder struct{}

func (stubBuilder) Predicate(p *spec.Predicate) (string, error) {
	return "col:" + p.Column, nil
}

type stubFinder map[string]int

func (f stubFinder) FindColumn(name string) int {
	if i, ok := f[name]; ok {
		return i
	}
	return -1
}

func TestSourceTraversal(t *testing.T) {
	f := &spec.Filter{All: []spec.Filter{
		{Predicate: &spec.Predicate{Column: "a", Op: spec.OpEq, Value: 1}},
		{Any: []spec.Filter{
			{Predicate: &spec.Predicate{Column: "b"}},
			{Predicate: &spec.Predicate{Column: "c"}},
		}},
	}}
	got, err := Source(f, "test", stubBuilder{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "(col:a) && ((col:b) || (col:c))" {
		t.Fatalf("Source = %q", got)
	}
}

func TestOperatorAndLiteral(t *testing.T) {
	if Operator(spec.OpGte) != ">=" || Operator(spec.OpEq) != "==" {
		t.Fatalf("Operator mapping wrong")
	}
	if s, _ := Literal("test", 1.0); s != "1.0" {
		t.Fatalf("Literal(1.0) = %q, want 1.0", s)
	}
	if s, _ := Literal("test", "x"); s != `"x"` {
		t.Fatalf("Literal string = %q", s)
	}
	if _, err := Literal("test", struct{}{}); err == nil || !strings.Contains(err.Error(), "test") {
		t.Fatalf("Literal(struct) err = %v, want a dialect-prefixed error", err)
	}
}

func TestCheckColumns(t *testing.T) {
	f := &spec.Filter{All: []spec.Filter{
		{Predicate: &spec.Predicate{Column: "a"}},
		{Predicate: &spec.Predicate{Column: "missing"}},
	}}
	// stubFinder.FindColumn returns -1 for anything not listed.
	if err := CheckColumns(f, stubFinder{"a": 0}); err == nil {
		t.Fatal("CheckColumns accepted a missing column")
	}
	if err := CheckColumns(f, stubFinder{"a": 0, "missing": 1}); err != nil {
		t.Fatalf("CheckColumns: %v", err)
	}
	if err := CheckColumns(nil, stubFinder{}); err != nil {
		t.Fatalf("nil filter: %v", err)
	}
}
