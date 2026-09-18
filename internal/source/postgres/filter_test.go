package postgres

import (
	"strings"
	"testing"

	"github.com/maltzsama/urutau/spec"
)

func TestFilterToSquirrel(t *testing.T) {
	cases := []struct {
		name string
		f    *spec.Filter
		want string // WHERE fragment
	}{
		{"eq", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"}}, `"status" = $1`},
		{"neq", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpNeq, Value: "x"}}, `"status" <> $1`},
		{"lt", &spec.Filter{Predicate: &spec.Predicate{Column: "amount", Op: spec.OpLt, Value: float64(10)}}, `"amount" < $1`},
		{"gte", &spec.Filter{Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGte, Value: float64(10)}}, `"amount" >= $1`},
		{"bool", &spec.Filter{Predicate: &spec.Predicate{Column: "active", Op: spec.OpEq, Value: true}}, `"active" = $1`},
		{"is_null", &spec.Filter{Predicate: &spec.Predicate{Column: "x", Op: spec.OpIsNull}}, `"x" IS NULL`},
		{"is_not_null", &spec.Filter{Predicate: &spec.Predicate{Column: "x", Op: spec.OpIsNotNull}}, `"x" IS NOT NULL`},
		{"in", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpIn, Value: []any{"a", "b"}}}, `"status" IN ($1,$2)`},
		{"not_in", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpNotIn, Value: []any{"x"}}}, `"status" NOT IN ($1)`},
		{"all", &spec.Filter{All: []spec.Filter{
			{Predicate: &spec.Predicate{Column: "a", Op: spec.OpEq, Value: "1"}},
			{Predicate: &spec.Predicate{Column: "b", Op: spec.OpEq, Value: "2"}},
		}}, `("a" = $1 AND "b" = $2)`},
		{"any", &spec.Filter{Any: []spec.Filter{
			{Predicate: &spec.Predicate{Column: "a", Op: spec.OpEq, Value: "1"}},
			{Predicate: &spec.Predicate{Column: "b", Op: spec.OpEq, Value: "2"}},
		}}, `("a" = $1 OR "b" = $2)`},
		{"not", &spec.Filter{Not: &spec.Filter{Predicate: &spec.Predicate{Column: "a", Op: spec.OpEq, Value: "1"}}},
			`NOT ("a" = $1)`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			filter, err := filterToSquirrel(c.f)
			if err != nil {
				t.Fatal(err)
			}
			sqlStr, _, err := psql.Select("*").From("t").Where(filter).ToSql()
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(sqlStr, c.want) {
				t.Fatalf("sql %q missing %q", sqlStr, c.want)
			}
		})
	}
}

func TestProjectionFilterExpr(t *testing.T) {
	row := map[string]any{
		"status": "active",
		"amount": int64(150),
		"active": true,
		"note":   nil,
	}
	cases := []struct {
		name string
		f    *spec.Filter
		want bool
	}{
		{"eq_true", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"}}, true},
		{"eq_false", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "banned"}}, false},
		{"neq", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpNeq, Value: "banned"}}, true},
		{"gt_num", &spec.Filter{Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGt, Value: float64(100)}}, true},
		{"lte_num", &spec.Filter{Predicate: &spec.Predicate{Column: "amount", Op: spec.OpLte, Value: float64(100)}}, false},
		{"bool", &spec.Filter{Predicate: &spec.Predicate{Column: "active", Op: spec.OpEq, Value: true}}, true},
		{"is_null", &spec.Filter{Predicate: &spec.Predicate{Column: "note", Op: spec.OpIsNull}}, true},
		{"is_not_null", &spec.Filter{Predicate: &spec.Predicate{Column: "note", Op: spec.OpIsNotNull}}, false},
		{"null_never_matches", &spec.Filter{Predicate: &spec.Predicate{Column: "note", Op: spec.OpEq, Value: "x"}}, false},
		{"in", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpIn, Value: []any{"x", "active"}}}, true},
		{"not_in", &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpNotIn, Value: []any{"x"}}}, true},
		{"all", &spec.Filter{All: []spec.Filter{
			{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"}},
			{Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGt, Value: float64(100)}},
		}}, true},
		{"any", &spec.Filter{Any: []spec.Filter{
			{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "banned"}},
			{Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGt, Value: float64(100)}},
		}}, true},
		{"not", &spec.Filter{Not: &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"}}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := newProjection(nil, c.f)
			if err != nil {
				t.Fatal(err)
			}
			got, err := p.keep(row)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("keep = %v, want %v", got, c.want)
			}
		})
	}
}
