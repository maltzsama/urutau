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

func testFilterState() *TableState {
	return &TableState{
		Schema: "public",
		Name:   "orders",
		Columns: []Column{
			{Name: "status", DataType: "text"},
			{Name: "amount", DataType: "numeric"},
			{Name: "active", DataType: "boolean"},
			{Name: "note", DataType: "text"},
		},
	}
}

func TestProjectionFilterExpr(t *testing.T) {
	st := testFilterState()
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
			p, err := newProjection(nil, c.f, st)
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

// A NULL column must never satisfy neq / not_in / not(...), matching SQL's
// three-valued logic (the snapshot WHERE excludes it, so CDC must too).
func TestFilterExprNullSemantics(t *testing.T) {
	st := testFilterState()
	row := map[string]any{"status": nil}
	cases := []*spec.Filter{
		{Predicate: &spec.Predicate{Column: "status", Op: spec.OpNeq, Value: "x"}},
		{Predicate: &spec.Predicate{Column: "status", Op: spec.OpNotIn, Value: []any{"x"}}},
		{Not: &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "x"}}},
		{Not: &spec.Filter{All: []spec.Filter{
			{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "x"}},
		}}},
	}
	for i, f := range cases {
		p, err := newProjection(nil, f, st)
		if err != nil {
			t.Fatal(err)
		}
		got, err := p.keep(row)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if got {
			t.Fatalf("case %d: NULL column must not satisfy the predicate", i)
		}
	}
}

// A numeric column is decoded as a decimal string; a numeric filter must
// still compare numerically, not lexically or with a type error.
func TestFilterExprNumericColumn(t *testing.T) {
	st := testFilterState()
	p, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGt, Value: float64(100)},
	}, st)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"150.00", true},  // 150 > 100
		{"9.00", false},   // lexical "9" > "100" would be true; numeric is false
		{"100.00", false}, // not strictly greater
	} {
		got, err := p.keep(map[string]any{"amount": tc.val})
		if err != nil {
			t.Fatalf("%s: %v", tc.val, err)
		}
		if got != tc.want {
			t.Fatalf("amount=%s: keep=%v, want %v", tc.val, got, tc.want)
		}
	}
}

// A `numeric` column is compared exactly: a value beyond float64's 53-bit
// integer precision must not collapse onto a neighbouring literal.
func TestFilterExprNumericExactPrecision(t *testing.T) {
	st := testFilterState()
	// 9007199254740993 = 2^53+1 is not representable as float64 (it rounds to
	// 2^53), so a float comparison would call it equal to 9007199254740992.
	p, err := newProjection(nil, &spec.Filter{
		Predicate: &spec.Predicate{Column: "amount", Op: spec.OpEq, Value: float64(9007199254740992)},
	}, st)
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.keep(map[string]any{"amount": "9007199254740993"})
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("2^53+1 must not equal 2^53: numeric comparison lost precision")
	}
}
