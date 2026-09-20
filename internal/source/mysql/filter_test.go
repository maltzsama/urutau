package mysql

import (
	"testing"

	"github.com/maltzsama/urutau/spec"
)

func TestCompileFilterSQL(t *testing.T) {
	cases := []struct {
		name  string
		f     *spec.Filter
		where string
		args  []any
	}{
		{
			name:  "eq",
			f:     &spec.Filter{Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"}},
			where: "`status` = ?",
			args:  []any{"active"},
		},
		{
			name:  "gte",
			f:     &spec.Filter{Predicate: &spec.Predicate{Column: "amount", Op: spec.OpGte, Value: 10.5}},
			where: "`amount` >= ?",
			args:  []any{10.5},
		},
		{
			name:  "in",
			f:     &spec.Filter{Predicate: &spec.Predicate{Column: "t", Op: spec.OpIn, Value: []any{"a", "b"}}},
			where: "`t` IN (?, ?)",
			args:  []any{"a", "b"},
		},
		{
			name:  "not_in",
			f:     &spec.Filter{Predicate: &spec.Predicate{Column: "t", Op: spec.OpNotIn, Value: []any{"a"}}},
			where: "`t` NOT IN (?)",
			args:  []any{"a"},
		},
		{
			name:  "is_null",
			f:     &spec.Filter{Predicate: &spec.Predicate{Column: "v", Op: spec.OpIsNull}},
			where: "`v` IS NULL",
		},
		{
			name: "all",
			f: &spec.Filter{All: []spec.Filter{
				{Predicate: &spec.Predicate{Column: "a", Op: spec.OpEq, Value: 1}},
				{Predicate: &spec.Predicate{Column: "b", Op: spec.OpGt, Value: 2}},
			}},
			where: "(`a` = ?) AND (`b` > ?)",
			args:  []any{1, 2},
		},
		{
			name:  "not",
			f:     &spec.Filter{Not: &spec.Filter{Predicate: &spec.Predicate{Column: "a", Op: spec.OpEq, Value: 1}}},
			where: "NOT (`a` = ?)",
			args:  []any{1},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := compileFilterSQL(c.f)
			if err != nil {
				t.Fatal(err)
			}
			if got.where != c.where {
				t.Fatalf("where = %q, want %q", got.where, c.where)
			}
			if len(got.args) != len(c.args) {
				t.Fatalf("args = %v, want %v", got.args, c.args)
			}
			for i := range c.args {
				if got.args[i] != c.args[i] {
					t.Fatalf("args[%d] = %v, want %v", i, got.args[i], c.args[i])
				}
			}
		})
	}
}

func TestCompileFilterSQLEmpty(t *testing.T) {
	got, err := compileFilterSQL(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !got.empty() {
		t.Fatalf("nil filter must compile to empty, got %q", got.where)
	}
}
