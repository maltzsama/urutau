package spec

import "testing"

func TestFilterMatchesOperators(t *testing.T) {
	row := map[string]any{"id": int64(7), "v": "hot", "amount": 2.5}

	cases := []struct {
		name  string
		p     Predicate
		match bool
	}{
		{"eq int", Predicate{Column: "id", Op: OpEq, Value: 7.0}, true},
		{"eq cross-type string", Predicate{Column: "v", Op: OpEq, Value: "hot"}, true},
		{"eq miss", Predicate{Column: "id", Op: OpEq, Value: 8.0}, false},
		{"neq", Predicate{Column: "v", Op: OpNeq, Value: "cold"}, true},
		{"neq null col is false (SQL semantics)", Predicate{Column: "ghost", Op: OpNeq, Value: "x"}, false},
		{"lt numeric", Predicate{Column: "amount", Op: OpLt, Value: 3.0}, true},
		{"lt int64 vs float", Predicate{Column: "id", Op: OpLt, Value: 10.0}, true},
		{"gte", Predicate{Column: "amount", Op: OpGte, Value: 2.5}, true},
		{"gt false", Predicate{Column: "amount", Op: OpGt, Value: 2.5}, false},
		{"lt string", Predicate{Column: "v", Op: OpLt, Value: "ice"}, true}, // hot < ice
		{"lt mixed types not comparable", Predicate{Column: "v", Op: OpLt, Value: 1.0}, false},
		{"in", Predicate{Column: "id", Op: OpIn, Value: []any{1.0, 7.0, 9.0}}, true},
		{"in miss", Predicate{Column: "id", Op: OpIn, Value: []any{1.0, 2.0}}, false},
		{"not_in", Predicate{Column: "id", Op: OpNotIn, Value: []any{1.0}}, true},
		{"in on null col", Predicate{Column: "ghost", Op: OpIn, Value: []any{1.0}}, false},
		{"is_null", Predicate{Column: "ghost", Op: OpIsNull}, true},
		{"is_null present", Predicate{Column: "id", Op: OpIsNull}, false},
		{"is_not_null", Predicate{Column: "id", Op: OpIsNotNull}, true},
	}

	for _, tc := range cases {
		if got := tc.p.Matches(row); got != tc.match {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.match)
		}
	}
}

func TestFilterMatchesCombinators(t *testing.T) {
	row := map[string]any{"id": int64(7), "v": "hot"}

	all := Filter{All: []Filter{
		{Predicate: &Predicate{Column: "id", Op: OpGt, Value: 1.0}},
		{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "hot"}},
	}}
	if !all.Matches(row) {
		t.Error("all: both sides match, want true")
	}

	allFail := Filter{All: []Filter{
		{Predicate: &Predicate{Column: "id", Op: OpGt, Value: 100.0}},
		{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "hot"}},
	}}
	if allFail.Matches(row) {
		t.Error("all: one side fails, want false")
	}

	any := Filter{Any: []Filter{
		{Predicate: &Predicate{Column: "id", Op: OpGt, Value: 100.0}},
		{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "hot"}},
	}}
	if !any.Matches(row) {
		t.Error("any: one side matches, want true")
	}

	not := Filter{Not: &Filter{Predicate: &Predicate{Column: "id", Op: OpGt, Value: 100.0}}}
	if !not.Matches(row) {
		t.Error("not: negated false, want true")
	}

	nested := Filter{All: []Filter{
		{Not: &Filter{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "cold"}}},
		{Any: []Filter{
			{Predicate: &Predicate{Column: "id", Op: OpEq, Value: 7.0}},
			{Predicate: &Predicate{Column: "id", Op: OpEq, Value: 8.0}},
		}},
	}}
	if !nested.Matches(row) {
		t.Error("nested tree: want true")
	}

	if !(*Filter)(nil).Matches(row) {
		t.Error("nil filter matches everything")
	}
}

func TestFilterSQL(t *testing.T) {
	cases := []struct {
		name string
		f    *Filter
		want string
		args []any
	}{
		{
			"eq",
			&Filter{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "hot"}},
			"(`v` = ?)", []any{"hot"},
		},
		{
			"neq",
			&Filter{Predicate: &Predicate{Column: "amount", Op: OpNeq, Value: 1.5}},
			"(`amount` <> ?)", []any{1.5},
		},
		{
			"is null",
			&Filter{Predicate: &Predicate{Column: "v", Op: OpIsNull}},
			"`v` IS NULL", nil,
		},
		{
			"in",
			&Filter{Predicate: &Predicate{Column: "id", Op: OpIn, Value: []any{1.0, 2.0, 3.0}}},
			"(`id` IN (?, ?, ?))", []any{1.0, 2.0, 3.0},
		},
		{
			"not in",
			&Filter{Predicate: &Predicate{Column: "id", Op: OpNotIn, Value: []any{1.0}}},
			"(`id` NOT IN (?))", []any{1.0},
		},
		{
			"empty in is false",
			&Filter{Predicate: &Predicate{Column: "id", Op: OpIn, Value: []any{}}},
			"(FALSE)", nil,
		},
		{
			"empty not_in is true",
			&Filter{Predicate: &Predicate{Column: "id", Op: OpNotIn, Value: []any{}}},
			"(TRUE)", nil,
		},
		{
			"all AND",
			&Filter{All: []Filter{
				{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "hot"}},
				{Predicate: &Predicate{Column: "id", Op: OpGt, Value: 1.0}},
			}},
			"((`v` = ?) AND (`id` > ?))", []any{"hot", 1.0},
		},
		{
			"any OR",
			&Filter{Any: []Filter{
				{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "hot"}},
				{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "cold"}},
			}},
			"((`v` = ?) OR (`v` = ?))", []any{"hot", "cold"},
		},
		{
			"not",
			&Filter{Not: &Filter{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "hot"}}},
			"(NOT (`v` = ?))", []any{"hot"},
		},
		{
			"nested tree",
			&Filter{All: []Filter{
				{Not: &Filter{Predicate: &Predicate{Column: "v", Op: OpEq, Value: "gone"}}},
				{Any: []Filter{
					{Predicate: &Predicate{Column: "amount", Op: OpLt, Value: 10.0}},
					{Predicate: &Predicate{Column: "amount", Op: OpGte, Value: 100.0}},
				}},
			}},
			"((NOT (`v` = ?)) AND ((`amount` < ?) OR (`amount` >= ?)))",
			[]any{"gone", 10.0, 100.0},
		},
	}

	for _, tc := range cases {
		got, args, err := tc.f.SQL()
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: sql %q, want %q", tc.name, got, tc.want)
		}
		if len(args) != len(tc.args) {
			t.Errorf("%s: args %v, want %v", tc.name, args, tc.args)
			continue
		}
		for i := range args {
			if args[i] != tc.args[i] {
				t.Errorf("%s: arg %d = %v, want %v", tc.name, i, args[i], tc.args[i])
			}
		}
	}

	if s, args, err := (*Filter)(nil).SQL(); s != "" || args != nil || err != nil {
		t.Errorf("nil filter: %q %v %v", s, args, err)
	}
}

func TestFilterSQLRejectsQuoteInColumn(t *testing.T) {
	f := &Filter{Predicate: &Predicate{Column: "v`x", Op: OpEq, Value: "hot"}}
	if _, _, err := f.SQL(); err == nil {
		t.Fatal("embedded backtick must be rejected")
	}
	if _, _, err := (&Filter{Predicate: &Predicate{Column: "", Op: OpEq, Value: "x"}}).SQL(); err == nil {
		t.Fatal("empty column must be rejected")
	}
}
