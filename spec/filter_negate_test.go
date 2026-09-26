package spec

import (
	"reflect"
	"testing"
)

func TestPredicateNegate(t *testing.T) {
	for op, want := range map[Operator]Operator{
		OpEq: OpNeq, OpNeq: OpEq, OpLt: OpGte, OpLte: OpGt, OpGt: OpLte, OpGte: OpLt,
		OpIn: OpNotIn, OpNotIn: OpIn, OpIsNull: OpIsNotNull, OpIsNotNull: OpIsNull,
	} {
		p := &Predicate{Column: "c", Op: op, Value: 1}
		got := p.Negate()
		if got.Op != want || got.Column != "c" || got.Value != 1 {
			t.Errorf("negate %s = %+v, want op %s", op, got, want)
		}
		if got.Negate().Op != op {
			t.Errorf("negating %s twice gave %s", op, got.Negate().Op)
		}
	}
}

// NOT is pushed to the leaves (De Morgan) and never survives as a node.
func TestFilterNegate(t *testing.T) {
	a := Filter{Predicate: &Predicate{Column: "a", Op: OpEq, Value: 1}}
	b := Filter{Predicate: &Predicate{Column: "b", Op: OpLt, Value: 2}}
	notA := Filter{Predicate: &Predicate{Column: "a", Op: OpNeq, Value: 1}}
	notB := Filter{Predicate: &Predicate{Column: "b", Op: OpGte, Value: 2}}
	for name, c := range map[string]struct{ in, want *Filter }{
		"nil":       {nil, nil},
		"predicate": {&a, &notA},
		"all→any":   {&Filter{All: []Filter{a, b}}, &Filter{Any: []Filter{notA, notB}}},
		"any→all":   {&Filter{Any: []Filter{a, b}}, &Filter{All: []Filter{notA, notB}}},
		"not(not)":  {&Filter{Not: &a}, &a},
		// The operand comes back as is: a nested not is the renderer's to
		// negate when it reaches it.
		"not(not(not))": {&Filter{Not: &Filter{Not: &a}}, &Filter{Not: &a}},
		"nested":        {&Filter{All: []Filter{a, {Any: []Filter{b}}}}, &Filter{Any: []Filter{notA, {All: []Filter{notB}}}}},
	} {
		if got := c.in.Negate(); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: negate = %+v, want %+v", name, got, c.want)
		}
	}
}
