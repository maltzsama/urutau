package spec

import "encoding/json"

// Filter is the closed-grammar predicate tree: all (AND) / any (OR) / not
// over row predicates. In the authoring format a predicate sits inline on
// the node ({col, op, value}); UnmarshalJSON lifts it into Predicate.
type Filter struct {
	All       []Filter   `json:"all,omitempty"`
	Any       []Filter   `json:"any,omitempty"`
	Not       *Filter    `json:"not,omitempty"`
	Predicate *Predicate `json:"where,omitempty"`
}

// UnmarshalJSON accepts both the inline authoring shape (a node that is
// itself a predicate) and the explicit `where` field.
func (f *Filter) UnmarshalJSON(b []byte) error {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(b, &probe); err != nil {
		return err
	}
	if _, ok := probe["col"]; ok {
		var p Predicate
		if err := json.Unmarshal(b, &p); err != nil {
			return err
		}
		f.Predicate = &p
		return nil
	}
	type filterAlias Filter
	var a filterAlias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*f = Filter(a)
	return nil
}

type Operator string

const (
	OpEq        Operator = "eq"
	OpNeq       Operator = "neq"
	OpLt        Operator = "lt"
	OpLte       Operator = "lte"
	OpGt        Operator = "gt"
	OpGte       Operator = "gte"
	OpIn        Operator = "in"
	OpNotIn     Operator = "not_in"
	OpIsNull    Operator = "is_null"
	OpIsNotNull Operator = "is_not_null"
)

type Predicate struct {
	Column string   `json:"col"`
	Op     Operator `json:"op"`
	Value  any      `json:"value,omitempty"`
}

// Negate returns the logical negation of f with NOT pushed down to the leaves
// (De Morgan): all↔any, not(not(x)) = x, and each predicate's operator
// negated. It never leaves a `not` node, so an expression built from it has
// no negation over a comparison. Shared by every SQL source (issue #403).
func (f *Filter) Negate() *Filter {
	switch {
	case f == nil:
		return nil
	case len(f.All) > 0:
		out := make([]Filter, len(f.All))
		for i := range f.All {
			out[i] = *f.All[i].Negate()
		}
		return &Filter{Any: out}
	case len(f.Any) > 0:
		out := make([]Filter, len(f.Any))
		for i := range f.Any {
			out[i] = *f.Any[i].Negate()
		}
		return &Filter{All: out}
	case f.Not != nil:
		return f.Not
	case f.Predicate != nil:
		return &Filter{Predicate: f.Predicate.Negate()}
	default:
		return nil
	}
}

// Negate returns p with its operator negated (= ↔ !=, < ↔ >=, in ↔ not in,
// is null ↔ is not null, …).
func (p *Predicate) Negate() *Predicate {
	op := p.Op
	switch p.Op {
	case OpEq:
		op = OpNeq
	case OpNeq:
		op = OpEq
	case OpLt:
		op = OpGte
	case OpLte:
		op = OpGt
	case OpGt:
		op = OpLte
	case OpGte:
		op = OpLt
	case OpIn:
		op = OpNotIn
	case OpNotIn:
		op = OpIn
	case OpIsNull:
		op = OpIsNotNull
	case OpIsNotNull:
		op = OpIsNull
	}
	return &Predicate{Column: p.Column, Op: op, Value: p.Value}
}
