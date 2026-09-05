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
