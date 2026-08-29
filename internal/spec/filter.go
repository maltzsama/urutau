package spec

// Filter is the closed-grammar predicate tree: all (AND) / any (OR) / not
// over row predicates. JSON tags are the wire grammar.
type Filter struct {
	All       []Filter   `json:"all,omitempty"`
	Any       []Filter   `json:"any,omitempty"`
	Not       *Filter    `json:"not,omitempty"`
	Predicate *Predicate `json:"where,omitempty"`
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
