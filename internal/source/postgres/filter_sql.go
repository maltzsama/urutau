package postgres

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/maltzsama/urutau/spec"
)

// filterToSquirrel compiles the structured filter into a squirrel Sqlizer for
// the snapshot chunk SELECT. Squirrel owns the SQL shape — identifier quoting
// aside, there is no hand-built SQL string, and it handles placeholder
// numbering and value binding. The structured spec.Filter is the single
// filter type; this is only its SQL rendering.
func filterToSquirrel(f *spec.Filter) (sq.Sqlizer, error) {
	if f == nil {
		return nil, nil
	}
	return filterNodeToSquirrel(f)
}

func filterNodeToSquirrel(f *spec.Filter) (sq.Sqlizer, error) {
	switch {
	case f == nil:
		return nil, fmt.Errorf("postgres: filter: empty node")
	case len(f.All) > 0:
		parts, err := filterGroupToSquirrel(f.All)
		if err != nil {
			return nil, err
		}
		return sq.And(parts), nil
	case len(f.Any) > 0:
		parts, err := filterGroupToSquirrel(f.Any)
		if err != nil {
			return nil, err
		}
		return sq.Or(parts), nil
	case f.Not != nil:
		inner, err := filterNodeToSquirrel(f.Not)
		if err != nil {
			return nil, err
		}
		return sq.Expr("NOT (?)", inner), nil
	case f.Predicate != nil:
		return filterPredicateToSquirrel(f.Predicate)
	default:
		return nil, fmt.Errorf("postgres: filter: node carries no all/any/not/where")
	}
}

func filterGroupToSquirrel(nodes []spec.Filter) ([]sq.Sqlizer, error) {
	parts := make([]sq.Sqlizer, 0, len(nodes))
	for i := range nodes {
		p, err := filterNodeToSquirrel(&nodes[i])
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, nil
}

func filterPredicateToSquirrel(p *spec.Predicate) (sq.Sqlizer, error) {
	col := quotePgIdent(p.Column)
	switch p.Op {
	case spec.OpEq:
		return sq.Eq{col: p.Value}, nil
	case spec.OpNeq:
		return sq.NotEq{col: p.Value}, nil
	case spec.OpLt:
		return sq.Lt{col: p.Value}, nil
	case spec.OpLte:
		return sq.LtOrEq{col: p.Value}, nil
	case spec.OpGt:
		return sq.Gt{col: p.Value}, nil
	case spec.OpGte:
		return sq.GtOrEq{col: p.Value}, nil
	case spec.OpIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return nil, fmt.Errorf("postgres: filter: op %q requires a list value", p.Op)
		}
		return sq.Eq{col: vals}, nil
	case spec.OpNotIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return nil, fmt.Errorf("postgres: filter: op %q requires a list value", p.Op)
		}
		return sq.NotEq{col: vals}, nil
	case spec.OpIsNull:
		return sq.Eq{col: nil}, nil
	case spec.OpIsNotNull:
		return sq.NotEq{col: nil}, nil
	default:
		return nil, fmt.Errorf("postgres: filter: unsupported operator %q", p.Op)
	}
}
