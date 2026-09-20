package mysql

import (
	"fmt"
	"strings"

	"github.com/maltzsama/urutau/spec"
)

// filterSQL is a compiled structured filter: a WHERE fragment with ? placeholders
// and its positional args, in the order the placeholders appear. Empty when
// there is no filter.
type filterSQL struct {
	where string
	args  []any
}

// empty reports whether the filter contributes no predicate.
func (f filterSQL) empty() bool { return f.where == "" }

// compileFilterSQL renders the structured spec.Filter as a MySQL WHERE
// fragment. The tree is walked once; the leaves are rendered with ?
// placeholders and backtick-quoted identifiers, mirroring the Postgres
// filter_sql.go (which uses squirrel for the same shape). NULL never satisfies
// a comparison — MySQL's own three-valued logic, no explicit guard needed.
func compileFilterSQL(f *spec.Filter) (filterSQL, error) {
	if f == nil {
		return filterSQL{}, nil
	}
	where, args, err := filterNodeSQL(f)
	if err != nil {
		return filterSQL{}, err
	}
	return filterSQL{where: where, args: args}, nil
}

func filterNodeSQL(f *spec.Filter) (string, []any, error) {
	switch {
	case f == nil:
		return "", nil, fmt.Errorf("mysql: filter: empty node")
	case len(f.All) > 0:
		return filterGroupSQL(f.All, " AND ")
	case len(f.Any) > 0:
		return filterGroupSQL(f.Any, " OR ")
	case f.Not != nil:
		inner, args, err := filterNodeSQL(f.Not)
		if err != nil {
			return "", nil, err
		}
		return "NOT (" + inner + ")", args, nil
	case f.Predicate != nil:
		return filterPredicateSQL(f.Predicate)
	default:
		return "", nil, fmt.Errorf("mysql: filter: node carries no all/any/not/where")
	}
}

func filterGroupSQL(nodes []spec.Filter, sep string) (string, []any, error) {
	parts := make([]string, 0, len(nodes))
	var args []any
	for i := range nodes {
		s, a, err := filterNodeSQL(&nodes[i])
		if err != nil {
			return "", nil, err
		}
		parts = append(parts, "("+s+")")
		args = append(args, a...)
	}
	return strings.Join(parts, sep), args, nil
}

func filterPredicateSQL(p *spec.Predicate) (string, []any, error) {
	col := quoteBacktick(p.Column)
	switch p.Op {
	case spec.OpEq:
		return col + " = ?", []any{p.Value}, nil
	case spec.OpNeq:
		return col + " <> ?", []any{p.Value}, nil
	case spec.OpLt:
		return col + " < ?", []any{p.Value}, nil
	case spec.OpLte:
		return col + " <= ?", []any{p.Value}, nil
	case spec.OpGt:
		return col + " > ?", []any{p.Value}, nil
	case spec.OpGte:
		return col + " >= ?", []any{p.Value}, nil
	case spec.OpIn, spec.OpNotIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return "", nil, fmt.Errorf("mysql: filter: op %q requires a list value", p.Op)
		}
		ph := make([]string, len(vals))
		for i := range vals {
			ph[i] = "?"
		}
		op := "IN"
		if p.Op == spec.OpNotIn {
			op = "NOT IN"
		}
		return col + " " + op + " (" + strings.Join(ph, ", ") + ")", vals, nil
	case spec.OpIsNull:
		return col + " IS NULL", nil, nil
	case spec.OpIsNotNull:
		return col + " IS NOT NULL", nil, nil
	default:
		return "", nil, fmt.Errorf("mysql: filter: unsupported operator %q", p.Op)
	}
}

// quoteBacktick quotes one MySQL identifier, doubling embedded backticks.
func quoteBacktick(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
