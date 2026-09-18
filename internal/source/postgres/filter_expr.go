package postgres

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/maltzsama/urutau/spec"
)

// compileFilterExpr compiles the structured filter into an expr program,
// evaluated against each decoded row at the source boundary — before the
// Arrow hot-path. expr owns comparison, nil and membership semantics; there
// is no hand-written evaluator. The structured spec.Filter remains the single
// filter type; this is only its Go rendering.
func compileFilterExpr(f *spec.Filter) (*vm.Program, error) {
	if f == nil {
		return nil, nil
	}
	src, err := filterExprSource(f)
	if err != nil {
		return nil, err
	}
	prog, err := expr.Compile(src,
		expr.Env(map[string]any{"row": map[string]any{}}),
		expr.AsBool(),
	)
	if err != nil {
		return nil, fmt.Errorf("postgres: filter: compile: %w", err)
	}
	return prog, nil
}

func filterExprSource(f *spec.Filter) (string, error) {
	switch {
	case f == nil:
		return "", fmt.Errorf("postgres: filter: empty node")
	case len(f.All) > 0:
		return filterExprGroup(f.All, "&&")
	case len(f.Any) > 0:
		return filterExprGroup(f.Any, "||")
	case f.Not != nil:
		inner, err := filterExprSource(f.Not)
		if err != nil {
			return "", err
		}
		return "!(" + inner + ")", nil
	case f.Predicate != nil:
		return filterExprPredicate(f.Predicate)
	default:
		return "", fmt.Errorf("postgres: filter: node carries no all/any/not/where")
	}
}

func filterExprGroup(nodes []spec.Filter, op string) (string, error) {
	parts := make([]string, 0, len(nodes))
	for i := range nodes {
		p, err := filterExprSource(&nodes[i])
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+p+")")
	}
	return strings.Join(parts, " "+op+" "), nil
}

func filterExprPredicate(p *spec.Predicate) (string, error) {
	col := "row[" + strconv.Quote(p.Column) + "]"
	switch p.Op {
	case spec.OpIsNull:
		return col + " == nil", nil
	case spec.OpIsNotNull:
		return col + " != nil", nil
	case spec.OpEq, spec.OpNeq, spec.OpLt, spec.OpLte, spec.OpGt, spec.OpGte:
		lit, err := exprLiteral(p.Value)
		if err != nil {
			return "", err
		}
		return col + " " + exprOperator(p.Op) + " " + lit, nil
	case spec.OpIn, spec.OpNotIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return "", fmt.Errorf("postgres: filter: op %q requires a list value", p.Op)
		}
		lits := make([]string, 0, len(vals))
		for _, v := range vals {
			l, err := exprLiteral(v)
			if err != nil {
				return "", err
			}
			lits = append(lits, l)
		}
		inner := col + " in [" + strings.Join(lits, ", ") + "]"
		if p.Op == spec.OpNotIn {
			return "!(" + inner + ")", nil
		}
		return inner, nil
	default:
		return "", fmt.Errorf("postgres: filter: unsupported operator %q", p.Op)
	}
}

func exprOperator(op spec.Operator) string {
	switch op {
	case spec.OpEq:
		return "=="
	case spec.OpNeq:
		return "!="
	case spec.OpLt:
		return "<"
	case spec.OpLte:
		return "<="
	case spec.OpGt:
		return ">"
	case spec.OpGte:
		return ">="
	default:
		return ""
	}
}

// exprLiteral renders a filter value as an expr literal.
func exprLiteral(v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "nil", nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	case float64: // JSON numbers decode to float64
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case int:
		return strconv.Itoa(t), nil
	case string:
		return strconv.Quote(t), nil
	default:
		return "", fmt.Errorf("postgres: filter: unsupported value type %T", v)
	}
}
