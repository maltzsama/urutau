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
// Arrow hot-path. expr owns comparison and membership; there is no
// hand-written evaluator. The structured spec.Filter remains the single
// filter type; this is only its Go rendering.
//
// The rendering is faithful to PostgreSQL three-valued logic, which expr's
// two-valued booleans do not have:
//
//   - NOT is pushed to the leaves (De Morgan), so no `!` ever negates an
//     unknown-valued leaf.
//   - Every comparison leaf is guarded by `col != nil`, so a NULL column
//     never satisfies it — matching SQL, where `col <> x` with a NULL col is
//     UNKNOWN and excludes the row.
//
// The table's introspected state tells which columns are numeric, so a
// numeric column (decoded as int64, float64, or a decimal string) is
// compared through expr's float() — otherwise a `numeric` column, decoded as
// a string, would fail a numeric comparison at runtime.
func compileFilterExpr(f *spec.Filter, st *TableState) (*vm.Program, error) {
	if f == nil {
		return nil, nil
	}
	src, err := filterExprSource(f, st)
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

func filterExprSource(f *spec.Filter, st *TableState) (string, error) {
	switch {
	case f == nil:
		return "", fmt.Errorf("postgres: filter: empty node")
	case len(f.All) > 0:
		return filterExprGroup(f.All, st, "&&")
	case len(f.Any) > 0:
		return filterExprGroup(f.Any, st, "||")
	case f.Not != nil:
		// Push the negation to the leaves so no `!` wraps an unknown-valued
		// comparison.
		return filterExprSource(negateFilter(f.Not), st)
	case f.Predicate != nil:
		return filterExprPredicate(f.Predicate, st)
	default:
		return "", fmt.Errorf("postgres: filter: node carries no all/any/not/where")
	}
}

func filterExprGroup(nodes []spec.Filter, st *TableState, op string) (string, error) {
	parts := make([]string, 0, len(nodes))
	for i := range nodes {
		p, err := filterExprSource(&nodes[i], st)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+p+")")
	}
	return strings.Join(parts, " "+op+" "), nil
}

func filterExprPredicate(p *spec.Predicate, st *TableState) (string, error) {
	raw := "row[" + strconv.Quote(p.Column) + "]"
	lhs := raw
	if columnIsNumeric(st, p.Column) {
		lhs = "float(" + raw + ")"
	}
	switch p.Op {
	case spec.OpIsNull:
		return raw + " == nil", nil
	case spec.OpIsNotNull:
		return raw + " != nil", nil
	case spec.OpEq, spec.OpNeq, spec.OpLt, spec.OpLte, spec.OpGt, spec.OpGte:
		lit, err := exprLiteral(p.Value)
		if err != nil {
			return "", err
		}
		// The NULL guard is SQL three-valued logic: a NULL column never
		// satisfies a comparison, so the leaf is false, not true.
		return fmt.Sprintf("(%s != nil) && (%s %s %s)", raw, lhs, exprOperator(p.Op), lit), nil
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
		inner := lhs + " in [" + strings.Join(lits, ", ") + "]"
		if p.Op == spec.OpNotIn {
			inner = "!(" + inner + ")"
		}
		return fmt.Sprintf("(%s != nil) && (%s)", raw, inner), nil
	default:
		return "", fmt.Errorf("postgres: filter: unsupported operator %q", p.Op)
	}
}

// columnIsNumeric reports whether the column maps to a numeric Postgres type
// in the introspected schema.
func columnIsNumeric(st *TableState, name string) bool {
	if st == nil {
		return false
	}
	i := st.FindColumn(name)
	if i < 0 {
		return false
	}
	switch strings.ToLower(st.Columns[i].DataType) {
	case "smallint", "integer", "bigint", "real", "double precision", "numeric", "money":
		return true
	}
	return false
}

// negateFilter returns the logical negation of f with NOT pushed down to the
// leaves (De Morgan): all↔any and each predicate operator negated. It never
// leaves a `not` node, so the emitted expression has no `!` over a comparison.
func negateFilter(f *spec.Filter) *spec.Filter {
	switch {
	case f == nil:
		return nil
	case len(f.All) > 0:
		out := make([]spec.Filter, len(f.All))
		for i := range f.All {
			out[i] = *negateFilter(&f.All[i])
		}
		return &spec.Filter{Any: out}
	case len(f.Any) > 0:
		out := make([]spec.Filter, len(f.Any))
		for i := range f.Any {
			out[i] = *negateFilter(&f.Any[i])
		}
		return &spec.Filter{All: out}
	case f.Not != nil:
		// NOT(NOT(x)) = x.
		return f.Not
	case f.Predicate != nil:
		return &spec.Filter{Predicate: negatePredicate(f.Predicate)}
	default:
		return nil
	}
}

func negatePredicate(p *spec.Predicate) *spec.Predicate {
	op := p.Op
	switch p.Op {
	case spec.OpEq:
		op = spec.OpNeq
	case spec.OpNeq:
		op = spec.OpEq
	case spec.OpLt:
		op = spec.OpGte
	case spec.OpLte:
		op = spec.OpGt
	case spec.OpGt:
		op = spec.OpLte
	case spec.OpGte:
		op = spec.OpLt
	case spec.OpIn:
		op = spec.OpNotIn
	case spec.OpNotIn:
		op = spec.OpIn
	case spec.OpIsNull:
		op = spec.OpIsNotNull
	case spec.OpIsNotNull:
		op = spec.OpIsNull
	}
	return &spec.Predicate{Column: p.Column, Op: op, Value: p.Value}
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

// exprLiteral renders a filter value as an expr literal. JSON numbers are
// rendered as floats so a numeric column (compared through float()) sees a
// float operand.
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
		s := strconv.FormatFloat(t, 'g', -1, 64)
		if !strings.ContainsAny(s, ".eE") {
			s += ".0"
		}
		return s, nil
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
