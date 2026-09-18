package postgres

import (
	"fmt"
	"math"
	"math/big"
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
// The table's introspected state tells which columns are numeric. A numeric
// column is decoded as int64, float64, or (for `numeric`) a decimal string,
// so it is compared through expr's float(). A `numeric` column is compared
// exactly through pg_numeric_cmp (math/big), because Postgres evaluates
// `numeric > float8` at the column's native precision, which float64 cannot
// represent.
func compileFilterExpr(f *spec.Filter, st *TableState) (*vm.Program, error) {
	if f == nil {
		return nil, nil
	}
	if err := checkFilterColumns(st, f); err != nil {
		return nil, err
	}
	src, err := filterExprSource(f, st)
	if err != nil {
		return nil, err
	}
	prog, err := expr.Compile(src,
		expr.Env(map[string]any{"row": map[string]any{}}),
		expr.Function("pg_numeric_cmp", func(params ...any) (any, error) {
			if len(params) != 2 {
				return nil, fmt.Errorf("pg_numeric_cmp wants 2 args, got %d", len(params))
			}
			return numericCompare(params[0], params[1])
		}),
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
	switch p.Op {
	case spec.OpIsNull:
		return raw + " == nil", nil
	case spec.OpIsNotNull:
		return raw + " != nil", nil
	}
	if columnIsPgNumeric(st, p.Column) {
		return filterExprPgNumeric(p, raw)
	}
	lhs := raw
	if columnIsInteger(st, p.Column) {
		lhs = "float(" + raw + ")"
	}
	switch p.Op {
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

// filterExprPgNumeric renders a predicate on a column that is compared
// through the exact pg_numeric_cmp helper: `numeric` (decoded as a decimal
// string) and `real`/`double precision`/`money` (decoded as float64). This
// keeps native precision and accepts the special values those columns can
// hold — NaN and ±Infinity — which a plain expr comparison cannot.
func filterExprPgNumeric(p *spec.Predicate, raw string) (string, error) {
	guard := "(" + raw + " != nil)"
	switch p.Op {
	case spec.OpEq, spec.OpNeq, spec.OpLt, spec.OpLte, spec.OpGt, spec.OpGte:
		lit, err := exprLiteral(p.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s && (pg_numeric_cmp(%s, %s) %s 0)", guard, raw, lit, exprOperator(p.Op)), nil
	case spec.OpIn, spec.OpNotIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return "", fmt.Errorf("postgres: filter: op %q requires a list value", p.Op)
		}
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			lit, err := exprLiteral(v)
			if err != nil {
				return "", err
			}
			parts = append(parts, fmt.Sprintf("(pg_numeric_cmp(%s, %s) == 0)", raw, lit))
		}
		inner := "false"
		if len(parts) > 0 {
			inner = strings.Join(parts, " || ")
		}
		if p.Op == spec.OpNotIn {
			inner = "!(" + inner + ")"
		}
		return fmt.Sprintf("%s && (%s)", guard, inner), nil
	default:
		return "", fmt.Errorf("postgres: filter: unsupported operator %q", p.Op)
	}
}

// checkFilterColumns reports an error when a filter references a column that
// is not in the introspected table. A typo would otherwise emit an unknown
// identifier into the snapshot SQL, and in CDC evaluate a missing map key as
// nil (so is_null matches every row and other predicates reject every row).
func checkFilterColumns(st *TableState, f *spec.Filter) error {
	if f == nil || st == nil {
		return nil
	}
	switch {
	case len(f.All) > 0:
		for i := range f.All {
			if err := checkFilterColumns(st, &f.All[i]); err != nil {
				return err
			}
		}
	case len(f.Any) > 0:
		for i := range f.Any {
			if err := checkFilterColumns(st, &f.Any[i]); err != nil {
				return err
			}
		}
	case f.Not != nil:
		return checkFilterColumns(st, f.Not)
	case f.Predicate != nil:
		if st.FindColumn(f.Predicate.Column) < 0 {
			return fmt.Errorf("filter column %q not found in the source table", f.Predicate.Column)
		}
	}
	return nil
}

// columnIsPgNumeric reports whether the column is compared through the exact
// pg_numeric_cmp helper: `numeric` and the floating types, which can hold
// NaN/Infinity.
func columnIsPgNumeric(st *TableState, name string) bool {
	if st == nil {
		return false
	}
	i := st.FindColumn(name)
	if i < 0 {
		return false
	}
	switch strings.ToLower(st.Columns[i].DataType) {
	case "numeric", "real", "double precision", "money":
		return true
	}
	return false
}

// numericCompare compares two numeric scalars with PostgreSQL `numeric`
// semantics and returns -1, 0, or 1. Unlike big.Rat alone, it accepts the
// special values a `numeric` column can hold — NaN and ±Infinity — ordering
// them as Postgres does: -Infinity < finite < Infinity < NaN, and NaN equal
// to itself.
func numericCompare(a, b any) (int, error) {
	av, err := parsePgNumeric(a)
	if err != nil {
		return 0, err
	}
	bv, err := parsePgNumeric(b)
	if err != nil {
		return 0, err
	}
	return cmpPgNumeric(av, bv), nil
}

const (
	pgNumFinite = iota
	pgNumNegInf
	pgNumPosInf
	pgNumNaN
)

type pgNumeric struct {
	kind int
	rat  *big.Rat // set only when kind == pgNumFinite
}

func parsePgNumeric(v any) (pgNumeric, error) {
	switch t := v.(type) {
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "nan":
			return pgNumeric{kind: pgNumNaN}, nil
		case "infinity", "+infinity", "inf", "+inf":
			return pgNumeric{kind: pgNumPosInf}, nil
		case "-infinity", "-inf":
			return pgNumeric{kind: pgNumNegInf}, nil
		}
		r, ok := new(big.Rat).SetString(t)
		if !ok {
			return pgNumeric{}, fmt.Errorf("pg_numeric_cmp: %q is not a number", t)
		}
		return pgNumeric{kind: pgNumFinite, rat: r}, nil
	case int64:
		return pgNumeric{kind: pgNumFinite, rat: new(big.Rat).SetInt64(t)}, nil
	case int:
		return pgNumeric{kind: pgNumFinite, rat: new(big.Rat).SetInt64(int64(t))}, nil
	case float64:
		switch {
		case math.IsNaN(t):
			return pgNumeric{kind: pgNumNaN}, nil
		case math.IsInf(t, 1):
			return pgNumeric{kind: pgNumPosInf}, nil
		case math.IsInf(t, -1):
			return pgNumeric{kind: pgNumNegInf}, nil
		}
		return pgNumeric{kind: pgNumFinite, rat: new(big.Rat).SetFloat64(t)}, nil
	default:
		return pgNumeric{}, fmt.Errorf("pg_numeric_cmp: unsupported type %T", v)
	}
}

func cmpPgNumeric(a, b pgNumeric) int {
	// NaN is the largest value and equal to itself (Postgres numeric).
	if a.kind == pgNumNaN || b.kind == pgNumNaN {
		switch {
		case a.kind == pgNumNaN && b.kind == pgNumNaN:
			return 0
		case a.kind == pgNumNaN:
			return 1
		default:
			return -1
		}
	}
	// -Infinity < finite < +Infinity.
	if a.kind != b.kind {
		switch {
		case a.kind == pgNumFinite:
			if b.kind == pgNumPosInf {
				return -1
			}
			return 1
		case b.kind == pgNumFinite:
			if a.kind == pgNumPosInf {
				return 1
			}
			return -1
		case a.kind == pgNumPosInf:
			return 1
		default:
			return -1
		}
	}
	if a.kind == pgNumFinite {
		return a.rat.Cmp(b.rat)
	}
	return 0
}

// columnIsInteger reports whether the column is an integer type. Those are
// compared through float() to match the snapshot, where Postgres casts the
// integer column to float8 against the float8 literal parameter.
func columnIsInteger(st *TableState, name string) bool {
	if st == nil {
		return false
	}
	i := st.FindColumn(name)
	if i < 0 {
		return false
	}
	switch strings.ToLower(st.Columns[i].DataType) {
	case "smallint", "integer", "bigint":
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
