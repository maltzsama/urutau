package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/shopspring/decimal"

	"github.com/maltzsama/urutau/spec"
)

// projection is a table's CDC read projection: the columns to emit and the
// compiled filter a row must satisfy. Both are applied on the decoded map,
// before the Arrow hot-path — mirroring the Postgres projection.
type projection struct {
	Columns []string
	program *vm.Program
}

// keep reports whether a full decoded row satisfies the filter.
func (p projection) keep(full map[string]any) (bool, error) {
	if p.program == nil {
		return true, nil
	}
	out, err := expr.Run(p.program, map[string]any{"row": full})
	if err != nil {
		return false, fmt.Errorf("mysql: filter: %w", err)
	}
	ok, isBool := out.(bool)
	if !isBool {
		return false, fmt.Errorf("mysql: filter: non-bool result %T", out)
	}
	return ok, nil
}

// project narrows a full decoded row to the selected columns. An empty
// projection returns the row unchanged.
func (p projection) project(full map[string]any) map[string]any {
	if len(p.Columns) == 0 {
		return full
	}
	out := make(map[string]any, len(p.Columns))
	for _, c := range p.Columns {
		out[c] = full[c]
	}
	return out
}

// newProjection builds a projection, compiling the structured filter to an
// expr program once per table using the introspected column types.
func newProjection(columns []string, f *spec.Filter, tbl *schema.Table) (projection, error) {
	p := projection{Columns: columns}
	prog, err := compileFilterExpr(f, tbl)
	if err != nil {
		return projection{}, err
	}
	p.program = prog
	return p, nil
}

// compileFilterExpr compiles the structured filter into an expr program,
// evaluated against each decoded row. expr owns comparison and membership.
//
// The rendering is faithful to SQL three-valued logic, which expr's
// two-valued booleans do not have:
//
//   - NOT is pushed to the leaves (De Morgan), so no `!` ever negates an
//     unknown-valued leaf.
//   - Every comparison leaf is guarded by `col != nil`, so a NULL column never
//     satisfies it.
//
// A DECIMAL column is compared exactly through mysql_decimal_cmp
// (shopspring/decimal): MySQL DECIMAL is exact (KindDecimal) and arrives as a
// string, so float() would round it and make the CDC filter disagree with the
// snapshot (native DECIMAL arithmetic) and the sink.
func compileFilterExpr(f *spec.Filter, tbl *schema.Table) (*vm.Program, error) {
	if f == nil {
		return nil, nil
	}
	if err := checkFilterColumns(tbl, f); err != nil {
		return nil, err
	}
	src, err := filterExprSource(f, tbl)
	if err != nil {
		return nil, err
	}
	prog, err := expr.Compile(src,
		expr.Env(map[string]any{"row": map[string]any{}}),
		expr.Function("mysql_decimal_cmp", func(params ...any) (any, error) {
			if len(params) != 2 {
				return nil, fmt.Errorf("mysql_decimal_cmp wants 2 args, got %d", len(params))
			}
			return decimalCompare(params[0], params[1])
		}),
		expr.AsBool(),
	)
	if err != nil {
		return nil, fmt.Errorf("mysql: filter: compile: %w", err)
	}
	return prog, nil
}

func filterExprSource(f *spec.Filter, tbl *schema.Table) (string, error) {
	switch {
	case f == nil:
		return "", fmt.Errorf("mysql: filter: empty node")
	case len(f.All) > 0:
		return filterExprGroup(f.All, tbl, "&&")
	case len(f.Any) > 0:
		return filterExprGroup(f.Any, tbl, "||")
	case f.Not != nil:
		return filterExprSource(negateFilter(f.Not), tbl)
	case f.Predicate != nil:
		return filterExprPredicate(f.Predicate, tbl)
	default:
		return "", fmt.Errorf("mysql: filter: node carries no all/any/not/where")
	}
}

func filterExprGroup(nodes []spec.Filter, tbl *schema.Table, op string) (string, error) {
	parts := make([]string, 0, len(nodes))
	for i := range nodes {
		p, err := filterExprSource(&nodes[i], tbl)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+p+")")
	}
	return strings.Join(parts, " "+op+" "), nil
}

func filterExprPredicate(p *spec.Predicate, tbl *schema.Table) (string, error) {
	raw := "row[" + strconv.Quote(p.Column) + "]"
	switch p.Op {
	case spec.OpIsNull:
		return raw + " == nil", nil
	case spec.OpIsNotNull:
		return raw + " != nil", nil
	}
	if columnIsDecimal(tbl, p.Column) {
		return filterExprDecimal(p, raw)
	}
	lhs := raw
	if columnIsNumeric(tbl, p.Column) {
		lhs = "float(" + raw + ")"
	} else if columnIsCaseInsensitive(tbl, p.Column) {
		// The snapshot compares in the column's case-insensitive collation;
		// lower() reproduces the common case so the two paths agree.
		lhs = "lower(" + raw + ")"
	}
	// literal renders a filter value, matching the LHS collation when needed.
	literal := func(v any) (string, error) {
		l, err := exprLiteral(v)
		if err != nil {
			return "", err
		}
		if columnIsCaseInsensitive(tbl, p.Column) {
			return "lower(" + l + ")", nil
		}
		return l, nil
	}
	switch p.Op {
	case spec.OpEq, spec.OpNeq, spec.OpLt, spec.OpLte, spec.OpGt, spec.OpGte:
		lit, err := literal(p.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s != nil) && (%s %s %s)", raw, lhs, exprOperator(p.Op), lit), nil
	case spec.OpIn, spec.OpNotIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return "", fmt.Errorf("mysql: filter: op %q requires a list value", p.Op)
		}
		lits := make([]string, 0, len(vals))
		for _, v := range vals {
			l, err := literal(v)
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
		return "", fmt.Errorf("mysql: filter: unsupported operator %q", p.Op)
	}
}

// filterExprDecimal renders a predicate on a DECIMAL column through the exact
// mysql_decimal_cmp helper, so the comparison keeps the column's native
// precision instead of rounding through float64.
func filterExprDecimal(p *spec.Predicate, raw string) (string, error) {
	guard := "(" + raw + " != nil)"
	switch p.Op {
	case spec.OpEq, spec.OpNeq, spec.OpLt, spec.OpLte, spec.OpGt, spec.OpGte:
		lit, err := exprLiteral(p.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s && (mysql_decimal_cmp(%s, %s) %s 0)", guard, raw, lit, exprOperator(p.Op)), nil
	case spec.OpIn, spec.OpNotIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return "", fmt.Errorf("mysql: filter: op %q requires a list value", p.Op)
		}
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			lit, err := exprLiteral(v)
			if err != nil {
				return "", err
			}
			parts = append(parts, fmt.Sprintf("(mysql_decimal_cmp(%s, %s) == 0)", raw, lit))
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
		return "", fmt.Errorf("mysql: filter: unsupported operator %q", p.Op)
	}
}

// checkFilterColumns reports an error when a filter references a column that
// is not in the introspected table — a typo would otherwise evaluate a missing
// map key as nil.
func checkFilterColumns(tbl *schema.Table, f *spec.Filter) error {
	if f == nil || tbl == nil {
		return nil
	}
	switch {
	case len(f.All) > 0:
		for i := range f.All {
			if err := checkFilterColumns(tbl, &f.All[i]); err != nil {
				return err
			}
		}
	case len(f.Any) > 0:
		for i := range f.Any {
			if err := checkFilterColumns(tbl, &f.Any[i]); err != nil {
				return err
			}
		}
	case f.Not != nil:
		return checkFilterColumns(tbl, f.Not)
	case f.Predicate != nil:
		if tbl.FindColumn(f.Predicate.Column) < 0 {
			return fmt.Errorf("filter column %q not found in the source table", f.Predicate.Column)
		}
	}
	return nil
}

// columnIsNumeric reports whether the column is an integer or float type,
// compared through float() (matching the snapshot).
func columnIsNumeric(tbl *schema.Table, name string) bool {
	switch columnType(tbl, name) {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT, schema.TYPE_FLOAT:
		return true
	}
	return false
}

// columnIsDecimal reports whether the column is DECIMAL, compared exactly.
func columnIsDecimal(tbl *schema.Table, name string) bool {
	return columnType(tbl, name) == schema.TYPE_DECIMAL
}

// columnIsCaseInsensitive reports whether the column is a string type with a
// case-insensitive collation (MySQL's default). The snapshot compares in that
// collation, so the CDC must match: a `_ci` string column is compared through
// lower(). (Accent-insensitivity is not reproduced.)
func columnIsCaseInsensitive(tbl *schema.Table, name string) bool {
	if tbl == nil {
		return false
	}
	i := tbl.FindColumn(name)
	if i < 0 {
		return false
	}
	switch tbl.Columns[i].Type {
	case schema.TYPE_STRING, schema.TYPE_ENUM, schema.TYPE_SET:
	default:
		return false
	}
	return strings.Contains(strings.ToLower(tbl.Columns[i].Collation), "_ci")
}

func columnType(tbl *schema.Table, name string) int {
	if tbl == nil {
		return 0
	}
	i := tbl.FindColumn(name)
	if i < 0 {
		return 0
	}
	return tbl.Columns[i].Type
}

// decimalCompare compares two values as exact decimals and returns -1, 0, or 1.
func decimalCompare(a, b any) (int, error) {
	av, err := toDecimal(a)
	if err != nil {
		return 0, err
	}
	bv, err := toDecimal(b)
	if err != nil {
		return 0, err
	}
	return av.Cmp(bv), nil
}

func toDecimal(v any) (decimal.Decimal, error) {
	switch t := v.(type) {
	case decimal.Decimal:
		return t, nil
	case string:
		d, err := decimal.NewFromString(strings.TrimSpace(t))
		if err != nil {
			return decimal.Decimal{}, fmt.Errorf("mysql_decimal_cmp: %q is not a decimal: %w", t, err)
		}
		return d, nil
	case int64:
		return decimal.NewFromInt(t), nil
	case int:
		return decimal.NewFromInt(int64(t)), nil
	case float64:
		return decimal.NewFromFloat(t), nil
	default:
		return decimal.Decimal{}, fmt.Errorf("mysql_decimal_cmp: unsupported type %T", v)
	}
}

// negateFilter returns the logical negation of f with NOT pushed down to the
// leaves (De Morgan). Mirrors the Postgres helper.
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
// float operand; a DECIMAL column compares the literal exactly through
// mysql_decimal_cmp (a float64 literal is parsed back to its shortest decimal,
// and a string literal is exact).
func exprLiteral(v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "nil", nil
	case bool:
		if t {
			return "true", nil
		}
		return "false", nil
	case float64:
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
		return "", fmt.Errorf("mysql: filter: unsupported value type %T", v)
	}
}
