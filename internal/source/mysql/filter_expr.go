package mysql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/shopspring/decimal"

	"github.com/maltzsama/urutau/internal/source/filterexpr"
	"github.com/maltzsama/urutau/spec"
)

// myPredicate adapts the MySQL leaf builder to filterexpr.PredicateBuilder.
type myPredicate struct{ tbl *schema.Table }

func (b myPredicate) Predicate(p *spec.Predicate) (string, error) {
	return filterExprPredicate(p, b.tbl)
}

// projection is a table's CDC read filter: the compiled filter a row must
// satisfy. The emitted column set is the canonical schema (the adapter filters
// it), so the live path appends the projected columns straight into Arrow; only
// the filter needs named access, and it reads a minimal map of just the columns
// it references.
type projection struct {
	program *vm.Program
	// filterCols is the distinct source columns the compiled filter reads.
	// keep builds its row map from exactly these, never the whole row.
	filterCols []string
}

// keep reports whether a decoded row (in table column order) satisfies the
// filter. It materializes a map of ONLY the filter-referenced columns — the
// hot path never builds a full row map (#455).
func (p projection) keep(row []any, tbl *schema.Table, loc *time.Location) (bool, error) {
	if p.program == nil {
		return true, nil
	}
	full := make(map[string]any, len(p.filterCols))
	for _, name := range p.filterCols {
		idx := tbl.FindColumn(name)
		if idx < 0 || idx >= len(row) {
			full[name] = nil
			continue
		}
		full[name] = normalizeCol(tbl.Columns[idx], row[idx], loc)
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

// filterColumns returns the distinct source columns a filter references.
func filterColumns(f *spec.Filter) []string {
	seen := map[string]bool{}
	var out []string
	var walk func(*spec.Filter)
	walk = func(n *spec.Filter) {
		if n == nil {
			return
		}
		for i := range n.All {
			walk(&n.All[i])
		}
		for i := range n.Any {
			walk(&n.Any[i])
		}
		if n.Not != nil {
			walk(n.Not)
		}
		if n.Predicate != nil && n.Predicate.Column != "" && !seen[n.Predicate.Column] {
			seen[n.Predicate.Column] = true
			out = append(out, n.Predicate.Column)
		}
	}
	walk(f)
	return out
}

// newProjection builds a projection, compiling the structured filter to an
// expr program once per table using the introspected column types.
func newProjection(f *spec.Filter, tbl *schema.Table) (projection, error) {
	p := projection{filterCols: filterColumns(f)}
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
	if err := filterexpr.CheckColumns(f, tbl); err != nil {
		return nil, err
	}
	src, err := filterexpr.Source(f, "mysql", myPredicate{tbl})
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
		l, err := filterexpr.Literal("mysql", v)
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
		return fmt.Sprintf("(%s != nil) && (%s %s %s)", raw, lhs, filterexpr.Operator(p.Op), lit), nil
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
		lit, err := filterexpr.Literal("mysql", p.Value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s && (mysql_decimal_cmp(%s, %s) %s 0)", guard, raw, lit, filterexpr.Operator(p.Op)), nil
	case spec.OpIn, spec.OpNotIn:
		vals, ok := p.Value.([]any)
		if !ok {
			return "", fmt.Errorf("mysql: filter: op %q requires a list value", p.Op)
		}
		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			lit, err := filterexpr.Literal("mysql", v)
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
// is not in the introspected table (see filterexpr.CheckColumns).
func checkFilterColumns(tbl *schema.Table, f *spec.Filter) error {
	return filterexpr.CheckColumns(f, tbl)
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
