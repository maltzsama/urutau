// Package filterexpr renders the shared spec.Filter AST into an expr-lang
// expression. The traversal, the operator tokens and the literal rendering are
// dialect-independent; only the leaf predicate varies (numeric/decimal
// comparison, integer casts), supplied per dialect through a PredicateBuilder
// (issue #609).
package filterexpr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/maltzsama/urutau/spec"
)

// PredicateBuilder renders one leaf predicate for a dialect.
type PredicateBuilder interface {
	Predicate(p *spec.Predicate) (string, error)
}

// Source renders f as an expr-lang expression, delegating leaves to b. The
// dialect name prefixes error messages ("postgres" / "mysql").
func Source(f *spec.Filter, dialect string, b PredicateBuilder) (string, error) {
	switch {
	case f == nil:
		return "", fmt.Errorf("%s: filter: empty node", dialect)
	case len(f.All) > 0:
		return group(f.All, dialect, b, "&&")
	case len(f.Any) > 0:
		return group(f.Any, dialect, b, "||")
	case f.Not != nil:
		// Push the negation to the leaves so no `!` wraps an unknown-valued
		// comparison.
		return Source(f.Not.Negate(), dialect, b)
	case f.Predicate != nil:
		return b.Predicate(f.Predicate)
	default:
		return "", fmt.Errorf("%s: filter: node carries no all/any/not/where", dialect)
	}
}

func group(nodes []spec.Filter, dialect string, b PredicateBuilder, op string) (string, error) {
	parts := make([]string, 0, len(nodes))
	for i := range nodes {
		p, err := Source(&nodes[i], dialect, b)
		if err != nil {
			return "", err
		}
		parts = append(parts, "("+p+")")
	}
	return strings.Join(parts, " "+op+" "), nil
}

// ColumnFinder reports a source column's index, or -1 when absent.
type ColumnFinder interface {
	FindColumn(name string) int
}

// CheckColumns reports an error when f references a column not in the source
// table. A typo would otherwise emit an unknown identifier into the snapshot
// SQL, and in CDC evaluate a missing map key as nil (so is_null matches every
// row and other predicates reject every row).
func CheckColumns(f *spec.Filter, c ColumnFinder) error {
	if f == nil || c == nil {
		return nil
	}
	switch {
	case len(f.All) > 0:
		for i := range f.All {
			if err := CheckColumns(&f.All[i], c); err != nil {
				return err
			}
		}
	case len(f.Any) > 0:
		for i := range f.Any {
			if err := CheckColumns(&f.Any[i], c); err != nil {
				return err
			}
		}
	case f.Not != nil:
		return CheckColumns(f.Not, c)
	case f.Predicate != nil:
		if c.FindColumn(f.Predicate.Column) < 0 {
			return fmt.Errorf("filter column %q not found in the source table", f.Predicate.Column)
		}
	}
	return nil
}

// Operator maps a spec operator to its expr-lang token ("" for a non-comparison
// operator).
func Operator(op spec.Operator) string {
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

// Literal renders a filter value as an expr literal. JSON numbers are rendered
// as floats so a numeric column (compared through float()) sees a float
// operand.
func Literal(dialect string, v any) (string, error) {
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
		return "", fmt.Errorf("%s: filter: unsupported value type %T", dialect, v)
	}
}
