package spec

import (
	"fmt"
	"strings"
)

// SQL renders the filter as a parameterized WHERE fragment for the source
// dialect (MySQL), so chunk SELECTs only read rows inside the filter. The
// fragment is parenthesized and composes with the chunk bounds by AND.
func (f *Filter) SQL() (string, []any, error) {
	if f == nil {
		return "", nil, nil
	}
	var sb strings.Builder
	args, err := f.appendSQL(&sb)
	if err != nil {
		return "", nil, err
	}
	return sb.String(), args, nil
}

func (f *Filter) appendSQL(sb *strings.Builder) ([]any, error) {
	switch {
	case len(f.All) > 0:
		sb.WriteByte('(')
		args := make([]any, 0, len(f.All))
		for i := range f.All {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			a, err := f.All[i].appendSQL(sb)
			if err != nil {
				return nil, err
			}
			args = append(args, a...)
		}
		sb.WriteByte(')')
		return args, nil
	case len(f.Any) > 0:
		sb.WriteByte('(')
		args := make([]any, 0, len(f.Any))
		for i := range f.Any {
			if i > 0 {
				sb.WriteString(" OR ")
			}
			a, err := f.Any[i].appendSQL(sb)
			if err != nil {
				return nil, err
			}
			args = append(args, a...)
		}
		sb.WriteByte(')')
		return args, nil
	case f.Not != nil:
		sb.WriteString("(NOT ")
		args, err := f.Not.appendSQL(sb)
		if err != nil {
			return nil, err
		}
		sb.WriteByte(')')
		return args, nil
	case f.Predicate != nil:
		return f.Predicate.appendSQL(sb)
	default:
		return nil, fmt.Errorf("spec: filter node is empty")
	}
}

func (p *Predicate) appendSQL(sb *strings.Builder) ([]any, error) {
	col, err := quoteColumn(p.Column)
	if err != nil {
		return nil, err
	}

	switch p.Op {
	case OpIsNull:
		sb.WriteString(col + " IS NULL")
		return nil, nil
	case OpIsNotNull:
		sb.WriteString(col + " IS NOT NULL")
		return nil, nil
	}

	switch p.Op {
	case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
		var op string
		switch p.Op {
		case OpEq:
			op = "="
		case OpNeq:
			op = "<>"
		case OpLt:
			op = "<"
		case OpLte:
			op = "<="
		case OpGt:
			op = ">"
		case OpGte:
			op = ">="
		}
		sb.WriteString("(" + col + " " + op + " ?)")
		return []any{p.Value}, nil
	case OpIn, OpNotIn:
		items, ok := p.Value.([]any)
		if !ok {
			return nil, fmt.Errorf("spec: op %q requires a list value", p.Op)
		}
		if len(items) == 0 {
			// An empty membership: nothing is in the empty set; everything
			// is outside it.
			if p.Op == OpIn {
				sb.WriteString("(FALSE)")
			} else {
				sb.WriteString("(TRUE)")
			}
			return nil, nil
		}
		sb.WriteString("(" + col)
		if p.Op == OpNotIn {
			sb.WriteString(" NOT")
		}
		sb.WriteString(" IN (")
		sb.WriteString(strings.TrimSuffix(strings.Repeat("?, ", len(items)), ", "))
		sb.WriteString("))")
		return items, nil
	default:
		return nil, fmt.Errorf("spec: unknown op %q", p.Op)
	}
}

// quoteColumn backtick-quotes an identifier, rejecting embedded quotes so
// the grammar stays closed over plain column names.
func quoteColumn(col string) (string, error) {
	if col == "" {
		return "", fmt.Errorf("spec: column name is empty")
	}
	if strings.ContainsAny(col, "`\x00") {
		return "", fmt.Errorf("spec: column name %q carries a quote character", col)
	}
	return "`" + col + "`", nil
}
