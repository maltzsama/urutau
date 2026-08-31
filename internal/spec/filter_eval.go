package spec

// Matches evaluates the filter against a row keyed by column name. The
// grammar closes over all/any/not and the row predicates; evaluation
// follows SQL null semantics: any comparison against a missing (null)
// value is false, except is_null.
func (f *Filter) Matches(row map[string]any) bool {
	if f == nil {
		return true
	}
	if len(f.All) > 0 {
		for i := range f.All {
			if !f.All[i].Matches(row) {
				return false
			}
		}
		return true
	}
	if len(f.Any) > 0 {
		for i := range f.Any {
			if f.Any[i].Matches(row) {
				return true
			}
		}
		return false
	}
	if f.Not != nil {
		return !f.Not.Matches(row)
	}
	if f.Predicate != nil {
		return f.Predicate.Matches(row)
	}
	return true
}

// Matches evaluates one row predicate.
func (p *Predicate) Matches(row map[string]any) bool {
	v, present := row[p.Column]
	if !present {
		v = nil
	}

	switch p.Op {
	case OpIsNull:
		return v == nil
	case OpIsNotNull:
		return v != nil
	}

	// SQL null semantics: comparisons against null are false.
	if v == nil || p.Value == nil {
		return false
	}

	switch p.Op {
	case OpEq:
		return valuesEqual(v, p.Value)
	case OpNeq:
		return !valuesEqual(v, p.Value)
	case OpIn:
		return listContains(p.Value, v)
	case OpNotIn:
		return !listContains(p.Value, v)
	case OpLt, OpLte, OpGt, OpGte:
		c, ok := compareValues(v, p.Value)
		if !ok {
			return false
		}
		switch p.Op {
		case OpLt:
			return c < 0
		case OpLte:
			return c <= 0
		case OpGt:
			return c > 0
		case OpGte:
			return c >= 0
		}
	}
	return false
}

// valuesEqual compares scalars: numerics across int/float widths, then
// strings, then raw equality.
func valuesEqual(a, b any) bool {
	if af, aok := numeric(a); aok {
		if bf, bok := numeric(b); bok {
			return af == bf
		}
		return false
	}
	if as, aok := text(a); aok {
		if bs, bok := text(b); bok {
			return as == bs
		}
		return false
	}
	return a == b
}

// compareValues orders two scalars: numerics first, then strings.
// The bool result reports comparability.
func compareValues(a, b any) (int, bool) {
	if af, aok := numeric(a); aok {
		if bf, bok := numeric(b); bok {
			switch {
			case af < bf:
				return -1, true
			case af > bf:
				return 1, true
			default:
				return 0, true
			}
		}
		return 0, false
	}
	if as, aok := text(a); aok {
		if bs, bok := text(b); bok {
			switch {
			case as < bs:
				return -1, true
			case as > bs:
				return 1, true
			default:
				return 0, true
			}
		}
	}
	return 0, false
}

func numeric(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	}
	return 0, false
}

func text(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

func listContains(list, v any) bool {
	items, ok := list.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if valuesEqual(v, item) {
			return true
		}
	}
	return false
}
