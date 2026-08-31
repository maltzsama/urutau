package postgres

import (
	"fmt"
	"strconv"
	"strings"

	pglogrepl "github.com/jackc/pglogrepl"
)

// tupleToMap decodes one pgoutput tuple into column-name → scalar, using
// the introspected column types. Unchanged-TOAST columns (kind 'u') are
// recovered from the old tuple — REPLICA IDENTITY FULL guarantees one —
// and are a hard error otherwise, because silently dropping a column
// would corrupt the mirror.
func tupleToMap(st *TableState, tuple *pglogrepl.TupleData, old *pglogrepl.TupleData) (map[string]any, error) {
	out := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(st.Columns) {
			return nil, fmt.Errorf("postgres: decode %s.%s: tuple has more columns than introspection",
				st.Schema, st.Name)
		}
		name := st.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			out[name] = nil
		case pglogrepl.TupleDataTypeText:
			v, err := decodeScalar(st.Columns[i].DataType, col.Data)
			if err != nil {
				return nil, fmt.Errorf("postgres: decode %s.%s.%s: %w", st.Schema, st.Name, name, err)
			}
			out[name] = v
		case pglogrepl.TupleDataTypeToast:
			if v, ok := toastFromOld(st, old, i); ok {
				out[name] = v
				continue
			}
			return nil, fmt.Errorf("postgres: decode %s.%s.%s: unchanged TOAST with no old image",
				st.Schema, st.Name, name)
		default:
			return nil, fmt.Errorf("postgres: decode %s.%s.%s: unsupported tuple kind %q",
				st.Schema, st.Name, name, col.DataType)
		}
	}
	return out, nil
}

// toastFromOld recovers column i from the old tuple when it carries a
// text value there.
func toastFromOld(st *TableState, old *pglogrepl.TupleData, i int) (any, bool) {
	if old == nil || i >= len(old.Columns) || i >= len(st.Columns) {
		return nil, false
	}
	col := old.Columns[i]
	if col.DataType != pglogrepl.TupleDataTypeText {
		return nil, false
	}
	v, err := decodeScalar(st.Columns[i].DataType, col.Data)
	if err != nil {
		return nil, false
	}
	return v, true
}

// decodeScalar parses the pgoutput text representation into the scalar
// subset the writer supports (int64, float64, bool, string, nil). Temporal
// and other exotic types stay as their source text — the same contract as
// the snapshot chunker.
func decodeScalar(dataType string, data []byte) (any, error) {
	s := string(data)
	switch strings.ToLower(dataType) {
	case "smallint", "integer", "bigint":
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "real", "double precision":
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "numeric", "money":
		// Money renders as "$1,234.56"; strip the decorations. Numeric
		// NaN parses as float NaN via ParseFloat.
		clean := strings.NewReplacer("$", "", ",", "", " ", "").Replace(s)
		v, err := strconv.ParseFloat(clean, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "boolean":
		switch strings.ToLower(s) {
		case "t", "true":
			return true, nil
		case "f", "false":
			return false, nil
		default:
			return nil, fmt.Errorf("bad boolean %q", s)
		}
	default:
		// text, varchar, char, date, timestamp(tz), uuid, json(b), bytea
		// (hex form), inet, and everything else the schema mapping sends
		// to String.
		return s, nil
	}
}
