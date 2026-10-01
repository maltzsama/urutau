package postgres

import (
	"encoding/hex"
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
	case "numeric":
		// Decimal lands as its text form (the canonical value for
		// KindDecimal); the sink parses it at the column boundary.
		return cleanNumeric(s), nil
	case "money":
		// Money renders as "$1,234.56" or "($1,234.56)" for negatives;
		// strip the decorations and convert parenthesized negatives.
		v, err := strconv.ParseFloat(cleanNumeric(s), 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "bytea":
		// pgoutput transmits bytea as hex text (e.g. "\\x6f6f").
		// Strip the "\\x" prefix and decode to raw bytes.
		hex := strings.TrimPrefix(s, "\\x")
		b, err := decodeStringToBytes(hex)
		if err != nil {
			return nil, fmt.Errorf("bytea hex: %w", err)
		}
		return b, nil
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

// tupleToMapByName decodes a tuple whose column order is given explicitly by
// names. A key-only old tuple ('K') carries just the identity key columns, in
// key order, not the table's full column order, so a positional decode would
// attribute them to the wrong columns (issue #500).
func tupleToMapByName(st *TableState, tuple *pglogrepl.TupleData, names []string) (map[string]any, error) {
	// A key-only tuple must carry exactly the identity key columns: a shorter
	// one is a malformed/truncated identity tuple, and a partial map would let
	// key extraction or filter evaluation run on missing key values.
	if len(tuple.Columns) != len(names) {
		return nil, fmt.Errorf("postgres: decode %s.%s: key tuple has %d columns, want the primary key's %d",
			st.Schema, st.Name, len(tuple.Columns), len(names))
	}
	out := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		name := names[i]
		j := st.FindColumn(name)
		if j < 0 {
			return nil, fmt.Errorf("postgres: decode %s.%s: key column %q not found", st.Schema, st.Name, name)
		}
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			out[name] = nil
		case pglogrepl.TupleDataTypeText:
			v, err := decodeScalar(st.Columns[j].DataType, col.Data)
			if err != nil {
				return nil, fmt.Errorf("postgres: decode %s.%s.%s: %w", st.Schema, st.Name, name, err)
			}
			out[name] = v
		default:
			return nil, fmt.Errorf("postgres: decode %s.%s.%s: unsupported key tuple kind %q",
				st.Schema, st.Name, name, col.DataType)
		}
	}
	return out, nil
}

// toastSource returns the tuple to recover unchanged TOAST columns from, or
// nil when the old tuple is key-only ('K') and carries no TOAST values.
func toastSource(old *pglogrepl.TupleData, keyOnly bool) *pglogrepl.TupleData {
	if keyOnly {
		return nil
	}
	return old
}

// oldTupleToMap decodes an old tuple: a key-only ('K') one by the primary
// key's column order, a full ('O') one positionally (issue #500).
func oldTupleToMap(st *TableState, t *pglogrepl.TupleData, keyOnly bool, key []string) (map[string]any, error) {
	if keyOnly {
		return tupleToMapByName(st, t, key)
	}
	return tupleToMap(st, t, nil)
}

// cleanNumeric strips the money decorations from a numeric text.
// Parenthesized values like "($1,234.56)" are converted to negatives.
func cleanNumeric(s string) string {
	// Convert parenthesized negatives: (1234.56) → -1234.56
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		s = "-" + strings.TrimSuffix(strings.TrimPrefix(s, "("), ")")
	}
	return strings.NewReplacer("$", "", ",", "", " ", "").Replace(s)
}

// decodeStringToBytes decodes a hex string to []byte.
func decodeStringToBytes(s string) ([]byte, error) {
	return hex.DecodeString(s)
}
