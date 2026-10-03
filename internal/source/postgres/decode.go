package postgres

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	pglogrepl "github.com/jackc/pglogrepl"
)

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

// toastSource returns the tuple to recover unchanged TOAST columns from, or
// nil when the old tuple is key-only ('K') and carries no TOAST values.
func toastSource(old *pglogrepl.TupleData, keyOnly bool) *pglogrepl.TupleData {
	if keyOnly {
		return nil
	}
	return old
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
