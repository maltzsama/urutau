// Package clickhouse implements the sink contract on ClickHouse. The
// position travels inside every row (a `position` column read back via
// argMax over a monotonic `seq`), never in a second table — atomicity is
// the engine's single-partition INSERT guarantee, not a two-commit dance.
package clickhouse

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/shopspring/decimal"
)

// chType renders the ClickHouse column type for one canonical column. The
// canonical schema arrives resolved (cast already applied by
// core.ResolveSchema), so this is a direct kind→type mapping. Nullability
// follows the canonical flag except where ClickHouse forbids it: sorting-key
// columns cannot be Nullable, so primary-key columns are forced non-null and
// a nil value encodes as the type's zero.
func chType(ct core.ColumnType, nullable bool) (string, error) {
	base, err := chBaseType(ct)
	if err != nil {
		return "", err
	}
	if nullable {
		return "Nullable(" + base + ")", nil
	}
	return base, nil
}

func chBaseType(ct core.ColumnType) (string, error) {
	switch ct.Kind {
	case core.KindBool:
		return "Bool", nil
	case core.KindInt32:
		return "Int32", nil
	case core.KindInt64:
		return "Int64", nil
	case core.KindUInt64:
		return "UInt64", nil
	case core.KindFloat32:
		return "Float32", nil
	case core.KindFloat64:
		return "Float64", nil
	case core.KindString, core.KindJSON, core.KindBinary, core.KindTime, core.KindFixedBinary:
		// ClickHouse has no TIME type; canonical time-of-day, JSON and
		// binary payloads land as String (byte-safe) — the same canonical
		// text the Iceberg sink receives.
		return "String", nil
	case core.KindDate:
		return "Date", nil
	case core.KindTimestamp:
		return "DateTime64(6)", nil
	case core.KindTimestampTZ:
		return "DateTime64(6, 'UTC')", nil
	case core.KindUUID:
		return "UUID", nil
	case core.KindDecimal:
		return fmt.Sprintf("Decimal(%d, %d)", ct.Precision, ct.Scale), nil
	case core.KindList:
		if ct.Elem == nil {
			return "", fmt.Errorf("list requires an element type")
		}
		elem, err := chBaseType(*ct.Elem)
		if err != nil {
			return "", fmt.Errorf("list element: %w", err)
		}
		return "Array(" + elem + ")", nil
	case core.KindMap:
		if ct.KeyType == nil || ct.ValueType == nil {
			return "", fmt.Errorf("map requires key and value types")
		}
		key, err := chBaseType(*ct.KeyType)
		if err != nil {
			return "", fmt.Errorf("map key: %w", err)
		}
		val, err := chBaseType(*ct.ValueType)
		if err != nil {
			return "", fmt.Errorf("map value: %w", err)
		}
		return "Map(" + key + ", " + val + ")", nil
	case core.KindStruct:
		if len(ct.Fields) == 0 {
			return "", fmt.Errorf("struct requires fields")
		}
		parts := make([]string, len(ct.Fields))
		for i, f := range ct.Fields {
			ft, err := chBaseType(f.Type)
			if err != nil {
				return "", fmt.Errorf("struct field %q: %w", f.Name, err)
			}
			parts[i] = quoteIdent(f.Name) + " " + ft
		}
		return "Tuple(" + strings.Join(parts, ", ") + ")", nil
	case core.KindUnknown:
		if ct.Opaque != nil {
			return "", fmt.Errorf("%s has no canonical form — declare an explicit cast", ct.Opaque)
		}
		return "", fmt.Errorf("unknown column type — declare an explicit cast")
	default:
		return "", fmt.Errorf("unsupported column kind %s", ct.Kind)
	}
}

// parseCHType splits a ClickHouse type string from system.columns into its
// base type and nullability. Only the shapes this sink generates are
// expected: Nullable(X), DateTime64(6[, 'UTC']), Decimal(P, S) and the
// scalar bases.
func parseCHType(s string) (base string, nullable bool) {
	t := strings.TrimSpace(s)
	if strings.HasPrefix(t, "LowCardinality(") && strings.HasSuffix(t, ")") {
		t = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(t, "LowCardinality("), ")"))
	}
	if strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")") {
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(t, "Nullable("), ")")), true
	}
	return t, false
}

// coerce converts one canonical value into the Go representation the
// clickhouse-go column expects for base. Temporal, decimal, uuid and json
// values arrive as their canonical text (same discipline as the Iceberg
// sink); numerics arrive as Go natives. A nil becomes nil here — the caller
// decides between NULL (nullable column) and the type's zero (non-nullable).
func coerce(base string, v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch {
	case base == "String":
		switch t := v.(type) {
		case string:
			return t, nil
		case []byte:
			return string(t), nil
		}
	case base == "Bool":
		switch t := v.(type) {
		case bool:
			return t, nil
		case string:
			switch t {
			case "true", "1":
				return true, nil
			case "false", "0":
				return false, nil
			}
		}
	case base == "Int32" || base == "Int64" || strings.HasPrefix(base, "Int") || strings.HasPrefix(base, "UInt"):
		return toInt(base, v)
	case base == "Float32" || base == "Float64":
		return toFloat(base, v)
	case base == "Date":
		return toTime(v, "2006-01-02")
	case strings.HasPrefix(base, "DateTime64"):
		return toTime(v, "")
	case base == "UUID":
		if s, ok := v.(string); ok {
			return s, nil
		}
	case strings.HasPrefix(base, "Decimal"):
		switch t := v.(type) {
		case string:
			d, err := decimal.NewFromString(t)
			if err != nil {
				return nil, fmt.Errorf("decimal %q: %w", t, err)
			}
			return d, nil
		case decimal.Decimal:
			return t, nil
		}
	case strings.HasPrefix(base, "Array"), strings.HasPrefix(base, "Tuple"):
		// Nested types arrive as []any from the canonical decoder.
		if arr, ok := v.([]any); ok {
			return arr, nil
		}
	case strings.HasPrefix(base, "Map"):
		// Maps arrive as map[string]any or map[any]any from the canonical decoder.
		if m, ok := v.(map[string]any); ok {
			return m, nil
		}
		if m, ok := v.(map[any]any); ok {
			return m, nil
		}
	}
	return nil, fmt.Errorf("cannot encode %T as %s", v, base)
}

// zeroOf is the zero a non-nullable column takes when the value is absent —
// tombstone rows carry only their key, every other column lands as zero.
// Temporals zero at the epoch: time.Time{} (year 1) is outside ClickHouse's
// DateTime64 range.
func zeroOf(base string) any {
	switch {
	case base == "String":
		return ""
	case base == "Bool":
		return false
	case base == "Float32":
		return float32(0)
	case base == "Float64":
		return float64(0)
	case base == "Date" || strings.HasPrefix(base, "DateTime64"):
		return time.Unix(0, 0).UTC()
	case strings.HasPrefix(base, "Decimal"):
		return decimal.Zero
	case strings.HasPrefix(base, "Array"):
		return []any{}
	case strings.HasPrefix(base, "Map"):
		return map[any]any{}
	case strings.HasPrefix(base, "Tuple"):
		return []any{}
	case strings.HasPrefix(base, "Int") || strings.HasPrefix(base, "UInt"):
		return zeroInt(base)
	default:
		return int64(0)
	}
}

// zeroInt is zeroOf's integer arm: the exact Go type clickhouse-go's typed
// column expects (AppendRow is type-exact).
func zeroInt(base string) any {
	switch base {
	case "Int8":
		return int8(0)
	case "Int16":
		return int16(0)
	case "Int32":
		return int32(0)
	case "Int64":
		return int64(0)
	case "UInt8":
		return uint8(0)
	case "UInt16":
		return uint16(0)
	case "UInt32":
		return uint32(0)
	case "UInt64":
		return uint64(0)
	}
	return int64(0)
}

func toInt(base string, v any) (any, error) {
	if base == "UInt64" {
		u, err := uintValue(base, v)
		if err != nil {
			return nil, err
		}
		return u, nil
	}
	n, err := intValue(base, v)
	if err != nil {
		return nil, err
	}
	switch base {
	case "Int8":
		if n < math.MinInt8 || n > math.MaxInt8 {
			return nil, fmt.Errorf("value %d overflows %s", n, base)
		}
		return int8(n), nil
	case "Int16":
		if n < math.MinInt16 || n > math.MaxInt16 {
			return nil, fmt.Errorf("value %d overflows %s", n, base)
		}
		return int16(n), nil
	case "Int32":
		if n < math.MinInt32 || n > math.MaxInt32 {
			return nil, fmt.Errorf("value %d overflows %s", n, base)
		}
		return int32(n), nil
	case "Int64":
		return n, nil
	case "UInt8":
		if n < 0 || n > math.MaxUint8 {
			return nil, fmt.Errorf("value %d overflows %s", n, base)
		}
		return uint8(n), nil
	case "UInt16":
		if n < 0 || n > math.MaxUint16 {
			return nil, fmt.Errorf("value %d overflows %s", n, base)
		}
		return uint16(n), nil
	case "UInt32":
		if n < 0 || n > math.MaxUint32 {
			return nil, fmt.Errorf("value %d overflows %s", n, base)
		}
		return uint32(n), nil
	}
	return n, nil
}

// intValue normalizes any Go integer, an integral float, or a decimal string
// to int64 — the shared front half of toInt.
func intValue(base string, v any) (int64, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case uint:
		if uint64(t) > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows %s", t, base)
		}
		return int64(t), nil
	case uint8:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint64:
		if t > math.MaxInt64 {
			return 0, fmt.Errorf("value %d overflows %s", t, base)
		}
		return int64(t), nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) || t != math.Trunc(t) {
			return 0, fmt.Errorf("value %v is not integral for %s", t, base)
		}
		if t < math.MinInt64 || t >= math.MaxInt64 {
			return 0, fmt.Errorf("value %v overflows %s", t, base)
		}
		return int64(t), nil
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value %q is not an integer for %s", t, base)
		}
		return n, nil
	}
	return 0, fmt.Errorf("cannot encode %T as %s", v, base)
}

// uintValue is intValue's unsigned twin: it keeps the full uint64 range that
// int64 cannot hold (a BIGINT UNSIGNED source column).
func uintValue(base string, v any) (uint64, error) {
	switch t := v.(type) {
	case int:
		if t < 0 {
			return 0, fmt.Errorf("value %d overflows %s", t, base)
		}
		return uint64(t), nil
	case int8:
		if t < 0 {
			return 0, fmt.Errorf("value %d overflows %s", t, base)
		}
		return uint64(t), nil
	case int16:
		if t < 0 {
			return 0, fmt.Errorf("value %d overflows %s", t, base)
		}
		return uint64(t), nil
	case int32:
		if t < 0 {
			return 0, fmt.Errorf("value %d overflows %s", t, base)
		}
		return uint64(t), nil
	case int64:
		if t < 0 {
			return 0, fmt.Errorf("value %d overflows %s", t, base)
		}
		return uint64(t), nil
	case uint:
		return uint64(t), nil
	case uint8:
		return uint64(t), nil
	case uint16:
		return uint64(t), nil
	case uint32:
		return uint64(t), nil
	case uint64:
		return t, nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) || t != math.Trunc(t) || t < 0 || t >= math.MaxUint64 {
			return 0, fmt.Errorf("value %v is not an unsigned integer for %s", t, base)
		}
		return uint64(t), nil
	case string:
		n, err := strconv.ParseUint(t, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value %q is not an unsigned integer for %s", t, base)
		}
		return n, nil
	}
	return 0, fmt.Errorf("cannot encode %T as %s", v, base)
}

func toFloat(base string, v any) (any, error) {
	f, err := floatValue(v)
	if err != nil {
		return nil, err
	}
	if base == "Float32" {
		return float32(f), nil
	}
	return f, nil
}

func floatValue(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case int:
		return float64(t), nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("value %q is not a number", t)
		}
		return f, nil
	}
	return 0, fmt.Errorf("cannot encode %T as Float", v)
}

func toTime(v any, layout string) (any, error) {
	switch t := v.(type) {
	case time.Time:
		return t, nil
	case string:
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05.999999", "2006-01-02 15:04:05"}
		if layout != "" {
			layouts = append([]string{layout}, layouts...)
		}
		for _, l := range layouts {
			if ts, err := time.Parse(l, t); err == nil {
				return ts, nil
			}
		}
		return nil, fmt.Errorf("timestamp %q: not a valid timestamp", t)
	}
	return nil, fmt.Errorf("cannot encode %T as temporal", v)
}
