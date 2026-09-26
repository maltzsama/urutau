package core

import (
	"math"
	"strconv"
)

// The scalar conversions every package that reads a Go value out of a driver
// or a columnar batch shares (issue #403). Each reports whether the value has
// an exact form in the target type; none maps an unsupported type, or a value
// out of range, to zero.

// AsInt64 widens any Go integer to int64. An unsigned value above MaxInt64 has
// no int64 form and is rejected, never wrapped.
func AsInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int32:
		return int64(t), true
	case int16:
		return int64(t), true
	case int8:
		return int64(t), true
	case int:
		return int64(t), true
	}
	if u, ok := asUnsigned(v); ok && u <= math.MaxInt64 {
		return int64(u), true
	}
	return 0, false
}

// AsUint64 widens any Go integer to uint64. A negative value is rejected.
func AsUint64(v any) (uint64, bool) {
	if u, ok := asUnsigned(v); ok {
		return u, true
	}
	if i, ok := AsInt64(v); ok && i >= 0 {
		return uint64(i), true
	}
	return 0, false
}

// AsFloat64 widens any Go float or integer to float64. Integers beyond 2^53
// round to the nearest float, as any float comparison of them would.
func AsFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	}
	if u, ok := asUnsigned(v); ok {
		return float64(u), true
	}
	if i, ok := AsInt64(v); ok {
		return float64(i), true
	}
	return 0, false
}

// ParseInt64 parses a base-10 integer rendered as text, the form a SQL driver
// returns a column in over a text protocol (e.g. go-sql-driver/mysql without
// column type information).
func ParseInt64(b []byte) (int64, error) {
	return strconv.ParseInt(string(b), 10, 64)
}

func asUnsigned(v any) (uint64, bool) {
	switch t := v.(type) {
	case uint64:
		return t, true
	case uint32:
		return uint64(t), true
	case uint16:
		return uint64(t), true
	case uint8:
		return uint64(t), true
	case uint:
		return uint64(t), true
	}
	return 0, false
}
