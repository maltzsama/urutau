package transport

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/core"
)

// appendTypedValue writes a single Go value into the appropriate Arrow
// builder. nil maps to null. The Go type must match the canonical Kind.
func appendTypedValue(bld array.Builder, ct core.ColumnType, v any) error {
	if v == nil {
		bld.AppendNull()
		return nil
	}
	switch ct.Kind {
	case core.KindBool:
		t, ok := v.(bool)
		if !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
		bld.(*array.BooleanBuilder).Append(t)
	case core.KindInt32:
		switch t := v.(type) {
		case int32:
			bld.(*array.Int32Builder).Append(t)
		case int:
			if t < math.MinInt32 || t > math.MaxInt32 {
				return fmt.Errorf("value %d out of int32 range", t)
			}
			bld.(*array.Int32Builder).Append(int32(t))
		case int64:
			if t < math.MinInt32 || t > math.MaxInt32 {
				return fmt.Errorf("value %d out of int32 range", t)
			}
			bld.(*array.Int32Builder).Append(int32(t))
		case float64:
			// RV-04 family: math.MaxInt32 as float64 rounds to exactly
			// 2^31 (2^31-1 is not representable), so the bound is >=.
			if !isIntegralFloat(t) || t < math.MinInt32 || t >= math.MaxInt32 {
				return fmt.Errorf("value %v is not an integer representable in int32", t)
			}
			bld.(*array.Int32Builder).Append(int32(t))
		default:
			return fmt.Errorf("want int32-compatible, got %T", v)
		}
	case core.KindInt64:
		switch t := v.(type) {
		case int64:
			bld.(*array.Int64Builder).Append(t)
		case int:
			bld.(*array.Int64Builder).Append(int64(t))
		case int32:
			bld.(*array.Int64Builder).Append(int64(t))
		case uint8, uint16, uint32, uint, uint64:
			// An unsigned source column cast to int64: go-mysql delivers
			// it as a Go unsigned type. Above MaxInt64 it cannot land.
			u, _ := core.AsUint64(t)
			if u > math.MaxInt64 {
				return fmt.Errorf("value %d out of int64 range", u)
			}
			bld.(*array.Int64Builder).Append(int64(u))
		case float64:
			// RV-04: MaxInt64 as an untyped constant converts to exactly
			// 2^63 in float64, so the exclusive bound is >=: t = 2^63 must
			// be REJECTED even though `t > math.MaxInt64` reads false.
			if !isIntegralFloat(t) || t < math.MinInt64 || t >= math.MaxInt64 {
				return fmt.Errorf("value %v is not an integer representable in int64", t)
			}
			bld.(*array.Int64Builder).Append(int64(t))
		default:
			return fmt.Errorf("want int64-compatible, got %T", v)
		}
	case core.KindUInt64:
		switch t := v.(type) {
		case uint64:
			bld.(*array.Uint64Builder).Append(t)
		case uint8, uint16, uint32, uint:
			u, _ := core.AsUint64(t)
			bld.(*array.Uint64Builder).Append(u)
		case int:
			if t < 0 {
				return fmt.Errorf("negative value %d is not representable in uint64", t)
			}
			bld.(*array.Uint64Builder).Append(uint64(t))
		case int64:
			if t < 0 {
				return fmt.Errorf("negative value %d is not representable in uint64", t)
			}
			bld.(*array.Uint64Builder).Append(uint64(t))
		case float64:
			// RV-04: math.MaxUint64 converts to float64 as exactly 2^64 —
			// the exclusive bound is >=. `t > math.MaxUint64` never fires.
			if t < 0 || t >= math.MaxUint64 || !isIntegralFloat(t) {
				return fmt.Errorf("value %v is not representable in uint64", t)
			}
			bld.(*array.Uint64Builder).Append(uint64(t))
		default:
			return fmt.Errorf("want uint64-compatible, got %T", v)
		}
	case core.KindFloat32:
		switch t := v.(type) {
		case float32:
			bld.(*array.Float32Builder).Append(t)
		case float64:
			bld.(*array.Float32Builder).Append(float32(t))
		case int64:
			bld.(*array.Float32Builder).Append(float32(t))
		default:
			return fmt.Errorf("want float32-compatible, got %T", v)
		}
	case core.KindFloat64:
		switch t := v.(type) {
		case float64:
			bld.(*array.Float64Builder).Append(t)
		case float32:
			bld.(*array.Float64Builder).Append(float64(t)) // widening — permitido
		case int64:
			if int64(float64(t)) != t {
				return fmt.Errorf("value %d loses precision in float64", t)
			}
			bld.(*array.Float64Builder).Append(float64(t))
		case int:
			if int64(float64(t)) != int64(t) {
				return fmt.Errorf("value %d loses precision in float64", t)
			}
			bld.(*array.Float64Builder).Append(float64(t))
		case int32:
			bld.(*array.Float64Builder).Append(float64(t))
		case uint8, uint16, uint32, uint, uint64:
			// An unsigned source column cast to float64. Above 2^53 a
			// float64 no longer holds every integer; reject a value it
			// would round (2^64 itself is out of uint64 range).
			u, _ := core.AsUint64(t)
			if f := float64(u); f >= 1<<64 || uint64(f) != u {
				return fmt.Errorf("value %d loses precision in float64", u)
			}
			bld.(*array.Float64Builder).Append(float64(u))
		default:
			return fmt.Errorf("want float64-compatible, got %T", v)
		}
	case core.KindDecimal:
		switch t := v.(type) {
		case string:
			if err := bld.(*array.Decimal128Builder).AppendValueFromString(t); err != nil {
				return fmt.Errorf("decimal parse: %w", err)
			}
		case int, int32, int64, float32, float64, uint8, uint16, uint32, uint, uint64:
			// A numeric source column cast to decimal renders its decimal
			// text through the shared kernel, then appends that. The decimal
			// kernel is Kind-agnostic (the value type is enough), so the
			// source Kind is not needed here.
			s, err := (core.CastTarget{Type: ct}).Convert(core.KindUnknown, v)
			if err != nil {
				return err
			}
			if err := bld.(*array.Decimal128Builder).AppendValueFromString(s.(string)); err != nil {
				return fmt.Errorf("decimal parse: %w", err)
			}
		default:
			return fmt.Errorf("decimal: want string or number, got %T", v)
		}
	case core.KindString, core.KindJSON:
		switch t := v.(type) {
		case string:
			bld.(*array.StringBuilder).Append(t)
		case []byte:
			// A text-typed source may hand over raw bytes (the snapshot
			// normalizers only cover some drivers); keep them as-is.
			bld.(*array.StringBuilder).Append(string(t))
		default:
			// "to string always": bool/int/float/time.Time/composite all
			// render through the shared scalar stringifier.
			s, err := core.StringifyScalar(v)
			if err != nil {
				return err
			}
			bld.(*array.StringBuilder).Append(s)
		}
	case core.KindBinary:
		switch t := v.(type) {
		case []byte:
			bld.(*array.BinaryBuilder).Append(t)
		case string:
			bld.(*array.BinaryBuilder).Append([]byte(t))
		default:
			return fmt.Errorf("want []byte, got %T", v)
		}
	case core.KindDate:
		// Dates travel as Date32 (days since epoch).
		switch t := v.(type) {
		case int32:
			bld.(*array.Date32Builder).Append(arrow.Date32(t))
		case int64:
			if t < math.MinInt32 || t > math.MaxInt32 {
				return fmt.Errorf("date %d out of int32 range", t)
			}
			bld.(*array.Date32Builder).Append(arrow.Date32(t))
		case int:
			if int64(t) < math.MinInt32 || int64(t) > math.MaxInt32 {
				return fmt.Errorf("date %d out of int32 range", t)
			}
			bld.(*array.Date32Builder).Append(arrow.Date32(t))
		case time.Time:
			bld.(*array.Date32Builder).Append(arrow.Date32FromTime(t))
		case string:
			tm, err := core.ParseTimestampText(t)
			if err != nil {
				return fmt.Errorf("date: %w", err)
			}
			bld.(*array.Date32Builder).Append(arrow.Date32FromTime(tm))
		default:
			return fmt.Errorf("want date-int32, got %T", v)
		}
	case core.KindTime:
		// Times travel as Time64 (micros since midnight).
		switch t := v.(type) {
		case int64:
			bld.(*array.Time64Builder).Append(arrow.Time64(t))
		case int:
			bld.(*array.Time64Builder).Append(arrow.Time64(t))
		case time.Time:
			bld.(*array.Time64Builder).Append(arrow.Time64(timeOfDayMicros(t)))
		case string:
			micros, err := core.ParseTimeOfDayText(t)
			if err != nil {
				return fmt.Errorf("time: %w", err)
			}
			bld.(*array.Time64Builder).Append(arrow.Time64(micros))
		default:
			return fmt.Errorf("want time-int64, got %T", v)
		}
	case core.KindTimestamp, core.KindTimestampTZ:
		switch t := v.(type) {
		case time.Time:
			bld.(*array.TimestampBuilder).AppendTime(t)
		case string:
			tm, err := core.ParseTimestampText(t)
			if err != nil {
				return fmt.Errorf("timestamp: %w", err)
			}
			bld.(*array.TimestampBuilder).AppendTime(tm)
		default:
			return fmt.Errorf("want time.Time, got %T", v)
		}
	case core.KindUUID:
		switch t := v.(type) {
		case []byte:
			if len(t) != 16 {
				return fmt.Errorf("uuid: want 16 bytes, got %d", len(t))
			}
			bld.(*array.FixedSizeBinaryBuilder).Append(t)
		case string:
			// Parse hex UUID.
			raw, err := core.ParseUUIDBytes(t)
			if err != nil {
				return err
			}
			bld.(*array.FixedSizeBinaryBuilder).Append(raw)
		default:
			return fmt.Errorf("want []byte or string for uuid, got %T", v)
		}
	case core.KindFixedBinary:
		// NOTE: FixedSizeBinaryBuilder.Append panics on a wrong-width value;
		// it does not return an error, so callers must size it exactly.
		switch t := v.(type) {
		case []byte:
			bld.(*array.FixedSizeBinaryBuilder).Append(t)
		case string:
			bld.(*array.FixedSizeBinaryBuilder).Append([]byte(t))
		default:
			return fmt.Errorf("want []byte for fixed binary, got %T", v)
		}
	case core.KindStruct:
		// Canonical Go form: map[string]any keyed by field name. A field
		// absent from the map appends null.
		sb, ok := bld.(*array.StructBuilder)
		if !ok {
			return fmt.Errorf("want struct builder, got %T", bld)
		}
		fields := make(map[string]int, len(ct.Fields))
		for j, f := range ct.Fields {
			fields[f.Name] = j
		}
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("want map[string]any for struct, got %T", v)
		}
		sb.Append(true)
		for j, f := range ct.Fields {
			fv, present := m[f.Name]
			if !present {
				sb.FieldBuilder(j).AppendNull()
				continue
			}
			if err := appendTypedValue(sb.FieldBuilder(j), f.Type, fv); err != nil {
				return fmt.Errorf("struct field %q: %w", f.Name, err)
			}
		}
	case core.KindList:
		lb, ok := bld.(*array.ListBuilder)
		if !ok {
			return fmt.Errorf("want list builder, got %T", bld)
		}
		if ct.Elem == nil {
			return fmt.Errorf("list element type is nil")
		}
		items, ok := v.([]any)
		if !ok {
			return fmt.Errorf("want []any for list, got %T", v)
		}
		lb.Append(true)
		for _, iv := range items {
			if err := appendTypedValue(lb.ValueBuilder(), *ct.Elem, iv); err != nil {
				return fmt.Errorf("list element: %w", err)
			}
		}
	case core.KindMap:
		mb, ok := bld.(*array.MapBuilder)
		if !ok {
			return fmt.Errorf("want map builder, got %T", bld)
		}
		if ct.KeyType == nil || ct.ValueType == nil {
			return fmt.Errorf("map key/value types are nil")
		}
		m, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("want map[string]any for map, got %T", v)
		}
		mb.Append(true)
		for k, mv := range m {
			if err := appendTypedValue(mb.KeyBuilder(), *ct.KeyType, k); err != nil {
				return fmt.Errorf("map key %q: %w", k, err)
			}
			if err := appendTypedValue(mb.ItemBuilder(), *ct.ValueType, mv); err != nil {
				return fmt.Errorf("map value %q: %w", k, err)
			}
		}
	default:
		return fmt.Errorf("unsupported kind %s", ct.Kind)
	}
	return nil
}

// timeOfDayMicros returns the micros since midnight for a wall-clock time,
// ignoring the date and zone (the canonical KindTime representation).
func timeOfDayMicros(t time.Time) int64 {
	return int64(t.Hour())*3_600_000_000 + int64(t.Minute())*60_000_000 +
		int64(t.Second())*1_000_000 + int64(t.Nanosecond())/1_000
}

// readTypedValue reads a single typed value from an Arrow column at row i.
func readTypedValue(col arrow.Array, ct core.ColumnType, i int) (any, error) {
	if col.IsNull(i) {
		return nil, nil
	}
	// Every assertion is comma-ok (RV-03, defense-in-depth): the type was
	// validated at the schema boundary, but a decoding path that feeds a
	// mismatched array must error, not panic a remote-triggerable wire
	// boundary. castErr is the uniform failure.
	castErr := func() (any, error) {
		return nil, fmt.Errorf("transport: column type %s does not match canonical kind %s", col.DataType(), ct.Kind)
	}
	switch ct.Kind {
	case core.KindBool:
		if a, ok := col.(*array.Boolean); ok {
			return a.Value(i), nil
		}
		return castErr()
	case core.KindInt32:
		if a, ok := col.(*array.Int32); ok {
			return a.Value(i), nil
		}
		return castErr()
	case core.KindInt64:
		if a, ok := col.(*array.Int64); ok {
			return a.Value(i), nil
		}
		return castErr()
	case core.KindUInt64:
		if a, ok := col.(*array.Uint64); ok {
			return a.Value(i), nil
		}
		return castErr()
	case core.KindFloat32:
		// Promote to float64 for map[string]any (M-3 contract).
		if a, ok := col.(*array.Float32); ok {
			return float64(a.Value(i)), nil
		}
		return castErr()
	case core.KindFloat64:
		if a, ok := col.(*array.Float64); ok {
			return a.Value(i), nil
		}
		return castErr()
	case core.KindDecimal:
		if a, ok := col.(*array.Decimal128); ok {
			return a.ValueStr(i), nil // canonical text form
		}
		return castErr()
	case core.KindString, core.KindJSON:
		if a, ok := col.(*array.String); ok {
			return a.Value(i), nil
		}
		return castErr()
	case core.KindBinary:
		if a, ok := col.(*array.Binary); ok {
			return bytes.Clone(a.Value(i)), nil
		}
		return castErr()
	case core.KindDate:
		if a, ok := col.(*array.Date32); ok {
			return int32(a.Value(i)), nil // days since epoch
		}
		return castErr()
	case core.KindTime:
		if a, ok := col.(*array.Time64); ok {
			return int64(a.Value(i)), nil // micros since midnight
		}
		return castErr()
	case core.KindTimestamp, core.KindTimestampTZ:
		if a, ok := col.(*array.Timestamp); ok {
			return a.Value(i).ToTime(arrow.Microsecond), nil
		}
		return castErr()
	case core.KindUUID, core.KindFixedBinary:
		if a, ok := col.(*array.FixedSizeBinary); ok {
			return bytes.Clone(a.Value(i)), nil
		}
		return castErr()
	case core.KindStruct:
		a, ok := col.(*array.Struct)
		if !ok {
			return castErr()
		}
		st := a.DataType().(*arrow.StructType)
		out := make(map[string]any, st.NumFields())
		for f := range st.NumFields() {
			if a.Field(f).IsNull(i) {
				continue
			}
			ct, err := fieldTypeToCore(st.Field(f))
			if err != nil {
				return nil, fmt.Errorf("struct field %q: %w", st.Field(f).Name, err)
			}
			fv, err := readTypedValue(a.Field(f), ct, i)
			if err != nil {
				return nil, fmt.Errorf("struct field %q: %w", st.Field(f).Name, err)
			}
			out[st.Field(f).Name] = fv
		}
		return out, nil
	case core.KindList:
		a, ok := col.(*array.List)
		if !ok {
			return castErr()
		}
		ef := a.DataType().(*arrow.ListType).ElemField()
		ect, err := fieldTypeToCore(ef)
		if err != nil {
			return nil, fmt.Errorf("list elem: %w", err)
		}
		s, e := a.ValueOffsets(i)
		items := a.ListValues()
		out := make([]any, 0, e-s)
		for j := s; j < e; j++ {
			if items.IsNull(int(j)) {
				out = append(out, nil)
				continue
			}
			ev, err := readTypedValue(items, ect, int(j))
			if err != nil {
				return nil, fmt.Errorf("list element %d: %w", j-s, err)
			}
			out = append(out, ev)
		}
		return out, nil
	case core.KindMap:
		a, ok := col.(*array.Map)
		if !ok {
			return castErr()
		}
		mt := a.DataType().(*arrow.MapType)
		kct, err := fieldTypeToCore(mt.KeyField())
		if err != nil {
			return nil, fmt.Errorf("map key: %w", err)
		}
		vct, err := fieldTypeToCore(mt.ItemField())
		if err != nil {
			return nil, fmt.Errorf("map value: %w", err)
		}
		s, e := a.ValueOffsets(i)
		out := make(map[string]any, e-s)
		for j := s; j < e; j++ {
			kv, err := readTypedValue(a.Keys(), kct, int(j))
			if err != nil {
				return nil, fmt.Errorf("map key %d: %w", j-s, err)
			}
			ks := fmt.Sprintf("%v", kv)
			if a.Items().IsNull(int(j)) {
				out[ks] = nil
				continue
			}
			vv, err := readTypedValue(a.Items(), vct, int(j))
			if err != nil {
				return nil, fmt.Errorf("map value %q: %w", ks, err)
			}
			out[ks] = vv
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported kind %s", ct.Kind)
	}
}

// isIntegralFloat reports whether f is an integer representable without loss.
func isIntegralFloat(f float64) bool {
	return f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f)
}
