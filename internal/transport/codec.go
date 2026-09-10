// Flight data-plane codec: change batches travel as Arrow IPC records with
// a BatchMeta proto in the FlightData app_metadata. Rows use a typed wire
// schema derived from the canonical core.Schema — each column travels as
// its native Arrow type, no JSON blobs, no lossy round-trips.
package transport

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// EncodeBatch renders rows as one complete Arrow IPC stream (schema +
// record) and marshals meta for the FlightData app_metadata. The schema
// is derived from the canonical core.Schema: data columns are typed,
// metadata columns (__op, __pos, etc.) are appended at the end.
// EncodeBatch encodes row-oriented changes into an Arrow IPC record body
// plus the BatchMeta frame (FlightData.app_metadata). A nil alloc falls
// back to the default allocator (M-5).
func EncodeBatch(rows []rowchange.Change, cs core.Schema, meta *pb.BatchMeta, alloc memory.Allocator) (body, metaBytes []byte, err error) {
	metaBytes, err = proto.Marshal(meta)
	if err != nil {
		return nil, nil, fmt.Errorf("transport: marshal batch meta: %w", err)
	}

	rec, err := RecordFromChanges(rows, cs, alloc)
	if err != nil {
		return nil, nil, err
	}
	defer rec.Release()

	body, err = recordToIPC(rec)
	if err != nil {
		return nil, nil, err
	}
	return body, metaBytes, nil
}

// DecodeBatch reads a typed Arrow record + app_metadata back into rows and
// meta. Column types are read from the RecordBatch's embedded schema — no
// separate schema parameter needed. Values are read directly from typed
// columns — no JSON parsing, no float64 normalization, no precision loss.
//
// primaryKey names the columns that form the change key, in key order; the
// decoded Key tuple is rebuilt from the row's own values. The wire schema
// does not carry the key separately — the sink's equality deletes need it,
// and an empty key would make every commit fail on arity. Pass nil only for
// batches whose consumer never commits (tests).
// DecodeBatch decodes an Arrow IPC record into row-oriented changes.
//
// The table identity is passed explicitly: on the real wire the BatchMeta
// frame (FlightData.app_metadata) is parsed by the transport layer BEFORE
// decode (worker/remote.go), so the codec takes the already-parsed identity
// instead of re-parsing bytes. Empty table is rejected — a batch without
// identity must not decode silently (M-2).
func DecodeBatch(rec arrow.RecordBatch, table string, primaryKey []string) ([]rowchange.Change, error) {
	if table == "" {
		return nil, fmt.Errorf("transport: batch has no identity — empty table")
	}

	schema := rec.Schema()
	numDataCols, err := validateWireSchema(schema)
	if err != nil {
		return nil, err
	}

	// Pre-resolve core types for each data column from the Arrow schema,
	// honoring extension metadata (uuid/json) so the Kind survives the wire.
	colTypes := make([]core.ColumnType, numDataCols)
	for j := 0; j < numDataCols; j++ {
		ct, err := fieldTypeToCore(schema.Field(j))
		if err != nil {
			return nil, fmt.Errorf("transport: column %d: %w", j, err)
		}
		colTypes[j] = ct
	}
	// Resolve key column positions once: data-column index per PK name.
	keyCols := make([]int, 0, len(primaryKey))
	for _, name := range primaryKey {
		idx := -1
		for j := 0; j < numDataCols; j++ {
			if schema.Field(j).Name == name {
				idx = j
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("transport: primary key column %q not in batch schema", name)
		}
		keyCols = append(keyCols, idx)
	}

	rows := make([]rowchange.Change, 0, rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		// No wall-clock fallback (M-4): an absent __ingest_ts stays a zero
		// time. IngestTS is measured upstream where the event entered the
		// pipeline; a decode-time stamp would silently re-age replayed
		// rows and corrupt lag metrics.
		c := rowchange.Change{
			Table: table,
		}

		// Data columns → After map.
		c.After = make(map[string]any, numDataCols)
		for j := 0; j < numDataCols; j++ {
			field := schema.Field(j)
			v, err := readTypedValue(rec.Column(j), colTypes[j], i)
			if err != nil {
				return nil, fmt.Errorf("transport: column %q row %d: %w", field.Name, i, err)
			}
			if v != nil {
				c.After[field.Name] = v
			} else {
				delete(c.After, field.Name)
			}
		}

		// Rebuild the key tuple from the row's own values, in PK order.
		if len(keyCols) > 0 {
			c.Key = make([]any, len(keyCols))
			for k, j := range keyCols {
				c.Key[k] = c.After[schema.Field(j).Name]
			}
		}

		// Metadata columns (last 5, fixed positions).
		opCol, _ := rec.Column(numDataCols).(*array.Uint8)
		c.Op = rowchange.Op(opCol.Value(i))
		posCol, _ := rec.Column(numDataCols + 1).(*array.String)
		c.Position = posCol.Value(i)
		tsCol, _ := rec.Column(numDataCols + 2).(*array.Timestamp)
		if !tsCol.IsNull(i) {
			c.CommitTS = tsCol.Value(i).ToTime(arrow.Nanosecond)
		}
		ingestCol, _ := rec.Column(numDataCols + 3).(*array.Timestamp)
		if !ingestCol.IsNull(i) {
			c.IngestTS = ingestCol.Value(i).ToTime(arrow.Microsecond)
		}
		snapCol, _ := rec.Column(numDataCols + 4).(*array.Boolean)
		c.Snapshot = snapCol.Value(i)

		rows = append(rows, c)
	}
	return rows, nil
}

// ── typed value helpers ──────────────────────────────────────────────

// arrowTypeToCore maps an Arrow DataType back to a core.ColumnType for
// driving the typed decoder. Unmappable types are an ERROR (M-1) — the
// old silent KindString fallback corrupted data downstream instead of
// failing at the decode boundary.
func arrowTypeToCore(dt arrow.DataType) (core.ColumnType, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return core.ColumnType{Kind: core.KindBool}, nil
	case arrow.INT32:
		return core.ColumnType{Kind: core.KindInt32}, nil
	case arrow.INT64:
		return core.ColumnType{Kind: core.KindInt64}, nil
	case arrow.UINT64:
		return core.ColumnType{Kind: core.KindUInt64}, nil
	case arrow.FLOAT32:
		return core.ColumnType{Kind: core.KindFloat32}, nil
	case arrow.FLOAT64:
		return core.ColumnType{Kind: core.KindFloat64}, nil
	case arrow.DECIMAL128:
		d := dt.(*arrow.Decimal128Type)
		return core.ColumnType{Kind: core.KindDecimal, Precision: int(d.Precision), Scale: int(d.Scale)}, nil
	case arrow.STRING:
		return core.ColumnType{Kind: core.KindString}, nil
	case arrow.BINARY:
		return core.ColumnType{Kind: core.KindBinary}, nil
	case arrow.DATE32:
		return core.ColumnType{Kind: core.KindDate}, nil
	case arrow.TIME64:
		return core.ColumnType{Kind: core.KindTime}, nil
	case arrow.TIMESTAMP:
		tt := dt.(*arrow.TimestampType)
		if tt.TimeZone != "" {
			return core.ColumnType{Kind: core.KindTimestampTZ}, nil
		}
		return core.ColumnType{Kind: core.KindTimestamp}, nil
	case arrow.FIXED_SIZE_BINARY:
		// A bare fixed-size binary without the uuid extension is a fixed
		// byte sequence; uuid is disambiguated at the field level via
		// extension metadata (fieldTypeToCore).
		fsb := dt.(*arrow.FixedSizeBinaryType)
		return core.ColumnType{Kind: core.KindFixedBinary, FixedSize: int(fsb.ByteWidth)}, nil
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		ct := core.ColumnType{Kind: core.KindStruct, Fields: make([]core.Column, 0, len(st.Fields()))}
		for _, f := range st.Fields() {
			ft, err := fieldTypeToCore(f)
			if err != nil {
				return core.ColumnType{}, fmt.Errorf("struct field %q: %w", f.Name, err)
			}
			ft.Nullable = f.Nullable
			ct.Fields = append(ct.Fields, core.Column{Name: f.Name, Type: ft})
		}
		return ct, nil
	case arrow.LIST:
		lt := dt.(*arrow.ListType)
		ef := lt.ElemField()
		et, err := fieldTypeToCore(ef)
		if err != nil {
			return core.ColumnType{}, fmt.Errorf("list elem: %w", err)
		}
		et.Nullable = ef.Nullable
		return core.ColumnType{Kind: core.KindList, Elem: &et}, nil
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		kf := mt.KeyField()
		vf := mt.ItemField()
		kt, err := fieldTypeToCore(kf)
		if err != nil {
			return core.ColumnType{}, fmt.Errorf("map key: %w", err)
		}
		vt, err := fieldTypeToCore(vf)
		if err != nil {
			return core.ColumnType{}, fmt.Errorf("map value: %w", err)
		}
		kt.Nullable = kf.Nullable
		vt.Nullable = vf.Nullable
		return core.ColumnType{Kind: core.KindMap, KeyType: &kt, ValueType: &vt}, nil
	default:
		// Closed world (RV-03): only types kindToArrow produces are
		// decodable. Widened mappings (uint32 -> int64, float16 ->
		// float32, ...) previously let a mismatched array reach a hard
		// type assertion in readTypedValue — a remote-triggerable panic.
		// The wire is ours end to end: what we do not produce, we reject.
		return core.ColumnType{}, fmt.Errorf("transport: arrow type %s is not produced by the encoder — rejected at decode", dt)
	}
}

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
		case float64:
			// RV-04: math.MaxInt64 as an untyped constant converts to
			// float64 as exactly 2^63 (float64 cannot represent 2^63-1
			// and rounds UP). The exclusive bound is therefore >=: t = 2^63
			// must be REJECTED even though `t > math.MaxInt64` reads false.
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
		default:
			return fmt.Errorf("want float64-compatible, got %T", v)
		}
	case core.KindDecimal:
		switch t := v.(type) {
		case string:
			if err := bld.(*array.Decimal128Builder).AppendValueFromString(t); err != nil {
				return fmt.Errorf("decimal parse: %w", err)
			}
		case int, int32, int64, float32, float64:
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
			raw, err := parseUUIDBytes(t)
			if err != nil {
				return err
			}
			bld.(*array.FixedSizeBinaryBuilder).Append(raw)
		default:
			return fmt.Errorf("want []byte or string for uuid, got %T", v)
		}
	case core.KindFixedBinary:
		// The builder validates the byte width against the field's declared
		// size, surfacing a truncated/oversized value instead of writing it.
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

// parseUUIDBytes parses a hyphenated or compact UUID string into 16 bytes.
func parseUUIDBytes(s string) ([]byte, error) {
	compact := make([]byte, 0, 32)
	for _, c := range s {
		if c == '-' {
			continue
		}
		compact = append(compact, byte(c))
	}
	if len(compact) != 32 {
		return nil, fmt.Errorf("uuid: want 32 hex chars, got %d", len(compact))
	}
	raw := make([]byte, 16)
	for i := 0; i < 16; i++ {
		hi := hexDigit(compact[i*2])
		lo := hexDigit(compact[i*2+1])
		if hi < 0 || lo < 0 {
			return nil, fmt.Errorf("uuid: invalid hex char")
		}
		raw[i] = byte(hi<<4 | lo)
	}
	return raw, nil
}

func hexDigit(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	default:
		return -1
	}
}

// isIntegralFloat reports whether f is an integer representable without loss.
func isIntegralFloat(f float64) bool {
	return f == math.Trunc(f) && !math.IsInf(f, 0) && !math.IsNaN(f)
}
