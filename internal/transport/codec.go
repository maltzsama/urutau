// Flight data-plane codec: change batches travel as Arrow IPC records with
// a BatchMeta proto in the FlightData app_metadata. Rows use a typed wire
// schema derived from the canonical core.Schema — each column travels as
// its native Arrow type, no JSON blobs, no lossy round-trips.
package transport

import (
	"bytes"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// EncodeBatch renders rows as one complete Arrow IPC stream (schema +
// record) and marshals meta for the FlightData app_metadata. The schema
// is derived from the canonical core.Schema: data columns are typed,
// metadata columns (__op, __pos, etc.) are appended at the end.
func EncodeBatch(rows []change.Change, cs core.Schema, meta *pb.BatchMeta) (body, metaBytes []byte, err error) {
	metaBytes, err = proto.Marshal(meta)
	if err != nil {
		return nil, nil, fmt.Errorf("transport: marshal batch meta: %w", err)
	}

	schema, err := CoreSchemaToArrow(cs)
	if err != nil {
		return nil, nil, err
	}

	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bld.Release()

	numDataCols := len(cs.Columns)
	for i := range rows {
		r := &rows[i]
		// Data columns: look up in After (or Before for deletes when After is nil).
		src := r.After
		if src == nil {
			src = r.Before
		}
		// A delete may carry no row image at all — only the key (the MySQL
		// binlog decoder works this way). The equality delete needs the key
		// values on the wire to match at read time, so project the key onto
		// the PK columns; without it the delete file holds NULL tuples and
		// silently deletes nothing.
		if src == nil && len(r.Key) > 0 {
			src = make(map[string]any, len(r.Key))
			for k, name := range cs.PrimaryKey {
				if k < len(r.Key) {
					src[name] = r.Key[k]
				}
			}
		}
		// Same guarantee when a row image exists but lacks a key column
		// (partial before-images): backfill from the key tuple. The row map
		// is copied, never mutated — it belongs to the caller.
		if src != nil && len(cs.PrimaryKey) > 0 && len(r.Key) > 0 {
			missing := false
			for i, name := range cs.PrimaryKey {
				if i < len(r.Key) && src[name] == nil {
					missing = true
					break
				}
			}
			if missing {
				cp := make(map[string]any, len(src)+len(cs.PrimaryKey))
				for k, v := range src {
					cp[k] = v
				}
				for i, name := range cs.PrimaryKey {
					if i < len(r.Key) && cp[name] == nil {
						cp[name] = r.Key[i]
					}
				}
				src = cp
			}
		}
		for j, col := range cs.Columns {
			var v any
			if src != nil {
				v = src[col.Name]
			}
			if err := appendTypedValue(bld.Field(j), col.Type, v); err != nil {
				return nil, nil, fmt.Errorf("transport: column %q row %d: %w", col.Name, i, err)
			}
		}
		// Metadata columns.
		bld.Field(numDataCols).(*array.Uint8Builder).Append(uint8(r.Op))
		bld.Field(numDataCols + 1).(*array.StringBuilder).Append(r.Position)
		if r.CommitTS.IsZero() {
			bld.Field(numDataCols + 2).AppendNull()
		} else {
			bld.Field(numDataCols + 2).(*array.TimestampBuilder).AppendTime(r.CommitTS)
		}
		bld.Field(numDataCols + 3).(*array.TimestampBuilder).AppendTime(r.IngestTS)
		bld.Field(numDataCols + 4).(*array.BooleanBuilder).Append(r.Snapshot)
	}

	rec := bld.NewRecordBatch()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		return nil, nil, fmt.Errorf("transport: ipc write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, nil, fmt.Errorf("transport: ipc close: %w", err)
	}
	return buf.Bytes(), metaBytes, nil
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
func DecodeBatch(rec arrow.RecordBatch, metaBytes []byte, primaryKey []string) ([]change.Change, *pb.BatchMeta, error) {
	meta := &pb.BatchMeta{}
	if err := proto.Unmarshal(metaBytes, meta); err != nil {
		return nil, nil, fmt.Errorf("transport: unmarshal batch meta: %w", err)
	}

	schema := rec.Schema()
	numCols := int(rec.NumCols())
	numDataCols := numCols - 5 // subtract metadata columns

	// Pre-resolve core types for each data column from the Arrow schema,
	// honoring extension metadata (uuid/json) so the Kind survives the wire.
	colTypes := make([]core.ColumnType, numDataCols)
	for j := 0; j < numDataCols; j++ {
		colTypes[j] = fieldTypeToCore(schema.Field(j))
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
			return nil, nil, fmt.Errorf("transport: primary key column %q not in batch schema", name)
		}
		keyCols = append(keyCols, idx)
	}

	rows := make([]change.Change, 0, rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		c := change.Change{
			Table:    meta.Table,
			IngestTS: time.Now(),
		}

		// Data columns → After map.
		c.After = make(map[string]any, numDataCols)
		for j := 0; j < numDataCols; j++ {
			field := schema.Field(j)
			v, err := readTypedValue(rec.Column(j), colTypes[j], i)
			if err != nil {
				return nil, nil, fmt.Errorf("transport: column %q row %d: %w", field.Name, i, err)
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
		c.Op = change.Op(opCol.Value(i))
		posCol, _ := rec.Column(numDataCols + 1).(*array.String)
		c.Position = posCol.Value(i)
		tsCol, _ := rec.Column(numDataCols + 2).(*array.Timestamp)
		if !tsCol.IsNull(i) {
			c.CommitTS = tsCol.Value(i).ToTime(arrow.Microsecond)
		}
		snapCol, _ := rec.Column(numDataCols + 4).(*array.Boolean)
		c.Snapshot = snapCol.Value(i)

		rows = append(rows, c)
	}
	return rows, meta, nil
}

// ── typed value helpers ──────────────────────────────────────────────

// arrowTypeToCore maps an Arrow DataType back to a core.ColumnType for
// driving the typed decoder.
func arrowTypeToCore(dt arrow.DataType) core.ColumnType {
	switch dt.ID() {
	case arrow.BOOL:
		return core.ColumnType{Kind: core.KindBool}
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return core.ColumnType{Kind: core.KindInt32}
	case arrow.INT64:
		return core.ColumnType{Kind: core.KindInt64}
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return core.ColumnType{Kind: core.KindInt64} // unsigned → int64
	case arrow.FLOAT16, arrow.FLOAT32:
		return core.ColumnType{Kind: core.KindFloat32}
	case arrow.FLOAT64:
		return core.ColumnType{Kind: core.KindFloat64}
	case arrow.DECIMAL128:
		d := dt.(*arrow.Decimal128Type)
		return core.ColumnType{Kind: core.KindDecimal, Precision: int(d.Precision), Scale: int(d.Scale)}
	case arrow.STRING, arrow.LARGE_STRING:
		return core.ColumnType{Kind: core.KindString}
	case arrow.BINARY, arrow.LARGE_BINARY:
		return core.ColumnType{Kind: core.KindBinary}
	case arrow.DATE32, arrow.DATE64:
		return core.ColumnType{Kind: core.KindDate}
	case arrow.TIME32, arrow.TIME64:
		return core.ColumnType{Kind: core.KindTime}
	case arrow.TIMESTAMP:
		return core.ColumnType{Kind: core.KindTimestampTZ} // timestamps travel as UTC
	case arrow.FIXED_SIZE_BINARY:
		// A bare fixed-size binary without the uuid extension is a fixed
		// byte sequence; uuid is disambiguated at the field level via
		// extension metadata (fieldTypeToCore).
		fsb := dt.(*arrow.FixedSizeBinaryType)
		return core.ColumnType{Kind: core.KindFixedBinary, FixedSize: int(fsb.ByteWidth)}
	case arrow.STRUCT:
		st := dt.(*arrow.StructType)
		ct := core.ColumnType{Kind: core.KindStruct, Fields: make([]core.Column, 0, len(st.Fields()))}
		for _, f := range st.Fields() {
			ft := fieldTypeToCore(f)
			ft.Nullable = f.Nullable
			ct.Fields = append(ct.Fields, core.Column{Name: f.Name, Type: ft})
		}
		return ct
	case arrow.LIST:
		lt := dt.(*arrow.ListType)
		ef := lt.ElemField()
		et := fieldTypeToCore(ef)
		et.Nullable = ef.Nullable
		return core.ColumnType{Kind: core.KindList, Elem: &et}
	case arrow.MAP:
		mt := dt.(*arrow.MapType)
		kf := mt.KeyField()
		vf := mt.ItemField()
		kt := fieldTypeToCore(kf)
		kt.Nullable = kf.Nullable
		vt := fieldTypeToCore(vf)
		vt.Nullable = vf.Nullable
		return core.ColumnType{Kind: core.KindMap, KeyType: &kt, ValueType: &vt}
	default:
		return core.ColumnType{Kind: core.KindString} // fallback
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
			bld.(*array.Int32Builder).Append(int32(t))
		case int64:
			bld.(*array.Int32Builder).Append(int32(t))
		case float64:
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
			bld.(*array.Int64Builder).Append(int64(t))
		default:
			return fmt.Errorf("want int64-compatible, got %T", v)
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
			bld.(*array.Float64Builder).Append(float64(t))
		case int64:
			bld.(*array.Float64Builder).Append(float64(t))
		case int:
			bld.(*array.Float64Builder).Append(float64(t))
		case int32:
			bld.(*array.Float64Builder).Append(float64(t))
		default:
			return fmt.Errorf("want float64-compatible, got %T", v)
		}
	case core.KindDecimal:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("decimal: want string, got %T", v)
		}
		if err := bld.(*array.Decimal128Builder).AppendValueFromString(s); err != nil {
			return fmt.Errorf("decimal parse: %w", err)
		}
	case core.KindString, core.KindJSON:
		switch t := v.(type) {
		case string:
			bld.(*array.StringBuilder).Append(t)
		case []byte:
			bld.(*array.StringBuilder).Append(string(t))
		default:
			return fmt.Errorf("want string, got %T", v)
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
		// Dates travel as int32 (days since epoch).
		switch t := v.(type) {
		case int32:
			bld.(*array.Int32Builder).Append(t)
		case int64:
			bld.(*array.Int32Builder).Append(int32(t))
		case int:
			bld.(*array.Int32Builder).Append(int32(t))
		default:
			return fmt.Errorf("want date-int32, got %T", v)
		}
	case core.KindTime:
		// Times travel as int64 (micros since midnight).
		switch t := v.(type) {
		case int64:
			bld.(*array.Int64Builder).Append(t)
		case int:
			bld.(*array.Int64Builder).Append(int64(t))
		default:
			return fmt.Errorf("want time-int64, got %T", v)
		}
	case core.KindTimestamp, core.KindTimestampTZ:
		switch t := v.(type) {
		case time.Time:
			bld.(*array.TimestampBuilder).AppendTime(t)
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
	default:
		return fmt.Errorf("unsupported kind %s", ct.Kind)
	}
	return nil
}

// readTypedValue reads a single typed value from an Arrow column at row i.
func readTypedValue(col arrow.Array, ct core.ColumnType, i int) (any, error) {
	if col.IsNull(i) {
		return nil, nil
	}
	switch ct.Kind {
	case core.KindBool:
		return col.(*array.Boolean).Value(i), nil
	case core.KindInt32:
		return col.(*array.Int32).Value(i), nil
	case core.KindInt64:
		return col.(*array.Int64).Value(i), nil
	case core.KindFloat32:
		return float64(col.(*array.Float32).Value(i)), nil // promote to float64 for map[string]any
	case core.KindFloat64:
		return col.(*array.Float64).Value(i), nil
	case core.KindDecimal:
		return col.(*array.Decimal128).ValueStr(i), nil // canonical text form
	case core.KindString, core.KindJSON:
		return col.(*array.String).Value(i), nil
	case core.KindBinary:
		return col.(*array.Binary).Value(i), nil
	case core.KindDate:
		return col.(*array.Int32).Value(i), nil // days since epoch
	case core.KindTime:
		return col.(*array.Int64).Value(i), nil // micros since midnight
	case core.KindTimestamp, core.KindTimestampTZ:
		ts := col.(*array.Timestamp).Value(i)
		return ts.ToTime(arrow.Microsecond), nil
	case core.KindUUID:
		return col.(*array.FixedSizeBinary).Value(i), nil
	case core.KindFixedBinary:
		return col.(*array.FixedSizeBinary).Value(i), nil
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
