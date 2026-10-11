// Flight data-plane codec: change batches travel as Arrow IPC records with
// a BatchMeta proto in the FlightData app_metadata. Rows use a typed wire
// schema derived from the canonical core.Schema — each column travels as
// its native Arrow type, no JSON blobs, no lossy round-trips.
package transport

import (
	"fmt"

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

		// Metadata columns (trailing, fixed positions).
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
		phaseCol, _ := rec.Column(numDataCols + 5).(*array.String)
		if !phaseCol.IsNull(i) {
			c.Phase = phaseCol.Value(i)
		}

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
