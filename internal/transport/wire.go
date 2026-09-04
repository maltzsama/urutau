// Control-plane wire helpers: schemas and chunk bounds travel as Arrow IPC
// bytes, not JSON. Communication is Arrow end to end — the data plane
// (Flight batches) and the control payloads share one encoding discipline.
package transport

import (
	"bytes"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/internal/core"
)

// EncodeTableSchema serializes the table's data columns as an Arrow IPC
// schema-only stream. Metadata wire columns are excluded — this is the
// TABLE schema, not the batch wire schema.
func EncodeTableSchema(cs core.Schema) ([]byte, error) {
	fields := make([]arrow.Field, 0, len(cs.Columns))
	for _, col := range cs.Columns {
		at, err := kindToArrow(col.Type)
		if err != nil {
			return nil, fmt.Errorf("transport: schema: column %q: %w", col.Name, err)
		}
		fields = append(fields, arrow.Field{
			Name:     col.Name,
			Type:     at,
			Nullable: col.Type.Nullable,
		})
	}
	return marshalArrowSchema(arrow.NewSchema(fields, nil))
}

// DecodeTableSchema reverses EncodeTableSchema. The primary key is not part
// of the Arrow schema — the caller fills it from the assignment's
// primary_key list.
func DecodeTableSchema(b []byte) (core.Schema, error) {
	schema, err := unmarshalArrowSchema(b)
	if err != nil {
		return core.Schema{}, err
	}
	cs := core.Schema{Columns: make([]core.Column, 0, len(schema.Fields()))}
	for _, f := range schema.Fields() {
		if IsMetadataColumn(f.Name) {
			continue
		}
		ct := arrowTypeToCore(f.Type)
		ct.Nullable = f.Nullable
		cs.Columns = append(cs.Columns, core.Column{Name: f.Name, Type: ct})
	}
	return cs, nil
}

// EncodeBounds serializes chunk bounds as a one-or-two-row Arrow record:
// row 0 is the low tuple, row 1 the high tuple (absent for the open-high
// last chunk; a nil low produces an empty record). Column types are
// inferred from the values — the PK tuple's native types survive the wire.
func EncodeBounds(low, high []any) ([]byte, error) {
	width := len(low)
	if high != nil && len(high) > width {
		width = len(high)
	}
	types := make([]arrow.DataType, width)
	for j := 0; j < width; j++ {
		dt, err := inferBoundType(low, high, j)
		if err != nil {
			return nil, err
		}
		types[j] = dt
	}
	fields := make([]arrow.Field, width)
	for j := range fields {
		fields[j] = arrow.Field{Name: fmt.Sprintf("pk%d", j), Type: types[j], Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	bldrs := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldrs.Release()

	rows := make([][]any, 0, 2)
	if low != nil {
		rows = append(rows, low)
	}
	if high != nil {
		rows = append(rows, high)
	}
	for i := range rows {
		for j := 0; j < width; j++ {
			var v any
			if j < len(rows[i]) {
				v = rows[i][j]
			}
			if err := appendTypedValue(bldrs.Field(j), arrowTypeToCore(types[j]), v); err != nil {
				return nil, fmt.Errorf("transport: bounds row %d col %d: %w", i, j, err)
			}
		}
	}
	rec := bldrs.NewRecordBatch()
	defer rec.Release()
	return marshalArrowRecord(schema, rec)
}

// DecodeBounds reverses EncodeBounds: one []any per row, in tuple order.
func DecodeBounds(b []byte) ([][]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("transport: bounds ipc: %w", err)
	}
	defer r.Release()
	rec, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("transport: bounds read: %w", err)
	}
	defer rec.Release()

	schema := rec.Schema()
	rows := make([][]any, 0, rec.NumRows())
	for i := 0; i < int(rec.NumRows()); i++ {
		tuple := make([]any, rec.NumCols())
		for j := 0; j < int(rec.NumCols()); j++ {
			v, err := readTypedValue(rec.Column(j), arrowTypeToCore(schema.Field(j).Type), i)
			if err != nil {
				return nil, fmt.Errorf("transport: bounds row %d col %d: %w", i, j, err)
			}
			tuple[j] = v
		}
		rows = append(rows, tuple)
	}
	return rows, nil
}

// marshalArrowSchema writes a schema-only IPC stream (no records).
func marshalArrowSchema(schema *arrow.Schema) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("transport: schema ipc: %w", err)
	}
	return buf.Bytes(), nil
}

func unmarshalArrowSchema(b []byte) (*arrow.Schema, error) {
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("transport: schema ipc: %w", err)
	}
	defer r.Release()
	return r.Schema(), nil
}

func marshalArrowRecord(schema *arrow.Schema, rec arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		return nil, fmt.Errorf("transport: record ipc: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("transport: record ipc close: %w", err)
	}
	return buf.Bytes(), nil
}

// inferBoundType picks a column's Arrow type from the first non-nil cell
// across both bound tuples. An all-null column decodes as string/null.
func inferBoundType(low, high []any, j int) (arrow.DataType, error) {
	for _, row := range [][]any{low, high} {
		if j < len(row) && row[j] != nil {
			return inferArrowType(row[j])
		}
	}
	return arrow.BinaryTypes.String, nil
}

func inferArrowType(v any) (arrow.DataType, error) {
	switch v.(type) {
	case bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case int32:
		return arrow.PrimitiveTypes.Int32, nil
	case int64:
		return arrow.PrimitiveTypes.Int64, nil
	case float64:
		return arrow.PrimitiveTypes.Float64, nil
	case string:
		return arrow.BinaryTypes.String, nil
	case []byte:
		return arrow.BinaryTypes.Binary, nil
	case time.Time:
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, nil
	default:
		return nil, fmt.Errorf("transport: bounds: unsupported value type %T", v)
	}
}
