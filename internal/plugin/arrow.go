package plugin

import (
	"encoding/base64"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
)

// Arrow <-> core conversion and value readers for the plugin adapters,
// split out of source.go to keep it within the size ratchet.

// arrowToCoreSchema converts an Arrow schema to a core schema.
func arrowToCoreSchema(s *arrow.Schema) core.Schema {
	cols := make([]core.Column, 0, s.NumFields())
	for i := range s.NumFields() {
		f := s.Field(i)
		cols = append(cols, core.Column{
			Name: f.Name,
			Type: arrowTypeToCore(f.Type, f.Nullable),
		})
	}
	return core.Schema{Columns: cols}
}

func arrowTypeToCore(t arrow.DataType, nullable bool) core.ColumnType {
	switch t.ID() {
	case arrow.BOOL:
		return core.ColumnType{Kind: core.KindBool, Nullable: nullable}
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return core.ColumnType{Kind: core.KindInt32, Nullable: nullable}
	case arrow.INT64:
		return core.ColumnType{Kind: core.KindInt64, Nullable: nullable}
	case arrow.UINT64:
		return core.ColumnType{Kind: core.KindUInt64, Nullable: nullable}
	case arrow.FLOAT32:
		return core.ColumnType{Kind: core.KindFloat32, Nullable: nullable}
	case arrow.FLOAT64:
		return core.ColumnType{Kind: core.KindFloat64, Nullable: nullable}
	case arrow.STRING, arrow.LARGE_STRING:
		return core.ColumnType{Kind: core.KindString, Nullable: nullable}
	case arrow.BINARY, arrow.LARGE_BINARY:
		return core.ColumnType{Kind: core.KindBinary, Nullable: nullable}
	case arrow.TIMESTAMP:
		return core.ColumnType{Kind: core.KindTimestampTZ, Nullable: nullable}
	default:
		return core.ColumnType{Kind: core.KindUnknown, Nullable: nullable}
	}
}

func columnIndex(s *arrow.Schema, name string) int {
	for i := range s.NumFields() {
		if s.Field(i).Name == name {
			return i
		}
	}
	return -1
}

func readStringCol(rec arrow.RecordBatch, idx, row int) string {
	if idx < 0 || rec.Column(idx).IsNull(row) {
		return ""
	}
	col := rec.Column(idx).(*array.String)
	return col.Value(row)
}

func readBinaryCol(rec arrow.RecordBatch, idx, row int) string {
	if idx < 0 || rec.Column(idx).IsNull(row) {
		return ""
	}
	col := rec.Column(idx).(*array.Binary)
	return base64.StdEncoding.EncodeToString(col.Value(row))
}

func structToMap(col arrow.Array, row int) (map[string]any, error) {
	if col == nil {
		return nil, nil
	}
	structArr, ok := col.(*array.Struct)
	if !ok {
		return nil, nil
	}
	fields := structArr.DataType().(*arrow.StructType)
	m := make(map[string]any, fields.NumFields())
	for i := range fields.NumFields() {
		field := fields.Field(i)
		// A null field is written explicitly rather than skipped: skipping it
		// drops the column from the schema inferred over the rows, so a column
		// null on every row vanished from the wire schema (issue #568).
		if structArr.Field(i).IsNull(row) {
			m[field.Name] = nil
			continue
		}
		val, err := readValue(structArr.Field(i), row)
		if err != nil {
			return nil, fmt.Errorf("plugin: column %s: %w", field.Name, err)
		}
		m[field.Name] = val
	}
	return m, nil
}

func readValue(col arrow.Array, row int) (any, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(row), nil
	case *array.Int8:
		return int32(c.Value(row)), nil
	case *array.Int16:
		return int32(c.Value(row)), nil
	case *array.Int32:
		return c.Value(row), nil
	case *array.Int64:
		return c.Value(row), nil
	case *array.Uint8:
		return c.Value(row), nil
	case *array.Uint16:
		return c.Value(row), nil
	case *array.Uint32:
		return c.Value(row), nil
	case *array.Uint64:
		return c.Value(row), nil
	case *array.Float32:
		return c.Value(row), nil
	case *array.Float64:
		return c.Value(row), nil
	case *array.String:
		return c.Value(row), nil
	case *array.Binary:
		return c.Value(row), nil
	case *array.Timestamp:
		// Honour the column's unit: the previous UnixMicro read flattened
		// every timestamp to microseconds and misread ns/ms/s by powers of
		// 1000 (issue #568).
		ts := c.DataType().(*arrow.TimestampType)
		return c.Value(row).ToTime(ts.Unit).UTC(), nil
	default:
		// An unmapped type used to become a silent NULL; fail the stream
		// instead, so the plugin author sees it (issue #568).
		return nil, fmt.Errorf("unsupported Arrow type %s", col.DataType())
	}
}

func extractPK(pkCols []string, row map[string]any) []any {
	if row == nil || len(pkCols) == 0 {
		return nil
	}
	keys := make([]any, 0, len(pkCols))
	for _, name := range pkCols {
		v, ok := row[name]
		if !ok {
			return nil
		}
		keys = append(keys, v)
	}
	return keys
}
