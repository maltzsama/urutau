package transport

// BatchReader is a column-oriented cursor over a wire-schema RecordBatch:
// per-row access to op/position/timestamps/key/data values WITHOUT
// materializing rowchange.Change or per-row maps (M4/G3). It is how sinks
// and worker internals consume batches natively — the row universe stays
// at the CDC decoder boundary where it belongs.
//
// Construction validates the wire schema exactly once (the same trailing-5
// validation DecodeBatch applies), so the per-row accessors are assertion-
// free afterwards.
//
// OWNERSHIP: the reader does NOT retain the record. The caller must keep
// the record alive (Rec() accessor returns it with its original refcount)
// for as long as the reader is used.

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// BatchReader exposes columnar access to one wire-schema record.
type BatchReader struct {
	rec       arrow.RecordBatch
	colTypes  []core.ColumnType // per data column
	dataIndex map[string]int    // column name -> data column index
	keyCols   []int             // data-column index per PK name, in PK order
	opIdx     int
	posIdx    int
	commitIdx int
	ingestIdx int
	snapIdx   int
}

// NewBatchReader validates the record against the wire schema and resolves
// column accessors once. Returns an error for non-wire-schema records —
// the same rules DecodeBatch enforces.
func NewBatchReader(rec arrow.RecordBatch, primaryKey []string) (*BatchReader, error) {
	numDataCols, err := validateWireSchema(rec.Schema())
	if err != nil {
		return nil, err
	}
	schema := rec.Schema()

	br := &BatchReader{
		rec:       rec,
		colTypes:  make([]core.ColumnType, numDataCols),
		dataIndex: make(map[string]int, numDataCols),
		opIdx:     numDataCols,
		posIdx:    numDataCols + 1,
		commitIdx: numDataCols + 2,
		ingestIdx: numDataCols + 3,
		snapIdx:   numDataCols + 4,
	}
	for j := 0; j < numDataCols; j++ {
		ct, err := fieldTypeToCore(schema.Field(j))
		if err != nil {
			return nil, fmt.Errorf("transport: column %d: %w", j, err)
		}
		br.colTypes[j] = ct
		br.dataIndex[schema.Field(j).Name] = j
	}
	for _, name := range primaryKey {
		idx, ok := br.dataIndex[name]
		if !ok {
			return nil, fmt.Errorf("transport: primary key column %q not in batch schema", name)
		}
		br.keyCols = append(br.keyCols, idx)
	}
	return br, nil
}

// validateWireSchema enforces the wire layout: the 5 trailing metadata
// columns by name and type, and no reserved names in the data region.
// Returns the number of data columns.
func validateWireSchema(schema *arrow.Schema) (int, error) {
	numCols := schema.NumFields()
	if numCols < 5 {
		return 0, fmt.Errorf("transport: batch has %d columns — wire schema requires >= 5", numCols)
	}
	numDataCols := numCols - 5
	want := []arrow.Field{
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "__pos", Type: arrow.BinaryTypes.String},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
		{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean},
	}
	for k, w := range want {
		f := schema.Field(numDataCols + k)
		if f.Name != w.Name || !arrow.TypeEqual(f.Type, w.Type) {
			return 0, fmt.Errorf(
				"transport: column %d: want %s, got %s(%s) — not wire schema",
				numDataCols+k, w.Name, f.Name, f.Type)
		}
	}
	for j := 0; j < numDataCols; j++ {
		if isMetadataColumn(schema.Field(j).Name) {
			return 0, fmt.Errorf("transport: column %d %q is reserved in the data region — post-AddMetadata batch is not decodable", j, schema.Field(j).Name)
		}
	}
	return numDataCols, nil
}

// Rec returns the underlying record. The reader does not manage its
// lifetime.
func (r *BatchReader) Rec() arrow.RecordBatch { return r.rec }

// NumRows returns the number of rows the cursor reads.
func (r *BatchReader) NumRows() int { return int(r.rec.NumRows()) }

// Op returns the row's operation.
func (r *BatchReader) Op(i int) rowchange.Op {
	return rowchange.Op(r.rec.Column(r.opIdx).(*array.Uint8).Value(i))
}

// Position returns the row's source coordinate.
func (r *BatchReader) Position(i int) string {
	return r.rec.Column(r.posIdx).(*array.String).Value(i)
}

// CommitTS returns the source commit timestamp, zero when null.
func (r *BatchReader) CommitTS(i int) (time.Time, bool) {
	col := r.rec.Column(r.commitIdx).(*array.Timestamp)
	if col.IsNull(i) {
		return time.Time{}, false
	}
	return col.Value(i).ToTime(arrow.Nanosecond), true
}

// IngestTS returns the pipeline ingest timestamp, zero when null.
func (r *BatchReader) IngestTS(i int) (time.Time, bool) {
	col := r.rec.Column(r.ingestIdx).(*array.Timestamp)
	if col.IsNull(i) {
		return time.Time{}, false
	}
	return col.Value(i).ToTime(arrow.Microsecond), true
}

// Snapshot reports a DBLog chunk row (true) versus a live event (false).
func (r *BatchReader) Snapshot(i int) bool {
	return r.rec.Column(r.snapIdx).(*array.Boolean).Value(i)
}

// Key returns the primary-key tuple for the row, in PK order. Values are
// nil when the PK column is null (the caller decides whether that is a
// contract violation).
func (r *BatchReader) Key(i int) []any {
	if len(r.keyCols) == 0 {
		return nil
	}
	key := make([]any, len(r.keyCols))
	for k, idx := range r.keyCols {
		v, _ := r.value(r.rec.Column(idx), r.colTypes[idx], i)
		key[k] = v
	}
	return key
}

// DataColumns returns the data column names in record order.
func (r *BatchReader) DataColumns() []string {
	names := make([]string, len(r.colTypes))
	for i := range r.colTypes {
		names[i] = r.rec.Schema().Field(i).Name
	}
	return names
}

// HasColumn reports whether a data column with the given name exists.
func (r *BatchReader) HasColumn(name string) bool {
	_, ok := r.dataIndex[name]
	return ok
}

// Value returns the canonical Go value of a data column at row i. Null
// reads as nil with ok=true (a present column legitimately holding NULL);
// ok=false means the column does not exist in the batch.
func (r *BatchReader) Value(name string, i int) (any, bool) {
	idx, ok := r.dataIndex[name]
	if !ok {
		return nil, false
	}
	v, err := r.value(r.rec.Column(idx), r.colTypes[idx], i)
	if err != nil {
		// Construction validated the schema; a mismatch here is a bug,
		// not data. Surface it as a missing value.
		return nil, false
	}
	return v, true
}

// value reads one canonical Go value from an Arrow column.
func (r *BatchReader) value(col arrow.Array, ct core.ColumnType, i int) (any, error) {
	return readTypedValue(col, ct, i)
}
