package transport

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// RowEncoder writes rows straight into a wire-schema record's builders, one
// cell at a time, so a source can read into Arrow with no row-oriented copy
// in between: a text or binary cell goes from the driver's buffer into the
// column's data buffer (AppendBytes). RecordFromChanges is the same encoding
// for callers that already hold rowchange.Change rows.
//
// OWNERSHIP: Release the encoder; NewRecord's record is the caller's.
type RowEncoder struct {
	cs    core.Schema
	bld   *array.RecordBuilder
	index map[string]int
	rows  int
	// reserved: the builders are sized for this record's rows.
	reserved bool
}

// NewRowEncoder builds an encoder for cs's data columns followed by the wire
// metadata columns. A nil alloc falls back to the default allocator.
func NewRowEncoder(cs core.Schema, alloc memory.Allocator) (*RowEncoder, error) {
	schema, err := CoreSchemaToArrow(cs)
	if err != nil {
		return nil, err
	}
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	index := make(map[string]int, len(cs.Columns))
	for i, c := range cs.Columns {
		index[c.Name] = i
	}
	return &RowEncoder{cs: cs, bld: array.NewRecordBuilder(alloc, schema), index: index}, nil
}

// Schema is the encoder's data schema.
func (e *RowEncoder) Schema() core.Schema { return e.cs }

// Column returns the data column index of name.
func (e *RowEncoder) Column(name string) (int, bool) {
	i, ok := e.index[name]
	return i, ok
}

// IsBytes reports whether data column col is text, JSON or binary: a column
// AppendBytes writes without converting.
func (e *RowEncoder) IsBytes(col int) bool {
	switch e.cs.Columns[col].Type.Kind {
	case core.KindString, core.KindJSON, core.KindBinary:
		return true
	}
	return false
}

// AppendBytes appends a text, JSON or binary cell by copying b into the
// column's data buffer; nil is NULL. b may be reused by the caller afterwards.
func (e *RowEncoder) AppendBytes(col int, b []byte) {
	fb := e.bld.Field(col)
	if b == nil {
		fb.AppendNull()
		return
	}
	bb := bytesBuilder(fb)
	if bb == nil {
		return
	}
	// Past its reserved size the builder would double the data buffer (to
	// the next power of two): grow it by a quarter instead.
	if need := bb.DataLen() + len(b); need > bb.DataCap() {
		bb.ReserveData(max(len(b), bb.DataCap()/4))
	}
	bb.Append(b)
}

// AppendValue appends a cell of any other kind, converted as
// RecordFromChanges converts it; nil is NULL.
func (e *RowEncoder) AppendValue(col int, v any) error {
	if err := appendTypedValue(e.bld.Field(col), e.cs.Columns[col].Type, v); err != nil {
		return fmt.Errorf("transport: column %q row %d: %w", e.cs.Columns[col].Name, e.rows, err)
	}
	return nil
}

// AppendNull appends a NULL to data column col.
func (e *RowEncoder) AppendNull(col int) { e.bld.Field(col).AppendNull() }

// RowMeta is one row's wire metadata.
type RowMeta struct {
	Op       rowchange.Op
	Position string
	CommitTS time.Time
	IngestTS time.Time
	Snapshot bool
	Phase    string
}

// EndRow appends the row's metadata columns. Every data column must have
// received exactly one cell for the row.
func (e *RowEncoder) EndRow(m RowMeta) {
	appendRowMeta(e.bld, len(e.cs.Columns), m)
	e.rows++
}

// Rows is the number of rows ended so far.
func (e *RowEncoder) Rows() int { return e.rows }

// Sizing the builders once for the rows to come keeps their buffers from
// growing by doubling, which holds up to twice the data plus a copy while
// growing. The size comes from rows already encoded: the previous record of
// the same table (ReservePerRow with its PerRow), or the first rows of this
// one (Reserve). A text or binary buffer gets its average bytes per row plus
// a twentieth; past that it grows by a quarter (AppendBytes).

// PerRow is each data column's average data bytes per row over the rows
// ended so far: its text or binary bytes, 0 for any other column. Read it
// before NewRecord.
func (e *RowEncoder) PerRow() []int {
	per := make([]int, len(e.cs.Columns))
	if e.rows == 0 {
		return per
	}
	for col := range e.cs.Columns {
		if b := bytesBuilder(e.bld.Field(col)); b != nil {
			per[col] = b.DataLen() / e.rows
		}
	}
	return per
}

// ReservePerRow sizes the builders for expected rows at perRow bytes per row
// per column (a previous record's PerRow), before any row is appended. Later
// Reserve calls are then no-ops.
func (e *RowEncoder) ReservePerRow(expected int, perRow []int) {
	if expected <= 0 || len(perRow) != len(e.cs.Columns) {
		return
	}
	e.reserve(expected, perRow)
}

// Reserve sizes the builders for expected rows in total from the rows ended
// so far, unless they are already sized.
func (e *RowEncoder) Reserve(expected int) {
	if e.rows == 0 || expected <= e.rows {
		return
	}
	e.reserve(expected, e.PerRow())
}

func (e *RowEncoder) reserve(expected int, perRow []int) {
	if e.reserved {
		return
	}
	e.reserved = true
	more := expected - e.rows
	e.bld.Reserve(more)
	for col := range e.cs.Columns {
		if b := bytesBuilder(e.bld.Field(col)); b != nil && perRow[col] > 0 {
			b.ReserveData(perRow[col]*more + perRow[col]*expected/20)
		}
	}
}

// bytesBuilder is fb's data builder when fb is a text or binary builder.
func bytesBuilder(fb array.Builder) *array.BinaryBuilder {
	switch t := fb.(type) {
	case *array.StringBuilder:
		return t.BinaryBuilder
	case *array.BinaryBuilder:
		return t
	}
	return nil
}

// NewRecord returns the rows ended so far as a record and resets the encoder.
func (e *RowEncoder) NewRecord() arrow.RecordBatch {
	e.rows, e.reserved = 0, false
	return e.bld.NewRecordBatch()
}

// Release frees the builders.
func (e *RowEncoder) Release() { e.bld.Release() }

// appendRowMeta appends the wire metadata columns that follow numDataCols
// data columns: __op, __pos, __commit_ts, __ingest_ts, __snapshot, __phase.
func appendRowMeta(bld *array.RecordBuilder, numDataCols int, m RowMeta) {
	bld.Field(numDataCols).(*array.Uint8Builder).Append(uint8(m.Op))
	bld.Field(numDataCols + 1).(*array.StringBuilder).Append(m.Position)
	if m.CommitTS.IsZero() {
		bld.Field(numDataCols + 2).AppendNull()
	} else {
		bld.Field(numDataCols + 2).(*array.TimestampBuilder).AppendTime(m.CommitTS)
	}
	if m.IngestTS.IsZero() {
		// Parity with CommitTS (M-4): a zero timestamp means "not set" —
		// it must not masquerade as a real instant on the wire.
		bld.Field(numDataCols + 3).AppendNull()
	} else {
		bld.Field(numDataCols + 3).(*array.TimestampBuilder).AppendTime(m.IngestTS)
	}
	bld.Field(numDataCols + 4).(*array.BooleanBuilder).Append(m.Snapshot)
	// __phase: the producer's value when set, otherwise derived from the
	// Snapshot boolean so producers that predate the Phase field still
	// land a phase on the wire. Empty and non-snapshot → null.
	switch {
	case m.Phase != "":
		bld.Field(numDataCols + 5).(*array.StringBuilder).Append(m.Phase)
	case m.Snapshot:
		bld.Field(numDataCols + 5).(*array.StringBuilder).Append(core.PhaseSnapshot)
	default:
		bld.Field(numDataCols + 5).(*array.StringBuilder).Append(core.PhaseStream)
	}
}

// ErrColumnNotInSchema reports a source column the encoder's schema lacks: a
// caller reading straight into Arrow falls back to rows, which widen the
// schema.
var ErrColumnNotInSchema = errors.New("transport: source column not in the encoder's schema")
