package mysql

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
)

// Direct columnar size ceilings. A live transaction is held as Arrow (not as
// map[string]any) until it ends, and a batch is materialized as soon as it
// crosses either ceiling: 2000 rows or 4 MiB. A run of 256 KiB payloads
// (pr_events) closes early instead of buffering the whole row target.
const (
	maxDirectRows  = 2000
	maxDirectBytes = 4 << 20
)

// tableEncoder is one target table's incremental Arrow builder for the live
// binlog path. cs is the resolved canonical (projected) schema; cols maps each
// encoder column to its position in the current binlog table image, so a cell
// goes from the decoder's []any straight into the column's Arrow buffer.
type tableEncoder struct {
	target string
	proj   projection
	enc    *transport.RowEncoder
	cols   []int
	// imgCols is the source table's column count when cols was built. A
	// different count is source drift (ADD/DROP COLUMN): the mapping would
	// otherwise silently misalign or drop a column. Fail loud.
	imgCols int
	bytes   int
	rows    int
}

// newTableEncoder builds an encoder for one target table. The column mapping
// resolves each projected column against the introspected table now; a column
// the table lacks (a filter typo caught earlier, or a rename) fails loud.
func newTableEncoder(target string, cs core.Schema, tbl *schema.Table, proj projection) (*tableEncoder, error) {
	enc, err := transport.NewRowEncoder(cs, nil)
	if err != nil {
		return nil, err
	}
	cols := make([]int, len(cs.Columns))
	for i, c := range cs.Columns {
		idx := tbl.FindColumn(c.Name)
		if idx < 0 {
			enc.Release()
			return nil, fmt.Errorf("%w: %s", transport.ErrColumnNotInSchema, c.Name)
		}
		cols[i] = idx
	}
	return &tableEncoder{target: target, proj: proj, enc: enc, cols: cols, imgCols: len(tbl.Columns)}, nil
}

// appendRow writes one projected image (a binlog row in table column order)
// into the builders and ends the row with meta's operation/position.
func (te *tableEncoder) appendRow(row []any, tbl *schema.Table, loc *time.Location, meta transport.RowMeta) error {
	if err := requireFullImage(tbl, row); err != nil {
		return err
	}
	if len(tbl.Columns) != te.imgCols {
		return fmt.Errorf("mysql: schema drift: table now carries %d columns, the schema was built for %d — declare the change and resume", len(tbl.Columns), te.imgCols)
	}
	for i, ti := range te.cols {
		if ti >= len(row) {
			te.enc.AppendNull(i)
			continue
		}
		v := normalizeCol(tbl.Columns[ti], row[ti], loc)
		te.bytes += cellBytes(v)
		if err := te.enc.AppendValue(i, v); err != nil {
			return err
		}
	}
	te.enc.EndRow(meta)
	te.rows++
	return nil
}

// requireFullImage fails loud when the binlog row carries fewer columns than
// the table. That happens only under binlog_row_image != FULL, where unchanged
// columns are omitted — decoding such a row would silently treat the missing
// columns as nil and corrupt the target. The boot preflight warns; this is the
// runtime guarantee that a partial image never lands.
func requireFullImage(tbl *schema.Table, row []any) error {
	if len(row) < len(tbl.Columns) {
		return fmt.Errorf("mysql: binlog row image carries %d of %d columns — set binlog_row_image=FULL (a partial image cannot be decoded safely)", len(row), len(tbl.Columns))
	}
	return nil
}

// cellBytes estimates one cell's Arrow payload for the byte ceiling.
func cellBytes(v any) int {
	switch t := v.(type) {
	case nil:
		return 1
	case string:
		return len(t)
	case []byte:
		return len(t)
	case time.Time:
		return 16
	default:
		return 8
	}
}

// materialize turns the encoder's buffered rows into one record and resets it.
// Returns nil when the encoder holds nothing.
func (te *tableEncoder) materialize() arrow.RecordBatch {
	if te.rows == 0 {
		return nil
	}
	rec := te.enc.NewRecord()
	te.rows, te.bytes = 0, 0
	return rec
}

// heldRec is one materialized record of the transaction being decoded, before
// its position is finalized.
type heldRec struct {
	target string
	rec    arrow.RecordBatch
}
