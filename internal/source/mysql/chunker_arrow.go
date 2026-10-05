package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/source"
)

// reserveAfterRows is how many rows ScanArrow reads before sizing the
// encoder's buffers for the whole chunk from their average.
const reserveAfterRows = 32

// ScanArrow reads one chunk straight into enc, one snapshot insert per row,
// and returns the row count; expected is the chunk's expected row count. An
// encoder the caller sized already (ReservePerRow) is used as is; otherwise
// it is sized from the chunk's first rows.
//
// Scan hands every row over as a map of Go values, which the caller then
// encodes: the chunk was held as maps and again as Arrow, and a 65 MB chunk
// of the full profile's events swung the worker's heap to ~0.9 GB (#448).
// Here a text, JSON or binary cell goes from the driver's buffer
// (sql.RawBytes, valid until the next row) into the column's Arrow data
// buffer, and the buffers are sized once for the chunk: the chunk is held
// once. Any other cell is converted exactly as Scan converts it.
func (c *Chunker) ScanArrow(ctx context.Context, ch source.Chunk, enc *transport.RowEncoder, expected int) (int, error) {
	query, args, err := c.chunkQuery(ch)
	if err != nil {
		return 0, err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("mysql: chunk scan: %w", err)
	}
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	// Each result column's encoder column and scan target: the driver's
	// buffer for a text or binary column, a Go value for any other.
	cols := make([]int, len(names))
	raw := make([]bool, len(names))
	dest := make([]any, len(names))
	seen := map[int]bool{}
	for i, name := range names {
		col, ok := enc.Column(name)
		if !ok {
			return 0, fmt.Errorf("%w: %s", transport.ErrColumnNotInSchema, name)
		}
		cols[i], seen[col] = col, true
		if enc.IsBytes(col) && rawBytesType(dbTypeName(types, i)) {
			raw[i] = true
			dest[i] = new(sql.RawBytes)
		} else {
			dest[i] = new(any)
		}
	}
	// Schema columns the SELECT does not return are NULL, as in Scan's rows.
	var absent []int
	for col := range enc.Schema().Columns {
		if !seen[col] {
			absent = append(absent, col)
		}
	}

	n := 0
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return n, fmt.Errorf("mysql: chunk scan row: %w", err)
		}
		for i, col := range cols {
			if raw[i] {
				enc.AppendBytes(col, *dest[i].(*sql.RawBytes))
				continue
			}
			v := normalizeSnapshot(*dest[i].(*any), dbTypeName(types, i), c.loc)
			if err := enc.AppendValue(col, v); err != nil {
				return n, err
			}
		}
		for _, col := range absent {
			enc.AppendNull(col)
		}
		enc.EndRow(transport.RowMeta{Op: rowchange.OpInsert, IngestTS: time.Now(), Snapshot: true})
		n++
		if n == reserveAfterRows {
			enc.Reserve(expected)
		}
	}
	return n, rows.Err()
}

// rawBytesType reports whether a MySQL column type reaches Scan as bytes that
// normalize turns into the same string: text, JSON and binary types. A
// temporal or numeric column goes through normalizeSnapshot instead.
func rawBytesType(dbType string) bool {
	switch dbType {
	case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",
		"BINARY", "VARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "JSON":
		return true
	}
	return false
}

// ScanArrowPages reads one chunk straight into enc, cutting it into pages when
// the accumulated data bytes reach maxBytes. emit is called per page with the
// page's record (the caller owns it) and its row count; it may block (the
// worker's backpressure) while the SQL cursor stays open — acceptable here,
// because the worker's query connection is a dedicated pool, not the
// replication stream (#622). The setup is ScanArrow's, byte for byte; only the
// loop cuts.
func (c *Chunker) ScanArrowPages(ctx context.Context, ch source.Chunk, enc *transport.RowEncoder, maxBytes int, emit func(rec arrow.RecordBatch, n int) error) (int, error) {
	query, args, err := c.chunkQuery(ch)
	if err != nil {
		return 0, err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("mysql: chunk scan: %w", err)
	}
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	cols := make([]int, len(names))
	raw := make([]bool, len(names))
	dest := make([]any, len(names))
	seen := map[int]bool{}
	for i, name := range names {
		col, ok := enc.Column(name)
		if !ok {
			return 0, fmt.Errorf("%w: %s", transport.ErrColumnNotInSchema, name)
		}
		cols[i], seen[col] = col, true
		if enc.IsBytes(col) && rawBytesType(dbTypeName(types, i)) {
			raw[i] = true
			dest[i] = new(sql.RawBytes)
		} else {
			dest[i] = new(any)
		}
	}
	var absent []int
	for col := range enc.Schema().Columns {
		if !seen[col] {
			absent = append(absent, col)
		}
	}

	total := 0
	page := 0
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return total, fmt.Errorf("mysql: chunk scan row: %w", err)
		}
		for i, col := range cols {
			if raw[i] {
				enc.AppendBytes(col, *dest[i].(*sql.RawBytes))
				continue
			}
			v := normalizeSnapshot(*dest[i].(*any), dbTypeName(types, i), c.loc)
			if err := enc.AppendValue(col, v); err != nil {
				return total, err
			}
		}
		for _, col := range absent {
			enc.AppendNull(col)
		}
		enc.EndRow(transport.RowMeta{Op: rowchange.OpInsert, IngestTS: time.Now(), Snapshot: true})
		total++
		page++
		if maxBytes > 0 && enc.DataBytes() >= maxBytes {
			if err := emit(enc.NewRecord(), page); err != nil {
				return total, err
			}
			page = 0
		}
	}
	if err := rows.Err(); err != nil {
		return total, err
	}
	if page > 0 {
		if err := emit(enc.NewRecord(), page); err != nil {
			return total, err
		}
	}
	return total, nil
}
