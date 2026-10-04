package postgres

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
// encodes: the chunk was held as maps and again as Arrow, the most
// memory-expensive path in the Postgres snapshot (#590). Here a text, JSON or
// binary cell goes from the driver's buffer into the column's Arrow data
// buffer, and the buffers are sized once for the chunk. Any other cell is
// converted exactly as Scan converts it.
//
// The read runs in one REPEATABLE READ, READ ONLY transaction, as Scan does,
// so the chunk sees a single consistent snapshot. Transaction and query setup
// are retried before the first row is appended; mid-scan failures are not
// retried, since retrying after rows have been appended would double-count
// them (they fall back to the coordinator's chunk redo on worker loss).
func (c *Chunker) ScanArrow(ctx context.Context, ch source.Chunk, enc *transport.RowEncoder, expected int) (int, error) {
	q, err := c.chunkQuery(ch)
	if err != nil {
		return 0, err
	}
	query, args, err := q.ToSql()
	if err != nil {
		return 0, fmt.Errorf("postgres: chunk scan sql: %w", err)
	}

	// Retry ONLY the setup — a fresh transaction and its query — before any
	// row is appended. Re-running a mid-scan failure would double-count the
	// rows already written into enc, so those fall back to the worker
	// session's chunk redo (the coordinator redoes uncommitted chunks when a
	// worker is lost). Scan can retry the whole read because it buffers the
	// rows first; ScanArrow streams into the caller's builders and cannot.
	var (
		tx   *sql.Tx
		rows *sql.Rows
	)
	err = retryTransientErr(ctx, c.retries, func() error {
		var e error
		tx, e = c.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
			ReadOnly:  true,
		})
		if e != nil {
			return fmt.Errorf("postgres: chunk scan tx: %w", e)
		}
		rows, e = tx.QueryContext(ctx, query, args...)
		if e != nil {
			_ = tx.Rollback()
			return fmt.Errorf("postgres: chunk scan: %w", e)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	defer func() { _ = tx.Rollback() }()

	names, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	// Each result column's encoder column and scan target: the driver's
	// buffer only for a PostgreSQL text, JSON or binary column; any other
	// type (numeric, temporal, bool) goes through normalize + AppendValue
	// exactly as Scan does. Selecting the raw path from the SOURCE type, not
	// the destination schema, matters when a cast maps a TIMESTAMPTZ source
	// to a string target: scanning time.Time into *[]byte would fail
	// (Sourcery finding on #590).
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
		if enc.IsBytes(col) && pgRawBytesType(types[i].DatabaseTypeName()) {
			raw[i] = true
			dest[i] = new([]byte)
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
			return n, fmt.Errorf("postgres: chunk scan row: %w", err)
		}
		for i, col := range cols {
			if raw[i] {
				enc.AppendBytes(col, *dest[i].(*[]byte))
				continue
			}
			if err := enc.AppendValue(col, normalize(*dest[i].(*any))); err != nil {
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
	if err := rows.Err(); err != nil {
		return n, err
	}
	if err := tx.Commit(); err != nil {
		return n, fmt.Errorf("postgres: chunk scan commit: %w", err)
	}
	return n, nil
}

// pgRawBytesType reports whether a PostgreSQL column type reaches Scan as
// bytes that normalize turns into the same string: the text, JSON and binary
// types. Any other type goes through normalize + AppendValue, exactly as the
// row path.
func pgRawBytesType(dbType string) bool {
	switch dbType {
	case "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME", "JSON", "JSONB", "BYTEA", "XML":
		return true
	}
	return false
}

// ScanArrowPages reads one chunk straight into enc, cutting it into pages when
// the accumulated data bytes reach maxBytes. emit is called per page with the
// page's record (the caller owns it) and its row count; it may block (the
// worker's backpressure). The read runs in one REPEATABLE READ, READ ONLY
// transaction — the same as ScanArrow — so every page sees one consistent
// snapshot; the transaction is committed after the last page (#622).
func (c *Chunker) ScanArrowPages(ctx context.Context, ch source.Chunk, enc *transport.RowEncoder, maxBytes int, emit func(rec arrow.RecordBatch, n int) error) (int, error) {
	q, err := c.chunkQuery(ch)
	if err != nil {
		return 0, err
	}
	query, args, err := q.ToSql()
	if err != nil {
		return 0, fmt.Errorf("postgres: chunk scan sql: %w", err)
	}

	var (
		tx   *sql.Tx
		rows *sql.Rows
	)
	err = retryTransientErr(ctx, c.retries, func() error {
		var e error
		tx, e = c.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
			ReadOnly:  true,
		})
		if e != nil {
			return fmt.Errorf("postgres: chunk scan tx: %w", e)
		}
		rows, e = tx.QueryContext(ctx, query, args...)
		if e != nil {
			_ = tx.Rollback()
			return fmt.Errorf("postgres: chunk scan: %w", e)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	defer func() { _ = tx.Rollback() }()

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
		if enc.IsBytes(col) && pgRawBytesType(types[i].DatabaseTypeName()) {
			raw[i] = true
			dest[i] = new([]byte)
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
			return total, fmt.Errorf("postgres: chunk scan row: %w", err)
		}
		for i, col := range cols {
			if raw[i] {
				enc.AppendBytes(col, *dest[i].(*[]byte))
				continue
			}
			if err := enc.AppendValue(col, normalize(*dest[i].(*any))); err != nil {
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
	if err := tx.Commit(); err != nil {
		return total, fmt.Errorf("postgres: chunk scan commit: %w", err)
	}
	return total, nil
}
