package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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
// so the chunk sees a single consistent snapshot. Unlike Scan it does not
// retry: a retry after rows have been appended would double-count them.
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
	// Each result column's encoder column and scan target: the driver's
	// buffer for a text, JSON or binary column, a Go value for any other.
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
		if enc.IsBytes(col) {
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
