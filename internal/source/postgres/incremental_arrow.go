package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/source"
)

// IncrementalBatch implements source.BatchIncrementalSource (#733): the
// columnar sibling of Incremental. It runs the SAME cursor-bounded page query
// (incrementalQuery), but encodes every row straight into the Arrow builders of
// a transport.RowEncoder built from schema, with no per-row map[string]any in
// between. schema is the engine's canonical wire schema for the target table
// (the shape it installs on the worker), so the returned batch lands exactly
// what the map path's RecordFromChanges would have produced.
//
// The returned batch carries the page's rows, every one stamped with the
// page's resume position (as the map path stamps every change with next), and
// Watermark = []byte(next). A page with no rows returns (next, nil, false,
// nil), which the engine treats as the end of the drain.
func (a Source) IncrementalBatch(ctx context.Context, t source.TableRef, cursor, after string, schema core.Schema) (string, *dataplane.Batch, bool, error) {
	if len(schema.Columns) == 0 {
		return "", nil, false, fmt.Errorf("postgres: incremental: no canonical schema for %s", t.Target)
	}
	query, args, err := a.incrementalQuery(ctx, t, cursor, after)
	if err != nil {
		return "", nil, false, err
	}
	enc, err := transport.NewRowEncoder(schema, nil)
	if err != nil {
		return "", nil, false, fmt.Errorf("postgres: incremental: %s: %w", t.Source, err)
	}
	defer enc.Release()

	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return "", nil, false, fmt.Errorf("postgres: incremental: %w", err)
	}
	defer func() { _ = rows.Close() }()

	names, err := rows.Columns()
	if err != nil {
		return "", nil, false, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return "", nil, false, err
	}
	link, err := newIncrementalArrow(enc, names, dbTypeNames(types))
	if err != nil {
		return "", nil, false, err
	}
	// dest is the scan target per SELECT column: the driver's own []byte for a
	// text/JSON/binary column the encoder writes as bytes, *any for everything
	// else (the same selection ScanArrow makes from the SOURCE type).
	dest := make([]any, len(names))
	for i := range dest {
		if link.raw[i] {
			dest[i] = new([]byte)
		} else {
			dest[i] = new(any)
		}
	}
	vals := make([]any, len(names))

	// The page's last-row cursor/PK and the lookahead row's cursor decide the
	// resume position and `more`, exactly as the map path computes them.
	var (
		n             int
		lastCursor    any
		lastPK        []any
		lookahead     any
		haveLookahead bool
	)
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return "", nil, false, fmt.Errorf("postgres: incremental: row: %w", err)
		}
		derefCells(dest, link.raw, vals)
		if n == incrementalPageSize {
			// The extra LIMIT row: it decides `more` and whether the boundary
			// cursor group spills. It is not part of the page.
			lookahead = cellByName(names, vals, cursor)
			haveLookahead = true
			break
		}
		if err := link.appendRow(vals); err != nil {
			return "", nil, false, err
		}
		lastCursor = cellByName(names, vals, cursor)
		lastPK = pkByName(names, vals, t.PrimaryKey)
		n++
	}
	if err := rows.Err(); err != nil {
		return "", nil, false, err
	}
	if n == 0 {
		return "", nil, false, nil
	}

	nextCursor := formatIncrementalCursor(lastCursor)
	next := nextCursor
	if haveLookahead && formatIncrementalCursor(lookahead) == nextCursor {
		pk := make([]string, 0, len(t.PrimaryKey))
		for _, v := range lastPK {
			pk = append(pk, formatIncrementalCursor(v))
		}
		next = encodeIncrementalPos(nextCursor, pk)
	}

	// Every row carries the page's resume position, so the whole page shares
	// one __pos value; recording it after the fact keeps the hot loop free of
	// knowing next before the page is read.
	rec, err := transport.WithPosition(enc.NewRecord(), next)
	if err != nil {
		return "", nil, false, fmt.Errorf("postgres: incremental: %s: %w", t.Source, err)
	}
	batch := &dataplane.Batch{Table: t.Target, Record: rec, Mode: dataplane.UpsertMode, Watermark: []byte(next)}
	return next, batch, haveLookahead, nil
}

// incrementalArrow maps one incremental page's SELECT columns onto a
// RowEncoder's data columns, so a scanned row is appended straight into the
// Arrow builders with no intermediate map. It is the incremental analogue of
// the chunker's ScanArrow column selection.
type incrementalArrow struct {
	enc *transport.RowEncoder
	// cols[i] is the encoder column the i-th SELECT column writes.
	cols []int
	// raw[i] reports whether the i-th SELECT column's driver value is a []byte
	// the encoder copies directly (text/JSON/binary); otherwise the value is
	// normalized and converted exactly as the map path converts it.
	raw []bool
	// absent are encoder columns the SELECT omits (enrichment destinations,
	// projected-out columns): they are NULL, as in the map path.
	absent []int
}

// newIncrementalArrow resolves names/dbTypes (SELECT-column order) against enc.
// A SELECT column the schema lacks is schema drift: fail loud rather than
// silently drop it (the same choice the chunker and the live encoder make).
func newIncrementalArrow(enc *transport.RowEncoder, names, dbTypes []string) (*incrementalArrow, error) {
	cols := make([]int, len(names))
	raw := make([]bool, len(names))
	seen := make(map[int]bool, len(names))
	for i, name := range names {
		col, ok := enc.Column(name)
		if !ok {
			return nil, fmt.Errorf("%w: %s", transport.ErrColumnNotInSchema, name)
		}
		cols[i], seen[col] = col, true
		raw[i] = enc.IsBytes(col) && pgRawBytesType(dbTypes[i])
	}
	var absent []int
	for col := range enc.Schema().Columns {
		if !seen[col] {
			absent = append(absent, col)
		}
	}
	return &incrementalArrow{enc: enc, cols: cols, raw: raw, absent: absent}, nil
}

// appendRow writes one scanned row (values in SELECT-column order) and ends it
// as an incremental insert. A raw cell goes from the driver's buffer into the
// column's Arrow data buffer; any other cell is normalized and appended the
// same way RecordFromChanges appends it, so the two paths agree byte for byte.
func (a *incrementalArrow) appendRow(values []any) error {
	for i, col := range a.cols {
		if a.raw[i] {
			b, _ := values[i].([]byte)
			a.enc.AppendBytes(col, b)
			continue
		}
		if err := a.enc.AppendValue(col, normalize(values[i])); err != nil {
			return err
		}
	}
	for _, col := range a.absent {
		a.enc.AppendNull(col)
	}
	a.enc.EndRow(transport.RowMeta{Op: rowchange.OpInsert, IngestTS: time.Now(), Phase: core.PhaseIncremental})
	return nil
}

// dbTypeNames flattens the per-column database type names for the raw-bytes
// decision.
func dbTypeNames(types []*sql.ColumnType) []string {
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = t.DatabaseTypeName()
	}
	return out
}

// cellByName returns the normalized value of the named SELECT column, or nil
// when the name is absent. Normalization matches the map path (a []byte becomes
// a string) so a cursor or key value formats identically.
func cellByName(names []string, vals []any, name string) any {
	for i, n := range names {
		if n == name {
			return normalize(vals[i])
		}
	}
	return nil
}

// pkByName returns the named primary-key values in key order, normalized as the
// map path normalizes them.
func pkByName(names []string, vals []any, primaryKey []string) []any {
	out := make([]any, 0, len(primaryKey))
	for _, name := range primaryKey {
		out = append(out, cellByName(names, vals, name))
	}
	return out
}

// derefCells copies the scan targets into vals: a raw cell as its []byte (nil
// for NULL), any other as its scanned value. vals is reused across rows.
func derefCells(dest []any, raw []bool, vals []any) {
	for i := range dest {
		if raw[i] {
			if p := *dest[i].(*[]byte); p != nil {
				vals[i] = p
			} else {
				vals[i] = nil
			}
		} else {
			vals[i] = *dest[i].(*any)
		}
	}
}
