package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/maltzsama/urutau/source"
)

// Chunker splits a table by its primary key, using the chunk-skipping
// bounds trick: pick every chunkSize-th key with LIMIT 1 OFFSET n, then read
// the slice between consecutive bounds. Reading ORDER BY pk LIMIT 1 OFFSET n
// is O(n) per bounds query, but avoids scanning gaps and stays correct under
// concurrent inserts (bounds are only seeds for the half-open range).
type Chunker struct {
	db        *sql.DB
	schema    string
	table     string
	pk        []string
	chunkSize int
	// loc is the operator's temporal location. The query connection parses in
	// UTC; Scan re-tags naive temporals into loc so a snapshot row matches the
	// CDC decode of the same row (issue #139).
	loc *time.Location
	// columns is the read projection (#162/#183): the SELECT list. Empty
	// means all columns.
	columns []string
	// filter is the compiled row filter (#163/#183), composed with the chunk
	// bounds.
	filter filterSQL
}

// NewChunker builds a chunker for one source table. loc may be nil (UTC).
// columns is the read projection (empty means all); filter is the compiled
// row filter (empty means none).
func NewChunker(db *sql.DB, source, pk string, chunkSize int, loc *time.Location, columns []string, filter filterSQL) (*Chunker, error) {
	schema, table, ok := strings.Cut(source, ".")
	if !ok {
		return nil, fmt.Errorf("mysql: chunker: source %q must be db.table", source)
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("mysql: chunker: chunkSize must be positive")
	}
	// A spec may write the key list as "a, b"; the raw split would leave a
	// leading space on the second column and break every keyed comparison.
	pks := make([]string, 0, 2)
	for _, c := range strings.Split(pk, ",") {
		if c = strings.TrimSpace(c); c != "" {
			pks = append(pks, c)
		}
	}
	if loc == nil {
		loc = time.UTC
	}
	return &Chunker{
		db:        db,
		schema:    schema,
		table:     table,
		pk:        pks,
		chunkSize: chunkSize,
		loc:       loc,
		columns:   columns,
		filter:    filter,
	}, nil
}

// PK returns the primary key columns the chunker splits by.
func (c *Chunker) PK() []string { return c.pk }

// Bounds returns the ordered list of chunk boundary keys: key[0] is the
// lowest PK, followed by every chunkSize-th key, then nil (the open high
// bound of the last chunk).
func (c *Chunker) Bounds(ctx context.Context) ([][]any, error) {
	cols := strings.Join(c.pk, ", ")
	query := fmt.Sprintf(
		"SELECT %s FROM `%s`.`%s` ORDER BY %s LIMIT 1 OFFSET ?",
		cols, c.schema, c.table, cols,
	)

	var bounds [][]any
	for offset := 0; ; offset += c.chunkSize {
		rows, err := c.db.QueryContext(ctx, query, offset)
		if err != nil {
			return nil, fmt.Errorf("mysql: chunker bounds: %w", err)
		}
		key, err := scanRow(rows)
		_ = rows.Close()
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return nil, err
		}
		bounds = append(bounds, key)
	}
	return bounds, nil
}

// Scan executes the chunk SELECT (with the row-filter WHERE pushed — none
// yet in this milestone) and calls fn for every row, keyed by column name.
func (c *Chunker) Scan(ctx context.Context, ch source.Chunk, fn func(row map[string]any) error) error {
	// Row-constructor comparison keeps composite PKs lexicographic.
	cond := make([]string, 0, 2)
	args := make([]any, 0, 2*len(c.pk))
	cols := "`" + strings.Join(c.pk, "`, `") + "`"

	if ch.Low != nil {
		cond = append(cond, fmt.Sprintf("(%s) >= (%s)", cols, placeholders(len(c.pk))))
		args = append(args, ch.Low...)
	}
	if ch.High != nil {
		cond = append(cond, fmt.Sprintf("(%s) < (%s)", cols, placeholders(len(c.pk))))
		args = append(args, ch.High...)
	}
	where := ""
	if len(cond) > 0 {
		where = " WHERE " + strings.Join(cond, " AND ")
	}
	// The row filter (#163/#183) is composed with the chunk bounds; its args
	// follow the bound args in placeholder order.
	if !c.filter.empty() {
		if where == "" {
			where = " WHERE " + c.filter.where
		} else {
			where += " AND (" + c.filter.where + ")"
		}
		args = append(args, c.filter.args...)
	}

	// The projection (#162/#183) narrows the SELECT list; empty means all.
	selectCols := "*"
	if len(c.columns) > 0 {
		selectCols = "`" + strings.Join(c.columns, "`, `") + "`"
	}
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s`%s ORDER BY %s",
		selectCols, c.schema, c.table, where, cols)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("mysql: chunk scan: %w", err)
	}
	defer func() { _ = rows.Close() }()

	colsMeta, err := rows.Columns()
	if err != nil {
		return err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	for rows.Next() {
		vals := make([]any, len(colsMeta))
		ptrs := make([]any, len(colsMeta))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("mysql: chunk scan row: %w", err)
		}
		m := make(map[string]any, len(colsMeta))
		for i, name := range colsMeta {
			m[name] = normalizeSnapshot(vals[i], dbTypeName(colTypes, i), c.loc)
		}
		if err := fn(m); err != nil {
			return err
		}
	}
	return rows.Err()
}

// dbTypeName returns column i's database type name (e.g. "DATETIME"), or "".
func dbTypeName(cols []*sql.ColumnType, i int) string {
	if i < len(cols) {
		return cols[i].DatabaseTypeName()
	}
	return ""
}

// normalizeSnapshot puts a snapshot cell's temporal value in the operator's
// location, matching the CDC decode: DATETIME (naive) is re-tagged, DATE is
// midnight, TIMESTAMP keeps its instant. The query connection parses in UTC,
// so these arrive UTC-tagged. Non-temporal values fall back to normalize.
func normalizeSnapshot(v any, dbType string, loc *time.Location) any {
	if dbType == "UNSIGNED BIGINT" {
		return normalizeUnsigned(v)
	}
	t, ok := v.(time.Time)
	if !ok {
		return normalize(v)
	}
	switch dbType {
	case "DATETIME":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
	case "DATE":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	case "TIMESTAMP":
		return t.In(loc)
	default:
		return t
	}
}

// normalizeUnsigned returns an UNSIGNED BIGINT cell as uint64. go-sql-driver
// returns one three ways: uint64 over the text protocol (a query without
// args), and over the binary protocol (a query with args, as every bounded
// chunk is) int64 when it fits in 63 bits and its decimal text above that.
// Bounds and rows must carry one type whichever protocol served them: chunk
// bounds that mix int64 and uint64 cannot be encoded, and the uint64 codec
// rejects the text form. A value that is none of these is returned as is, so
// the codec reports it.
func normalizeUnsigned(v any) any {
	switch t := v.(type) {
	case int64:
		if t >= 0 {
			return uint64(t)
		}
	case []byte:
		if u, err := strconv.ParseUint(string(t), 10, 64); err == nil {
			return u
		}
		return string(t)
	case string:
		if u, err := strconv.ParseUint(t, 10, 64); err == nil {
			return u
		}
	}
	return v
}

// scanRow scans one row into a normalized []any (used for chunk bounds).
func scanRow(rows *sql.Rows) ([]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	// Same scalar mapping as Scan, so bounds and snapshot rows agree: a
	// []byte cell here would otherwise flow raw into the persisted bounds
	// and be bound back into SQL as the wrong type.
	for i := range vals {
		if dbTypeName(colTypes, i) == "UNSIGNED BIGINT" {
			vals[i] = normalizeUnsigned(vals[i])
			continue
		}
		vals[i] = normalize(vals[i])
	}
	return vals, nil
}

// placeholders renders n comma-separated "?" placeholders.
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}
