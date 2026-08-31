package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Chunk is a half-open primary-key range [Low, High). The last chunk of a
// table is emitted with the marker in the orchestrator; Low/High are tuples
// in PK column order.
type Chunk struct {
	Low  []any
	High []any
}

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

	filterSQL  string // optional pushed-down row filter (parameterized)
	filterArgs []any
}

// WithFilter pushes a row filter (a rendered, parameterized WHERE fragment)
// into every chunk SELECT, so the snapshot only carries rows inside the
// filter. Composed with the chunk bounds by AND.
func (c *Chunker) WithFilter(where string, args []any) *Chunker {
	c.filterSQL = where
	c.filterArgs = args
	return c
}

// NewChunker builds a chunker for one source table.
func NewChunker(db *sql.DB, source, pk string, chunkSize int) (*Chunker, error) {
	schema, table, ok := strings.Cut(source, ".")
	if !ok {
		return nil, fmt.Errorf("mysql: chunker: source %q must be db.table", source)
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("mysql: chunker: chunkSize must be positive")
	}
	pks := strings.Split(pk, ",")
	return &Chunker{
		db:        db,
		schema:    schema,
		table:     table,
		pk:        pks,
		chunkSize: chunkSize,
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

// Chunks materializes the half-open ranges from the bounds. Each chunk is
// [bounds[i], bounds[i+1]); the last is [bounds[n-1], nil) (open high).
func Chunks(bounds [][]any) []Chunk {
	if len(bounds) == 0 {
		return nil
	}
	out := make([]Chunk, 0, len(bounds))
	for i := 0; i < len(bounds)-1; i++ {
		out = append(out, Chunk{Low: bounds[i], High: bounds[i+1]})
	}
	out = append(out, Chunk{Low: bounds[len(bounds)-1], High: nil})
	return out
}

// Scan executes the chunk SELECT (chunk bounds AND the pushed-down row
// filter, if any) and calls fn for every row, keyed by column name.
func (c *Chunker) Scan(ctx context.Context, ch Chunk, fn func(row map[string]any) error) error {
	where, args := chunkWhere(ch, c.pk, c.filterSQL, c.filterArgs)

	// With no row filter the SELECT must still read the whole row; select *
	// keeps it simple and correct for the spike.
	cols := "`" + strings.Join(c.pk, "`, `") + "`"
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`%s ORDER BY %s",
		c.schema, c.table, where, cols)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("mysql: chunk scan: %w", err)
	}
	defer func() { _ = rows.Close() }()

	colsMeta, err := rows.Columns()
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
			m[name] = normalize(vals[i])
		}
		if err := fn(m); err != nil {
			return err
		}
	}
	return rows.Err()
}

// chunkWhere composes the chunk bounds (half-open PK range; row-constructor
// comparison keeps composite PKs lexicographic) with an optional pushed-down
// filter fragment. Bound args come first, filter args last.
func chunkWhere(ch Chunk, pk []string, filterSQL string, filterArgs []any) (string, []any) {
	cond := make([]string, 0, 2)
	cols := "`" + strings.Join(pk, "`, `") + "`"
	args := make([]any, 0, 2*len(pk)+len(filterArgs))

	if ch.Low != nil {
		cond = append(cond, fmt.Sprintf("(%s) >= (%s)", cols, placeholders(len(pk))))
		args = append(args, ch.Low...)
	}
	if ch.High != nil {
		cond = append(cond, fmt.Sprintf("(%s) < (%s)", cols, placeholders(len(pk))))
		args = append(args, ch.High...)
	}
	if filterSQL != "" {
		cond = append(cond, filterSQL)
		args = append(args, filterArgs...)
	}
	if len(cond) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(cond, " AND "), args
}

func scanRow(rows *sql.Rows) ([]any, error) {
	cols, err := rows.Columns()
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
	return vals, nil
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}
