package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/maltzsama/urutau/source"
)

// Chunker splits a table by its primary key, using the chunk-skipping
// bounds trick shared with the MySQL source: pick every chunkSize-th key
// with LIMIT 1 OFFSET n, then read the slice between consecutive bounds.
// Bounds are only seeds for the half-open range, so the split stays
// correct under concurrent inserts.
type Chunker struct {
	db        *sql.DB
	schema    string
	table     string
	pk        []string
	chunkSize int
}

// NewChunker builds a chunker for one source table.
func NewChunker(db *sql.DB, source, pk string, chunkSize int) (*Chunker, error) {
	schema, table, ok := strings.Cut(source, ".")
	if !ok {
		return nil, fmt.Errorf("postgres: chunker: source %q must be schema.table", source)
	}
	if chunkSize <= 0 {
		return nil, fmt.Errorf("postgres: chunker: chunkSize must be positive")
	}
	// A spec may write the key list as "a, b"; the raw split would leave a
	// leading space on the second column and break every keyed comparison.
	var pks []string
	for _, c := range strings.Split(pk, ",") {
		if c = strings.TrimSpace(c); c != "" {
			pks = append(pks, c)
		}
	}
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
	cols := quotedList(c.pk)
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s ORDER BY %s LIMIT 1 OFFSET $1",
		cols, quoteIdent(c.schema), quoteIdent(c.table), cols,
	)

	var bounds [][]any
	for offset := 0; ; offset += c.chunkSize {
		rows, err := c.db.QueryContext(ctx, query, offset)
		if err != nil {
			return nil, fmt.Errorf("postgres: chunker bounds: %w", err)
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

// Scan executes the chunk SELECT and calls fn for every row, keyed by
// column name. Values decode through the same scalar mapping the pgoutput
// reader uses, so snapshot rows and stream rows land in Iceberg identically.
func (c *Chunker) Scan(ctx context.Context, ch source.Chunk, fn func(row map[string]any) error) error {
	// Row-constructor comparison keeps composite PKs lexicographic; the
	// placeholder index runs across clauses ($1..$k, then $k+1..).
	cond := make([]string, 0, 2)
	args := make([]any, 0, 2*len(c.pk))
	cols := quotedList(c.pk)
	argIdx := 1

	if ch.Low != nil {
		cond = append(cond, fmt.Sprintf("(%s) >= (%s)", cols, placeholders(argIdx, len(c.pk))))
		args = append(args, ch.Low...)
		argIdx += len(c.pk)
	}
	if ch.High != nil {
		cond = append(cond, fmt.Sprintf("(%s) < (%s)", cols, placeholders(argIdx, len(c.pk))))
		args = append(args, ch.High...)
	}
	where := ""
	if len(cond) > 0 {
		where = " WHERE " + strings.Join(cond, " AND ")
	}

	// Without a row filter the SELECT reads the whole row; select * keeps
	// it simple and correct.
	query := fmt.Sprintf("SELECT * FROM %s.%s%s ORDER BY %s",
		quoteIdent(c.schema), quoteIdent(c.table), where, cols)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("postgres: chunk scan: %w", err)
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
			return fmt.Errorf("postgres: chunk scan row: %w", err)
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
	// Same scalar mapping as Scan, so bounds and snapshot rows agree: a
	// []byte cell here would otherwise flow raw into the persisted bounds
	// and be bound back into SQL as the wrong type.
	for i := range vals {
		vals[i] = normalize(vals[i])
	}
	return vals, nil
}

// placeholders renders $from..$from+n-1.
func placeholders(from, n int) string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("$%d", from+i)
	}
	return strings.Join(out, ", ")
}

// quoteIdent quotes one identifier, doubling embedded quotes.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quotedList(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = quoteIdent(strings.TrimSpace(c))
	}
	return strings.Join(out, ", ")
}

// normalize maps driver values into the scalar subset shared with the
// stream decoder.
func normalize(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	default:
		return v
	}
}
