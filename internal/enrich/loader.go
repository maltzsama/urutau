package enrich

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	// The reference read is a plain SQL query against a second database —
	// the same engines as the sources, registered for database/sql.
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Loader reads one reference image. Load returns every row as a column →
// value map with the driver's natural Go types (int64, float64, string,
// []byte, time.Time) — the same canonical family the CDC sources decode
// into, which is what lets the join key match without coercion theater.
type Loader interface {
	Load(ctx context.Context) ([]map[string]any, error)
	Close() error
}

// NewSQLLoader opens the reference connection. The URI scheme picks the
// driver (mysql:// or postgres://); the query returns the full reference
// image and is re-run on every refresh.
func NewSQLLoader(uri, query string) (Loader, error) {
	var driver string
	switch {
	case strings.HasPrefix(uri, "mysql://"):
		driver = "mysql"
	case strings.HasPrefix(uri, "postgres://"), strings.HasPrefix(uri, "postgresql://"):
		driver = "pgx"
	default:
		return nil, fmt.Errorf("enrich: reference uri %q: unsupported scheme (mysql:// | postgres://)", uri)
	}
	db, err := sql.Open(driver, uri)
	if err != nil {
		return nil, fmt.Errorf("enrich: open reference: %w", err)
	}
	db.SetMaxOpenConns(1) // one sequential re-read per refresh; no pool theater
	return &sqlLoader{db: db, query: query}, nil
}

type sqlLoader struct {
	db    *sql.DB
	query string
}

func (l *sqlLoader) Load(ctx context.Context) ([]map[string]any, error) {
	rows, err := l.db.QueryContext(ctx, l.query)
	if err != nil {
		return nil, fmt.Errorf("enrich: reference query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("enrich: reference row: %w", err)
		}
		row := make(map[string]any, len(cols))
		for i, c := range cols {
			if v, ok := vals[i].([]byte); ok {
				row[c] = string(v) // TEXT/VARCHAR land as []byte; the join and the document want strings
				continue
			}
			row[c] = vals[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (l *sqlLoader) Close() error { return l.db.Close() }

// fakeLoader is the test seam (Load counter included — the O(N) proof is
// "the loader ran once per refresh, never per event"). Thread-safe: the
// concurrency test swaps rows while the refresher goroutine reads.
type fakeLoader struct {
	mu    sync.Mutex
	rows  []map[string]any
	err   error
	loads int
}

func (f *fakeLoader) SetRows(rows []map[string]any) {
	f.mu.Lock()
	f.rows = rows
	f.mu.Unlock()
}

func (f *fakeLoader) SetErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *fakeLoader) Load(context.Context) ([]map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	if f.err != nil {
		return nil, f.err
	}
	return f.rows, nil
}

func (f *fakeLoader) Close() error { return nil }
