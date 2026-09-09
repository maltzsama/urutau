package enrich

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	// The reference read is a plain SQL query against a second database —
	// the same engines as the sources. Both are wired through
	// driver.Connector (never a re-serialized DSN string).
	"github.com/go-sql-driver/mysql"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
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
	var connector driver.Connector
	switch {
	case strings.HasPrefix(uri, "mysql://"):
		cfg, err := mysqlConfig(uri)
		if err != nil {
			return nil, fmt.Errorf("enrich: reference uri: %w", err)
		}
		c, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, fmt.Errorf("enrich: mysql connector: %w", err)
		}
		connector = c
	case strings.HasPrefix(uri, "postgres://") || strings.HasPrefix(uri, "postgresql://"):
		dsn, err := pgx.ParseConfig(uri)
		if err != nil {
			return nil, fmt.Errorf("enrich: reference uri: %w", err)
		}
		connector = stdlib.GetConnector(*dsn)
	default:
		return nil, fmt.Errorf("enrich: reference uri %q: unsupported scheme (mysql:// | postgres://)", uri)
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1) // one sequential re-read per refresh; no pool theater
	db.SetConnMaxLifetime(5 * time.Minute)
	return &sqlLoader{db: db, query: query}, nil
}

// mysqlConfig parses a mysql:// URI into the driver config — WITHOUT
// ever re-serializing the credentials as a DSN string (RV-06): a password
// decoded from the URI may contain ':', '@', '/' or '?' — any of them
// corrupts a DSN the driver parses positionally. mysql.NewConfig defaults
// apply (Loc: UTC, AllowNativePasswords, CheckConnLiveness); query params
// from the URI carry through as driver system variables (RV-06b).
func mysqlConfig(raw string) (*mysql.Config, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "mysql" {
		return nil, fmt.Errorf("mysql: uri scheme %q, want mysql", u.Scheme)
	}
	user := u.User.Username()
	pass, _ := u.User.Password()
	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("mysql: uri %q lacks host", raw)
	}
	port := u.Port()
	if port == "" {
		port = "3306"
	}
	db := strings.TrimPrefix(u.Path, "/")
	if db == "" {
		return nil, fmt.Errorf("mysql: uri %q lacks /db", raw)
	}

	cfg := mysql.NewConfig()
	cfg.User = user
	cfg.Passwd = pass
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(host, port)
	cfg.DBName = db
	cfg.ParseTime = true // DATETIME decodes as time.Time, matching goTypeToCore
	// URI query params survive (RV-06b): ?timeout=10s must not vanish.
	if q := u.Query(); len(q) > 0 {
		cfg.Params = make(map[string]string, len(q))
		for k, vs := range q {
			if len(vs) > 0 {
				cfg.Params[k] = vs[0]
			}
		}
	}
	return cfg, nil
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
