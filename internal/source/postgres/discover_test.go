package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

// captureQueryDriver is a database/sql driver that records the last query
// text and returns no rows.
type captureQueryDriver struct{}

var capturedQuery string

func (captureQueryDriver) Open(string) (driver.Conn, error) { return captureQueryConn{}, nil }

type captureQueryConn struct{}

func (captureQueryConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (captureQueryConn) Close() error                        { return nil }
func (captureQueryConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (captureQueryConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	capturedQuery = query
	return noRows{}, nil
}

// CheckNamedValue accepts any argument as-is: database/sql's default
// converter rejects []string, which the real pgx stdlib encodes as text[]
// (used by the discovery schema filter).
func (captureQueryConn) CheckNamedValue(*driver.NamedValue) error { return nil }

type noRows struct{}

func (noRows) Columns() []string         { return []string{"id"} }
func (noRows) Close() error              { return nil }
func (noRows) Next([]driver.Value) error { return io.EOF }

func init() { sql.Register("urutau_capture_query", captureQueryDriver{}) }

// TestDiscoverSQL locks the discovery query's shape without a real database
// (captureQueryDriver records the last query text). The three clauses that
// are easy to "simplify" and wrong:
//
//   - relkind IN ('r','p'): matview/foreign tables fail EnsureSetup's
//     REPLICA IDENTITY FULL and CREATE PUBLICATION;
//   - NOT relispartition: a leaf partition has relkind 'r', so without it the
//     parent AND its leaves are discovered and every row replicates twice;
//   - pg\_%: the underscore must be escaped or a user schema like pgx_meta is
//     silently excluded.
func TestDiscoverSQL(t *testing.T) {
	db, err := sql.Open("urutau_capture_query", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	a := Source{db: db}
	if _, err := a.Discover(context.Background(), nil); err != nil {
		t.Fatalf("Discover: %v", err)
	}
	for _, want := range []string{
		"c.relkind IN ('r','p')",
		"NOT c.relispartition",
		`n.nspname NOT LIKE 'pg\_%'`,
		"n.nspname <> 'information_schema'",
		"has_table_privilege(c.oid, 'SELECT')",
		"has_schema_privilege(current_user, n.nspname, 'USAGE')",
	} {
		if !strings.Contains(capturedQuery, want) {
			t.Fatalf("discover SQL missing %q:\n%s", want, capturedQuery)
		}
	}
	if strings.Contains(capturedQuery, "ANY($1)") {
		t.Fatalf("no schemas must not add the ANY filter:\n%s", capturedQuery)
	}

	// A schema filter adds the ANY clause.
	if _, err := a.Discover(context.Background(), []string{"public"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(capturedQuery, "n.nspname = ANY($1)") {
		t.Fatalf("a schema filter must add ANY($1):\n%s", capturedQuery)
	}
}
