package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
)

// errRowsDriver is a minimal database/sql driver whose query yields a Rows
// whose Next fails, so scanRow can be exercised without a real database.
type errRowsDriver struct{}

func (errRowsDriver) Open(string) (driver.Conn, error) { return errRowsConn{}, nil }

type errRowsConn struct{}

func (errRowsConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (errRowsConn) Close() error                        { return nil }
func (errRowsConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (errRowsConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return errRows{}, nil
}

type errRows struct{}

func (errRows) Columns() []string         { return []string{"id"} }
func (errRows) Close() error              { return nil }
func (errRows) Next([]driver.Value) error { return errors.New("transient boom") }

func init() { sql.Register("urutau_err_rows", errRowsDriver{}) }

// A failed iteration must surface the real error, not be mistaken for an
// empty result (which would truncate Bounds).
func TestScanRowPropagatesIterationError(t *testing.T) {
	db, err := sql.Open("urutau_err_rows", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query("SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	_, err = scanRow(rows)
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("scanRow = %v, want the iteration error, not sql.ErrNoRows", err)
	}
}

// normalize folds driver-native []byte cells into strings so bounds and
// rows share one scalar mapping.
func TestNormalize(t *testing.T) {
	if got := normalize([]byte("abc")); got != "abc" {
		t.Fatalf("normalize([]byte) = %v, want string", got)
	}
	if got := normalize(int64(5)); got != int64(5) {
		t.Fatalf("normalize(int64) changed the value")
	}
}

func TestQuotedList(t *testing.T) {
	got := quotedList([]string{"id", "v"})
	want := `"id", "v"`
	if got != want {
		t.Fatalf("quotedList = %q, want %q", got, want)
	}
	// Embedded quotes are doubled.
	got = quotedList([]string{`we"ird`})
	if got != `"we""ird"` {
		t.Fatalf("quotedList with quote = %q", got)
	}
}

func TestQuoteIdent(t *testing.T) {
	if quoteIdent("orders") != `"orders"` {
		t.Fatalf("quoteIdent = %q", quoteIdent("orders"))
	}
}

func TestPlaceholders(t *testing.T) {
	if got := placeholders(1, 2); got != "$1, $2" {
		t.Fatalf("placeholders(1,2) = %q", got)
	}
	if got := placeholders(3, 1); got != "$3" {
		t.Fatalf("placeholders(3,1) = %q", got)
	}
}
