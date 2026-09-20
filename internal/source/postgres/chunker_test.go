package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
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

// captureTxDriver records the sql.TxOptions passed to BeginTx, so the
// isolation/read-only guarantee of the chunk scan can be asserted without a
// real database.
var (
	capturedTxOptions  sql.TxOptions
	capturedChunkQuery string
)

type captureTxDriver struct{}

func (captureTxDriver) Open(string) (driver.Conn, error) { return captureTxConn{}, nil }

type captureTxConn struct{}

func (captureTxConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (captureTxConn) Close() error                        { return nil }
func (captureTxConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (captureTxConn) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	capturedTxOptions = sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	}
	return captureTx{}, nil
}
func (captureTxConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	capturedChunkQuery = query
	return emptyRows{}, nil
}

// CheckNamedValue accepts any argument as-is: database/sql's default
// converter rejects []string, which the real pgx stdlib encodes as text[]
// (used by the discovery schema filter).
func (captureTxConn) CheckNamedValue(*driver.NamedValue) error { return nil }

type captureTx struct{}

func (captureTx) Commit() error   { return nil }
func (captureTx) Rollback() error { return nil }

type emptyRows struct{}

func (emptyRows) Columns() []string         { return []string{"id"} }
func (emptyRows) Close() error              { return nil }
func (emptyRows) Next([]driver.Value) error { return io.EOF }

func init() {
	sql.Register("urutau_err_rows", errRowsDriver{})
	sql.Register("urutau_capture_tx", captureTxDriver{})
}

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

// The chunk scan must run inside a REPEATABLE READ, READ ONLY transaction
// (#164).
func TestChunkScanUsesRepeatableReadReadOnly(t *testing.T) {
	db, err := sql.Open("urutau_capture_tx", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c, err := NewChunker(context.Background(), db, "public.orders", "id", 100, WithWorkers(1), WithRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	capturedTxOptions = sql.TxOptions{}
	if err := c.Scan(context.Background(), source.Chunk{}, func(map[string]any) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if capturedTxOptions.Isolation != sql.LevelRepeatableRead {
		t.Fatalf("isolation = %v, want RepeatableRead", capturedTxOptions.Isolation)
	}
	if !capturedTxOptions.ReadOnly {
		t.Fatal("transaction must be READ ONLY")
	}
}

// The chunk SELECT must list the projected columns and compose the filter
// with the chunk bounds. With no chunk column the default is CTID.
func TestChunkScanProjectionAndFilterSQL(t *testing.T) {
	db, err := sql.Open("urutau_capture_tx", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	filter, err := filterToSquirrel(&spec.Filter{
		Predicate: &spec.Predicate{Column: "status", Op: spec.OpEq, Value: "active"},
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewChunker(context.Background(), db, "public.orders", "id", 100,
		WithWorkers(1), WithRetries(0),
		WithColumns([]string{"id", "name"}),
		WithFilter(filter))
	if err != nil {
		t.Fatal(err)
	}
	capturedChunkQuery = ""
	err = c.Scan(context.Background(),
		source.Chunk{Low: []any{"(0,0)"}, High: []any{"(1000,0)"}},
		func(map[string]any) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	q := capturedChunkQuery
	for _, want := range []string{
		`SELECT "id", "name" FROM "public"."orders"`,
		`ctid >= $1::tid`,
		`ctid < $2::tid`,
		`"status" = $3`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("chunk query %q missing %q", q, want)
		}
	}
}

// A configured chunk column switches to a key-based strategy: the range
// predicate uses the column scalar, ordered by it (#151).
func TestChunkScanKeyStrategySQL(t *testing.T) {
	db, err := sql.Open("urutau_capture_tx", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// The capture driver has no pg_catalog, so resolveStrategy would fail
	// introspection; build the chunker and set the strategy directly.
	c, err := NewChunker(context.Background(), db, "public.orders", "id", 100,
		WithWorkers(1), WithRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	c.strategy = strategyBatch
	c.chunkColumn = "id"
	c.chunkColumnKind = kindInt

	capturedChunkQuery = ""
	err = c.Scan(context.Background(),
		source.Chunk{Low: []any{int64(1)}, High: []any{int64(10)}},
		func(map[string]any) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	q := capturedChunkQuery
	for _, want := range []string{
		`SELECT * FROM "public"."orders"`,
		`"id" >= $1`,
		`"id" < $2`,
		`ORDER BY "id"`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("chunk query %q missing %q", q, want)
		}
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
