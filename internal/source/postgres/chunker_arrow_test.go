package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/source"
)

// mixedRowsDriver serves a chunk of mixed-typed rows, as pgx returns them:
// text/JSON as a byte slice, integers/floats/bools as Go scalars, timestamps
// as time.Time, NULL as nil.
type mixedRowsDriver struct{}

func (mixedRowsDriver) Open(string) (driver.Conn, error) { return mixedRowsConn{}, nil }

type mixedRowsConn struct{}

func (mixedRowsConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (mixedRowsConn) Close() error                        { return nil }
func (mixedRowsConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (mixedRowsConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return captureTx{}, nil
}
func (mixedRowsConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &mixedRows{}, nil
}

type mixedRows struct{ i int }

func (r *mixedRows) Columns() []string {
	return []string{"id", "name", "doc", "score", "ok", "at", "note"}
}
func (r *mixedRows) Close() error { return nil }

// The source type names, so ScanArrow picks the raw-byte path only for the
// text/JSON columns (Sourcery finding on #590).
func (r *mixedRows) ColumnTypeDatabaseTypeName(i int) string {
	return [...]string{"INT8", "TEXT", "JSONB", "FLOAT8", "BOOL", "TIMESTAMPTZ", "TEXT"}[i]
}
func (r *mixedRows) Next(dest []driver.Value) error {
	if r.i >= 3 {
		return io.EOF
	}
	at := time.Date(2026, 9, 27, 10, 11, 12, 0, time.UTC)
	rows := [][]driver.Value{
		{int64(1), []byte("ana"), []byte(`{"a":1}`), float64(1.5), true, at, []byte("n1")},
		{int64(2), nil, nil, float64(-0.5), nil, nil, nil},
		{int64(3), []byte("ção"), []byte(`[]`), float64(0), false, at.Add(time.Hour), []byte("n3")},
	}
	copy(dest, rows[r.i])
	r.i++
	return nil
}

func init() { sql.Register("urutau_pg_mixed", mixedRowsDriver{}) }

// A Chunker satisfies the worker's direct-to-Arrow chunk scanner (#590).
var _ interface {
	ScanArrow(context.Context, source.Chunk, *transport.RowEncoder, int) (int, error)
} = (*Chunker)(nil)

// The direct path must land exactly what Scan's rows land through
// RecordFromChanges: a snapshot row that differs from its CDC decode would be
// a different value in Iceberg.
func TestChunkerScanArrowMatchesTheRowPath(t *testing.T) {
	db, err := sql.Open("urutau_pg_mixed", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	c, err := NewChunker(context.Background(), db, "public.mixed", "id", 10, WithWorkers(1), WithRetries(0))
	if err != nil {
		t.Fatal(err)
	}
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "doc", Type: core.ColumnType{Kind: core.KindJSON, Nullable: true}},
			{Name: "score", Type: core.ColumnType{Kind: core.KindFloat64, Nullable: true}},
			{Name: "ok", Type: core.ColumnType{Kind: core.KindBool, Nullable: true}},
			{Name: "at", Type: core.ColumnType{Kind: core.KindTimestamp, Nullable: true}},
			{Name: "note", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "absent", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}

	// The row path: Scan, then RecordFromChanges, as the worker did.
	var changes []rowchange.Change
	if err := c.Scan(context.Background(), source.Chunk{}, func(row map[string]any) error {
		changes = append(changes, rowchange.Change{Op: rowchange.OpInsert, Table: "raw.mixed",
			Key: []any{row["id"]}, After: row, Snapshot: true, Phase: core.PhaseSnapshot})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer want.Release()

	enc, err := transport.NewRowEncoder(cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	if _, err := c.ScanArrow(context.Background(), source.Chunk{}, enc, 3); err != nil {
		t.Fatal(err)
	}
	got := enc.NewRecord()
	defer got.Release()

	if got.NumRows() != want.NumRows() || !got.Schema().Equal(want.Schema()) {
		t.Fatalf("got %d rows %v, want %d rows %v", got.NumRows(), got.Schema(), want.NumRows(), want.Schema())
	}
	for i := range int(want.NumCols()) {
		if want.ColumnName(i) == "__ingest_ts" {
			continue // set to the read's own instant
		}
		if !array.Equal(got.Column(i), want.Column(i)) {
			t.Errorf("column %s: got %v, want %v", want.ColumnName(i), got.Column(i), want.Column(i))
		}
	}
}

// flakyDriver fails the first QueryContext with a transient error, then serves
// rows, so ScanArrow's setup retry can be exercised (Sourcery finding #590).
type flakyDriver struct{}

var flakyQueries atomic.Int64

func (flakyDriver) Open(string) (driver.Conn, error) { return flakyConn{}, nil }

type flakyConn struct{}

func (flakyConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (flakyConn) Close() error                        { return nil }
func (flakyConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (flakyConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return captureTx{}, nil
}
func (flakyConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if flakyQueries.Add(1) == 1 {
		return nil, fmt.Errorf("query: %w", context.DeadlineExceeded)
	}
	return &mixedRows{}, nil
}

func init() { sql.Register("urutau_pg_flaky", flakyDriver{}) }

// A transient failure while opening the chunk query is retried before any row
// is appended (Sourcery finding on #590).
func TestChunkerScanArrowRetriesTheSetup(t *testing.T) {
	flakyQueries.Store(0)
	db, err := sql.Open("urutau_pg_flaky", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	c, err := NewChunker(context.Background(), db, "public.mixed", "id", 10, WithWorkers(1), WithRetries(1))
	if err != nil {
		t.Fatal(err)
	}
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "doc", Type: core.ColumnType{Kind: core.KindJSON, Nullable: true}},
			{Name: "score", Type: core.ColumnType{Kind: core.KindFloat64, Nullable: true}},
			{Name: "ok", Type: core.ColumnType{Kind: core.KindBool, Nullable: true}},
			{Name: "at", Type: core.ColumnType{Kind: core.KindTimestamp, Nullable: true}},
			{Name: "note", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
	enc, err := transport.NewRowEncoder(cs, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	n, err := c.ScanArrow(context.Background(), source.Chunk{}, enc, 3)
	if err != nil {
		t.Fatalf("ScanArrow: %v", err)
	}
	if n != 3 {
		t.Fatalf("rows = %d, want 3 after a retry", n)
	}
	if got := flakyQueries.Load(); got < 2 {
		t.Fatalf("QueryContext calls = %d, want the setup retried at least once", got)
	}
}
