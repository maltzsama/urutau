package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/source"
)

// payloadDriver serves a chunk of n rows (id BIGINT, payload TEXT of size
// bytes) from one reused buffer, as the MySQL driver returns them: a row's
// bytes live in the connection's read buffer until the next row.
type payloadDriver struct{ n, size int }

var payloadDriverCfg atomic.Pointer[payloadDriver]

func (payloadDriver) Open(string) (driver.Conn, error) { return payloadConn{}, nil }

type payloadConn struct{}

func (payloadConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (payloadConn) Close() error                        { return nil }
func (payloadConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (payloadConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	d := payloadDriverCfg.Load()
	return &payloadRows{n: d.n, size: d.size}, nil
}

type payloadRows struct {
	n, size, i int
	buf        []byte
}

func (r *payloadRows) Columns() []string { return []string{"id", "payload"} }
func (r *payloadRows) Close() error      { return nil }
func (r *payloadRows) Next(dest []driver.Value) error {
	if r.i >= r.n {
		return io.EOF
	}
	if r.buf == nil {
		r.buf = make([]byte, r.size)
		for j := range r.buf {
			r.buf[j] = 'x'
		}
	}
	dest[0], dest[1] = int64(r.i), r.buf
	r.i++
	return nil
}
func (r *payloadRows) ColumnTypeDatabaseTypeName(i int) string {
	return [...]string{"BIGINT", "TEXT"}[i]
}

func init() { sql.Register("urutau_mysql_payload", payloadDriver{}) }

func liveHeap() uint64 {
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

var payloadSchema = core.Schema{
	Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "payload", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	},
	PrimaryKey: []string{"id"},
}

// scanPeak reads a chunk into Arrow and returns the peak live heap above the
// baseline, and the record's row count.
// perRow, when set, sizes the encoder as the previous chunk of the table
// would (the worker's case after a table's first chunk); it returns the
// chunk's own PerRow.
func scanPeak(t *testing.T, c *Chunker, rows int, perRow []int) (uint64, []int) {
	t.Helper()
	enc, err := transport.NewRowEncoder(payloadSchema, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Release()
	runtime.GC()
	base := liveHeap()
	var peak atomic.Uint64
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			if l := liveHeap(); l > peak.Load() {
				peak.Store(l)
			}
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()
	if perRow != nil {
		enc.ReservePerRow(rows, perRow)
	}
	n, err := c.ScanArrow(context.Background(), source.Chunk{Low: []any{int64(0)}}, enc, rows)
	per := enc.PerRow()
	rec := enc.NewRecord()
	close(stop)
	<-done
	defer rec.Release()
	if err != nil {
		t.Fatal(err)
	}
	if n != rows || rec.NumRows() != int64(rows) {
		t.Fatalf("read %d rows, record %d; want %d", n, rec.NumRows(), rows)
	}
	held := uint64(0)
	if p := peak.Load(); p > base {
		held = p - base
	}
	return held, per
}

// A snapshot chunk must go from the driver into Arrow holding the chunk about
// once. Reading it as row maps and then into Arrow held it three times over
// (3.1x), and the concatenated parts of #449 twice (2.0x) (#448). The bar:
// 1.1x the chunk's payload at peak.
func TestChunkerScanArrowHoldsTheChunkOnce(t *testing.T) {
	const rows, size = 4000, 6 << 10 // ~23 MiB of payload
	defer debug.SetGCPercent(debug.SetGCPercent(5))
	payloadDriverCfg.Store(&payloadDriver{n: rows, size: size})
	db, err := sql.Open("urutau_mysql_payload", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	c, err := NewChunker(db, "shop.events", "id", rows, nil, nil, filterSQL{})
	if err != nil {
		t.Fatal(err)
	}

	// The live-heap metric is process-wide: the smallest of a few reads is
	// the chunk's own. A table's first chunk is sized from its first rows;
	// every later one from the previous chunk.
	chunk := uint64(rows * size)
	var first, next uint64
	for i := 0; i < 3; i++ {
		h, per := scanPeak(t, c, rows, nil)
		if i == 0 || h < first {
			first = h
		}
		h, _ = scanPeak(t, c, rows, per)
		if i == 0 || h < next {
			next = h
		}
	}
	for _, m := range []struct {
		name string
		held uint64
	}{{"first chunk", first}, {"later chunk", next}} {
		t.Logf("%s: chunk %d MiB, peak live heap above baseline %d MiB (%.2fx)", m.name, chunk>>20, m.held>>20, float64(m.held)/float64(chunk))
		if m.held > chunk*11/10 {
			t.Errorf("%s: reading a %d MiB chunk held %d MiB live (%.2fx); want at most 1.1x", m.name, chunk>>20, m.held>>20, float64(m.held)/float64(chunk))
		}
	}
}

// mixedDriver serves rows of every kind the direct path handles differently:
// text and JSON (driver buffer), integers, unsigned, DECIMAL, DATETIME, NULL.
type mixedDriver struct{}

func (mixedDriver) Open(string) (driver.Conn, error) { return mixedConn{}, nil }

type mixedConn struct{}

func (mixedConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (mixedConn) Close() error                        { return nil }
func (mixedConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (mixedConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &mixedRows{}, nil
}

var mixedTypes = []string{"BIGINT", "VARCHAR", "JSON", "UNSIGNED BIGINT", "DECIMAL", "DATETIME", "MEDIUMTEXT"}

type mixedRows struct{ i int }

func (r *mixedRows) Columns() []string {
	return []string{"id", "name", "doc", "big", "amount", "at", "note"}
}
func (r *mixedRows) Close() error { return nil }
func (r *mixedRows) Next(dest []driver.Value) error {
	if r.i >= 3 {
		return io.EOF
	}
	at := time.Date(2026, 9, 27, 10, 11, 12, 0, time.UTC)
	rows := [][]driver.Value{
		{int64(1), []byte("ana"), []byte(`{"a":1}`), int64(7), []byte("12.34"), at, []byte("n1")},
		{int64(2), []byte(""), nil, []byte("18446744073709551615"), []byte("-0.50"), nil, nil},
		{int64(3), nil, []byte(`[]`), int64(0), nil, at.Add(time.Hour), []byte("ção")},
	}
	copy(dest, rows[r.i])
	r.i++
	return nil
}
func (r *mixedRows) ColumnTypeDatabaseTypeName(i int) string { return mixedTypes[i] }

func init() { sql.Register("urutau_mysql_mixed", mixedDriver{}) }

// The direct path must land exactly what Scan's rows land through
// RecordFromChanges: a snapshot row that differs from its CDC decode would
// be a different value in Iceberg.
func TestChunkerScanArrowMatchesTheRowPath(t *testing.T) {
	db, err := sql.Open("urutau_mysql_mixed", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	loc := time.FixedZone("BRT", -3*3600)
	c, err := NewChunker(db, "shop.mixed", "id", 10, loc, nil, filterSQL{})
	if err != nil {
		t.Fatal(err)
	}
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "doc", Type: core.ColumnType{Kind: core.KindJSON, Nullable: true}},
			{Name: "big", Type: core.ColumnType{Kind: core.KindUInt64, Nullable: true}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 10, Scale: 2, Nullable: true}},
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
