package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/source"
)

// captureChunkDriver records the SQL and args the chunker issues, so the
// projection and filter composition can be asserted without a MySQL server.
type captureChunkDriver struct{}

var (
	capturedChunkQuery string
	capturedChunkArgs  []driver.NamedValue
)

func (captureChunkDriver) Open(string) (driver.Conn, error) { return captureChunkConn{}, nil }

type captureChunkConn struct{}

func (captureChunkConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (captureChunkConn) Close() error                        { return nil }
func (captureChunkConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (captureChunkConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	capturedChunkQuery = q
	capturedChunkArgs = args
	return &fakeRows{cols: []string{"id", "v"}}, nil
}

func init() { sql.Register("urutau_mysql_capture_chunk", captureChunkDriver{}) }

func TestChunkerProjectionAndFilter(t *testing.T) {
	capturedChunkQuery, capturedChunkArgs = "", nil
	db, err := sql.Open("urutau_mysql_capture_chunk", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c, err := NewChunker(db, "shop.orders", "id", 10, nil,
		[]string{"id", "v"}, filterSQL{where: "`status` = ?", args: []any{"active"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Scan(context.Background(), source.Chunk{Low: []any{int64(1)}}, func(map[string]any) error { return nil }); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(capturedChunkQuery, "SELECT `id`, `v` FROM") {
		t.Fatalf("projection missing from query:\n%s", capturedChunkQuery)
	}
	if !strings.Contains(capturedChunkQuery, "(`id` >= ?)") || !strings.Contains(capturedChunkQuery, "(`status` = ?)") {
		t.Fatalf("bounds + filter not composed:\n%s", capturedChunkQuery)
	}
	// Placeholder order: the bound arg, then the filter arg.
	if len(capturedChunkArgs) != 2 || capturedChunkArgs[0].Value != int64(1) || capturedChunkArgs[1].Value != "active" {
		t.Fatalf("args = %+v, want [1 active]", capturedChunkArgs)
	}
}

func TestChunkerSelectStarWithoutProjection(t *testing.T) {
	capturedChunkQuery, capturedChunkArgs = "", nil
	db, err := sql.Open("urutau_mysql_capture_chunk", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c, err := NewChunker(db, "shop.orders", "id", 10, nil, nil, filterSQL{})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Scan(context.Background(), source.Chunk{Low: []any{int64(1)}}, func(map[string]any) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(capturedChunkQuery, "SELECT * FROM") {
		t.Fatalf("no projection must select *:\n%s", capturedChunkQuery)
	}
	if strings.Contains(capturedChunkQuery, "WHERE") == false {
		t.Fatalf("a bound chunk must carry a WHERE:\n%s", capturedChunkQuery)
	}
}

// MySQL 8.4 does not use the primary key for a row-constructor range such as
// (`tenant_id`, `event_id`) >= (?, ?): EXPLAIN shows a full index scan (type
// index, every row of the table) for every chunk, so a composite-key table's
// snapshot is O(chunks × table) — the full profile's events table took 30-160
// s per 10,000-row chunk, the SELECT alone (#443). The bounds must be spelled
// as the lexicographic OR expansion, which EXPLAIN shows as a range scan of
// the chunk.
func TestChunkerCompositeBoundsAreSargable(t *testing.T) {
	capturedChunkQuery, capturedChunkArgs = "", nil
	db, err := sql.Open("urutau_mysql_capture_chunk", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c, err := NewChunker(db, "shop.events", "tenant_id, event_id", 10, nil, nil, filterSQL{})
	if err != nil {
		t.Fatal(err)
	}
	ch := source.Chunk{Low: []any{int64(5), int64(6661)}, High: []any{int64(6), int64(1013)}}
	if err := c.Scan(context.Background(), ch, func(map[string]any) error { return nil }); err != nil {
		t.Fatal(err)
	}
	want := " WHERE ((`tenant_id` > ?) OR (`tenant_id` = ? AND `event_id` >= ?))" +
		" AND ((`tenant_id` < ?) OR (`tenant_id` = ? AND `event_id` < ?)) ORDER BY"
	if !strings.Contains(capturedChunkQuery, want) {
		t.Fatalf("composite bounds are not the sargable OR expansion:\n got %s\nwant ...%s...", capturedChunkQuery, want)
	}
	var got []any
	for _, a := range capturedChunkArgs {
		got = append(got, a.Value)
	}
	wantArgs := []any{int64(5), int64(5), int64(6661), int64(6), int64(6), int64(1013)}
	if fmt.Sprint(got) != fmt.Sprint(wantArgs) {
		t.Fatalf("args = %v, want %v", got, wantArgs)
	}
}
