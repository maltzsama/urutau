package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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
	if !strings.Contains(capturedChunkQuery, "(`id`) >= (?)") || !strings.Contains(capturedChunkQuery, "(`status` = ?)") {
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
