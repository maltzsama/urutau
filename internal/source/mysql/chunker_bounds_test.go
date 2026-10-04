package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
)

// boundsCaptureDriver scripts Bounds: the MIN probe returns one row, the first
// seek probe returns one row, and the second seek returns none (ends the
// chunk). The query/args are recorded so the seek's placeholder order — key
// arg before the LIMIT offset — is asserted (a wrong order bound the offset to
// the WHERE and the key to the LIMIT, and the small e2e tables masked it).
type boundsCaptureDriver struct{}

var (
	boundsQueries  []string
	boundsArgsList [][]driver.NamedValue
)

func (boundsCaptureDriver) Open(string) (driver.Conn, error) { return boundsCaptureConn{}, nil }

type boundsCaptureConn struct{}

func (boundsCaptureConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (boundsCaptureConn) Close() error                        { return nil }
func (boundsCaptureConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (boundsCaptureConn) QueryContext(_ context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	boundsQueries = append(boundsQueries, q)
	boundsArgsList = append(boundsArgsList, args)
	switch len(boundsQueries) {
	case 1: // MIN(pk)
		return &fakeRows{cols: []string{"id"}, data: [][]driver.Value{{int64(1)}}}, nil
	case 2: // chunkSize-th key after 1, with chunkSize=3 → 4
		return &fakeRows{cols: []string{"id"}, data: [][]driver.Value{{int64(4)}}}, nil
	default:
		return &fakeRows{cols: []string{"id"}}, nil
	}
}

func init() { sql.Register("urutau_mysql_capture_bounds", boundsCaptureDriver{}) }

func TestChunkerBoundsSeek(t *testing.T) {
	boundsQueries, boundsArgsList = nil, nil
	db, err := sql.Open("urutau_mysql_capture_bounds", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	c, err := NewChunker(db, "shop.orders", "id", 3, nil, nil, filterSQL{})
	if err != nil {
		t.Fatal(err)
	}
	bounds, err := c.Bounds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(bounds) != 2 || bounds[0][0] != int64(1) || bounds[1][0] != int64(4) {
		t.Fatalf("bounds = %v, want [[1] [4]]", bounds)
	}
	if len(boundsQueries) != 3 {
		t.Fatalf("queries = %d, want 3 (min, seek, end)", len(boundsQueries))
	}
	seek := boundsQueries[1]
	if !strings.Contains(seek, "(`id` > ?)") || !strings.Contains(seek, "LIMIT ?, 1") {
		t.Fatalf("seek query shape:\n%s", seek)
	}
	if len(boundsArgsList[1]) != 2 || boundsArgsList[1][0].Value != int64(1) || boundsArgsList[1][1].Value != int64(2) {
		t.Fatalf("seek args = %v, want [1 2] (key arg before the LIMIT offset)", boundsArgsList[1])
	}
}
