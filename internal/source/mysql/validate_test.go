package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/maltzsama/urutau/position"
)

// fakePreflightDriver answers the preflight and gtid_purged queries with a
// canned single row, reusing the fakeRows type from querytable_test.go. It
// lets ValidateServer and checkNotPurged be exercised without a MySQL server.
type fakePreflightDriver struct{}

var (
	fakePreflightCols []string
	fakePreflightData [][]driver.Value
)

func (fakePreflightDriver) Open(string) (driver.Conn, error) { return fakePreflightConn{}, nil }

type fakePreflightConn struct{}

func (fakePreflightConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (fakePreflightConn) Close() error                        { return nil }
func (fakePreflightConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }
func (fakePreflightConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &fakeRows{cols: fakePreflightCols, data: fakePreflightData}, nil
}

func init() { sql.Register("urutau_mysql_fake_preflight", fakePreflightDriver{}) }

func fakeDB(t *testing.T, cols []string, row []driver.Value) *sql.DB {
	t.Helper()
	fakePreflightCols, fakePreflightData = cols, [][]driver.Value{row}
	db, err := sql.Open("urutau_mysql_fake_preflight", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestValidateServer(t *testing.T) {
	cols := []string{"log_bin", "binlog_format", "gtid_mode", "enforce_gtid_consistency", "binlog_row_image"}
	cases := []struct {
		name    string
		row     []driver.Value
		wantErr string
		wantWrn bool
	}{
		{"ok", []driver.Value{"1", "ROW", "ON", "ON", "FULL"}, "", false},
		{"binlog off", []driver.Value{"0", "ROW", "ON", "ON", "FULL"}, "log_bin", false},
		{"statement format", []driver.Value{"1", "STATEMENT", "ON", "ON", "FULL"}, "binlog_format", false},
		{"gtid off", []driver.Value{"1", "ROW", "OFF", "ON", "FULL"}, "gtid_mode", false},
		{"enforce off", []driver.Value{"1", "ROW", "ON", "OFF", "FULL"}, "enforce_gtid_consistency", false},
		{"row image minimal warns", []driver.Value{"1", "ROW", "ON", "ON", "MINIMAL"}, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := fakeDB(t, cols, c.row)
			warn, err := ValidateServer(context.Background(), db)
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("want error containing %q, got nil", c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if (warn != "") != c.wantWrn {
				t.Fatalf("warning = %q, want non-empty=%v", warn, c.wantWrn)
			}
		})
	}
}

func TestCheckNotPurged(t *testing.T) {
	db := fakeDB(t, []string{"gtid_purged"}, []driver.Value{"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10"})
	s := stream{db: db}
	// A resume that contains the purged set: no gap.
	if err := s.checkNotPurged(context.Background(), mustGTID(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-20")); err != nil {
		t.Fatalf("resume ahead of purged must pass: %v", err)
	}
	// A resume behind the purged set: gap -> error.
	if err := s.checkNotPurged(context.Background(), mustGTID(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")); err == nil {
		t.Fatal("resume behind purged must error")
	}
	// An empty resume (first boot) skips the check entirely.
	if err := s.checkNotPurged(context.Background(), mustGTID(t, "")); err != nil {
		t.Fatalf("empty resume must skip the check: %v", err)
	}
}

func TestResolveMaxReconnectAttempts(t *testing.T) {
	if got := resolveMaxReconnectAttempts(0); got != defaultMaxReconnectAttempts {
		t.Fatalf("resolveMaxReconnectAttempts(0) = %d, want %d", got, defaultMaxReconnectAttempts)
	}
	if got := resolveMaxReconnectAttempts(7); got != 7 {
		t.Fatalf("resolveMaxReconnectAttempts(7) = %d, want 7", got)
	}
	if got := resolveMaxReconnectAttempts(-1); got != 0 {
		t.Fatalf("resolveMaxReconnectAttempts(-1) = %d, want 0", got)
	}
}

func TestCanalConfigMaxReconnect(t *testing.T) {
	c := canalConfig(Config{MaxReconnectAttempts: 5}, nil)
	if c.MaxReconnectAttempts != 5 {
		t.Fatalf("MaxReconnectAttempts = %d, want 5", c.MaxReconnectAttempts)
	}
}

func mustGTID(t *testing.T, s string) *position.GTID {
	t.Helper()
	g, err := position.ParseGTID(s)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
