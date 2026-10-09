package mysql

import (
	"log/slog"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/maltzsama/urutau/source"
)

func TestDestructiveDDL(t *testing.T) {
	tests := []struct {
		stmt  string
		kind  string
		table string
		ok    bool
	}{
		{"TRUNCATE TABLE `shop`.`orders`", "truncate", "shop.orders", true},
		{"truncate orders", "truncate", "orders", true},
		{"DROP TABLE IF EXISTS orders", "ddl", "orders", true},
		{"ALTER TABLE orders ADD COLUMN x INT", "ddl", "orders", true},
		{"RENAME TABLE a TO b", "ddl", "a", true},
		{"CREATE INDEX idx ON orders (id)", "", "", false},
		{"INSERT INTO orders VALUES (1)", "", "", false},
		{"", "", "", false},
	}
	for _, tt := range tests {
		kind, table, ok := destructiveDDL(tt.stmt)
		if ok != tt.ok || kind != tt.kind || table != tt.table {
			t.Errorf("destructiveDDL(%q) = (%q,%q,%v), want (%q,%q,%v)",
				tt.stmt, kind, table, ok, tt.kind, tt.table, tt.ok)
		}
	}
}

func TestOnDDLReportsDestructiveAndIgnoresBenign(t *testing.T) {
	var got []source.DestructiveDDL
	r := &Reader{cfg: Config{
		Logger:           slog.New(slog.DiscardHandler),
		OnTruncate:       "ignore",
		OnDestructiveDDL: func(d source.DestructiveDDL) { got = append(got, d) },
	}}
	query := func(q string) *replication.QueryEvent { return &replication.QueryEvent{Query: []byte(q)} }

	if err := r.OnDDL(nil, gomysql.Position{}, query("TRUNCATE TABLE orders")); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}
	if len(got) != 1 || got[0].Source != "mysql" || got[0].Kind != "truncate" || got[0].Table != "orders" {
		t.Fatalf("reports = %+v", got)
	}
	// Benign DDL is not reported.
	if err := r.OnDDL(nil, gomysql.Position{}, query("CREATE TABLE t (id int)")); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("benign DDL was reported: %+v", got)
	}
}

func TestOnDDLFailsUnderFailPolicy(t *testing.T) {
	r := &Reader{cfg: Config{Logger: slog.New(slog.DiscardHandler), OnTruncate: "fail"}}
	err := r.OnDDL(nil, gomysql.Position{}, &replication.QueryEvent{Query: []byte("DROP TABLE orders")})
	if err == nil {
		t.Fatal("expected failure for destructive DDL under onTruncate: fail")
	}
}
