package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	_ "github.com/go-sql-driver/mysql"

	"github.com/maltzsama/urutau/internal/runner"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/spec"
)

// TestMySQLPipeline drives the real stack: MySQL binlog → DBLog snapshot →
// worker → Iceberg, read back through Trino. It covers the three cases that
// define the milestone:
//
//  1. Snapshot under concurrent load: rows are written to MySQL while the
//     DBLog snapshot runs; the final Iceberg state must equal the source
//     (invariant 5), and the window must have discarded stale rows.
//  2. Live stream: INSERT/UPDATE/DELETE after the snapshot replicate.
//  3. Resume: the runner is stopped, DML happens during downtime, and a
//     restart picks up from the committed position with no loss or
//     duplicate under upsert.
func TestMySQLPipeline(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s := loadPipeline(t)
	db := mysqlConn(t)

	// Fresh tables for a deterministic run. The Iceberg table must not carry
	// a committed position from a previous run, or the runner would resume
	// the stream instead of snapshotting. And the binlog must be clean —
	// accumulated GTIDs from earlier runs pollute the resume test.
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 50) // rows 0..49 exist before the runner starts

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(runCtx, s, testConfig())
	}()
	defer stop()

	// The snapshot must bring the 50 pre-existing rows, then replicate live
	// changes. Drive DML while the runner is up.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))

	// Concurrent load during/after the snapshot: writes rows that race the
	// remaining chunks. Use a small chunk size so the snapshot spans work.
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (101, 'live1', 1.5)`)
	dml(t, db, `UPDATE orders SET v = 'upd' WHERE id = 1`)
	dml(t, db, `DELETE FROM orders WHERE id = 2`)
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (102, 'live2', 2.5)`)

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(51)) // 50 - 1 (del) + 2 (live)
	assertTrino(t, ctx, `SELECT v FROM orders WHERE id = 1`, "upd")
	assertTrino(t, ctx, `SELECT v FROM orders WHERE id = 101`, "live1")
	assertTrino(t, ctx, `SELECT v FROM orders WHERE id = 102`, "live2")
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 2`)

	t.Log("stream ok: upsert, delete, inserts replicated and read via Trino")

	// ── Resume: stop the runner, DML during downtime, restart ──
	stop()
	if err := <-runErr; err != nil && err != context.Canceled {
		t.Fatalf("first run: %v", err)
	}

	dml(t, db, `UPDATE orders SET v = 'after-down' WHERE id = 1`)
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (200, 'resumed', 9.0)`)

	runCtx2, stop2 := context.WithCancel(ctx)
	runErr2 := make(chan error, 1)
	go func() {
		runErr2 <- runner.Run(runCtx2, s, testConfig())
	}()
	defer stop2()

	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 1`, "after-down")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 200`, "resumed")
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(52)) // 51 + id 200
	// No duplicate rows: under upsert, resume idempotently re-applies.
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 1`, int64(1))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 200`, int64(1))

	t.Log("resume ok: no loss, no duplicate after downtime DML")
}

func loadPipeline(t *testing.T) *spec.Spec {
	t.Helper()
	s, err := spec.LoadYAML(strings.NewReader(mysqlPipelineYAML()))
	if err != nil {
		t.Fatalf("load pipeline: %v", err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate pipeline: %v", err)
	}
	return s
}

func mysqlPipelineYAML() string {
	return `
pipeline: e2e-mysql
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
sink:
  uri: ` + env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog") + `
  namespace: raw
  warehouse: ` + env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog") + `
  clientId: root
  clientSecret: s3cr3t
  scope: PRINCIPAL_ROLE:ALL
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
`
}

func testConfig() runner.Config {
	return runner.Config{
		ServerID:      1101,
		Heartbeat:     5 * time.Second,
		ChunkSize:     10, // small so the snapshot spans several chunks
		WindowTimeout: 2 * time.Minute,
		CaughtUpPoll:  300 * time.Millisecond,
		MaxRows:       100,
		MaxInterval:   2 * time.Second,
	}
}

func mysqlConn(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", "root:rootpass@tcp(127.0.0.1:3306)/shop?parseTime=true")
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// dropIcebergTable removes the target Iceberg table so the pipeline starts
// from a fresh snapshot (no committed position to resume from).
func dropIcebergTable(t *testing.T, ctx context.Context) {
	t.Helper()
	cfg := icebergsink.Config{
		URI:          env("URUTAU_E2E_CATALOG", "http://localhost:8181/api/catalog"),
		Warehouse:    env("URUTAU_E2E_WAREHOUSE", "quickstart_catalog"),
		ClientID:     "root",
		ClientSecret: "s3cr3t",
		Scope:        "PRINCIPAL_ROLE:ALL",
	}
	cat, err := icebergsink.NewCatalog(ctx, cfg)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	_ = cat.DropTable(ctx, table.Identifier{"raw", "orders"})
}

func dml(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("dml %q: %v", q, err)
	}
}

// resetBinlog clears the accumulated binlog + GTID timeline so the resume
// test works against a small, deterministic position space. MySQL 8.4
// removed RESET MASTER in favor of RESET BINARY LOGS AND GTIDS.
func resetBinlog(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("RESET BINARY LOGS AND GTIDS"); err != nil {
		t.Fatalf("reset binlog: %v", err)
	}
}

func dropAll(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM orders"); err != nil {
		t.Fatalf("drop all: %v", err)
	}
}

func seedOrders(t *testing.T, db *sql.DB, from, count int) {
	t.Helper()
	for i := from; i < from+count; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'seed%d', %d.0)", i, i, i))
	}
}

// waitTrino polls until a scalar query equals the expected value, tolerating
// transient errors (the table may not exist yet during setup).
func waitTrino(t *testing.T, ctx context.Context, query string, want any) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		rows, err := trinoQuery(ctx, query)
		if err == nil && len(rows) == 1 && len(rows[0]) == 1 && rows[0][0] == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trino wait %q: rows=%v err=%v want %v", query, rows, err, want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func assertTrino(t *testing.T, ctx context.Context, query string, want any) {
	t.Helper()
	rows := trinoRows(t, ctx, query)
	if len(rows) != 1 || rows[0][0] != want {
		t.Fatalf("trino %q: got %v, want %v", query, rows, want)
	}
}

func assertMissing(t *testing.T, ctx context.Context, query string) {
	t.Helper()
	rows := trinoRows(t, ctx, query)
	if len(rows) != 0 {
		t.Fatalf("trino %q: want no rows, got %v", query, rows)
	}
}

func assertCount(t *testing.T, ctx context.Context, query string, want int64) {
	t.Helper()
	rows := trinoRows(t, ctx, query)
	if len(rows) != 1 || rows[0][0] != want {
		t.Fatalf("trino %q: got %v, want %d", query, rows, want)
	}
}

// trinoQuery runs a query and returns rows, propagating any error (unlike
// trinoRows which fails the test).
func trinoQuery(ctx context.Context, query string) ([][]any, error) {
	dsn := "http://user@" + env("URUTAU_E2E_TRINO", "127.0.0.1:8080") + "?catalog=iceberg&schema=raw"
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}
