package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/spec"
)

// TestPostgresPipeline drives the second source end to end: Postgres
// logical decoding (pgoutput) → DBLog snapshot → worker → Iceberg, read
// back through Trino. Same three cases that define the MySQL milestone:
// snapshot under no concurrent load yet, live INSERT/UPDATE/DELETE, and
// resume from the LSN committed on the table property.
func TestPostgresPipeline(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s := loadPostgresPipeline(t)
	db := pgConn(t)

	// Fresh source state: drop rows and the replication slot, so the run
	// starts from a clean consistency point with no stale WAL replay.
	pgExec(t, db, `TRUNCATE orders`)
	dropIcebergTable(t, ctx)
	dropSlot(t, db, "urutau_e2e")
	seedPostgresOrders(t, db, 0, 50)

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(runCtx, s, testConfig())
	}()
	// Fails fast with the runner's cause when the stream dies mid-test,
	// instead of swallowing it until stop().
	checkRun := func() {
		t.Helper()
		select {
		case err := <-runErr:
			t.Fatalf("runner exited early: %v", err)
		default:
		}
	}
	defer stop()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	checkRun()

	// Live stream: upsert by PK, first-class delete.
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (101, 'live1', 1.5, true)`)
	pgExec(t, db, `UPDATE orders SET v = 'upd' WHERE id = 1`)
	pgExec(t, db, `DELETE FROM orders WHERE id = 2`)
	pgExec(t, db, `UPDATE orders SET active = false WHERE id = 3`)
	time.Sleep(3 * time.Second)
	checkRun()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50)) // 50 - 1 (del) + 1 (live)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 1`, "upd")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 101`, "live1")
	waitTrino(t, ctx, `SELECT active FROM orders WHERE id = 3`, false)
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 2`)

	t.Log("stream ok: upsert, delete, insert, boolean update replicated via pgoutput")

	// ── Resume: stop the runner, DML during downtime, restart ──
	stop()
	if err := <-runErr; err != nil && err != context.Canceled {
		t.Fatalf("first run: %v", err)
	}

	pgExec(t, db, `UPDATE orders SET v = 'after-down' WHERE id = 1`)
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (200, 'resumed', 9.0, true)`)

	runCtx2, stop2 := context.WithCancel(ctx)
	runErr2 := make(chan error, 1)
	go func() {
		runErr2 <- runner.Run(runCtx2, s, testConfig())
	}()
	defer stop2()

	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 1`, "after-down")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 200`, "resumed")
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(51)) // 50 + id 200
	// No duplicate rows: under upsert, resume idempotently re-applies.
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 1`, int64(1))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 200`, int64(1))

	t.Log("resume ok: no loss, no duplicate after downtime DML (LSN position)")
}

// loadPostgresPipeline loads the same spec examples/postgres-iceberg.yaml
// documents, from disk — overriding connection strings when the CI stack
// differs from the file's local-compose defaults.
func loadPostgresPipeline(t *testing.T) *spec.Spec {
	t.Helper()
	return loadExampleSpec(t, "postgres-iceberg.yaml", func(s *spec.Spec) {
		s.Source.URI = env("URUTAU_E2E_PG_URI", s.Source.URI)
		s.Sink.URI = env("URUTAU_E2E_CATALOG", s.Sink.URI)
		s.Sink.Warehouse = env("URUTAU_E2E_WAREHOUSE", s.Sink.Warehouse)
	})
}

func pgConn(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", env("URUTAU_E2E_PG_URI", "postgres://repl:replpass@127.0.0.1:5433/shop?sslmode=disable"))
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

func pgExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("postgres exec %q: %v", q, err)
	}
}

func dropSlot(t *testing.T, db *sql.DB, slot string) {
	t.Helper()
	pgExec(t, db, fmt.Sprintf(
		`SELECT pg_catalog.pg_drop_replication_slot(%[1]s) WHERE EXISTS
		 (SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %[1]s)`,
		"'"+slot+"'"))
}

func seedPostgresOrders(t *testing.T, db *sql.DB, from, count int) {
	t.Helper()
	for i := from; i < from+count; i++ {
		pgExec(t, db, fmt.Sprintf(
			`INSERT INTO orders (id, v, amount, active) VALUES (%d, 'seed-%d', %d.25, %t)`,
			i, i, i%7, i%2 == 0))
	}
}
