package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/spec"
)

// structuredPostgresSpec loads the postgres example and swaps the flat URI
// for a structured source.postgres block, so the TLS (#149) and SSH (#148)
// paths are exercised through the same pipeline.
func structuredPostgresSpec(t *testing.T, pg *spec.PostgresSource, slot string) *spec.Spec {
	t.Helper()
	s := loadPostgresPipeline(t)
	s.Source.URI = ""
	s.Source.Postgres = pg
	s.Source.SlotName = slot
	if err := s.Validate(); err != nil {
		t.Fatalf("validate structured spec: %v", err)
	}
	return s
}

// pgConnTo opens a direct connection for test-side seeding/DDL.
func pgConnTo(t *testing.T, uri string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", uri)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

// runPostgresPipeline starts the runner and returns a stop func that cancels
// AND waits for runner.Run to return, plus a check that fails the test if the
// runner exited early. Waiting matters: these tests share the raw.orders
// table, so the next test must not truncate it while the previous runner is
// still cleaning up. The completion signal is a channel separate from the
// error, so check does not consume what stop waits on.
func runPostgresPipeline(t *testing.T, ctx context.Context, s *spec.Spec) (stop func(), check func()) {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	var (
		mu   sync.Mutex
		err  error
		done = make(chan struct{})
	)
	go func() {
		e := runner.Run(runCtx, s, testConfig())
		mu.Lock()
		err = e
		mu.Unlock()
		close(done)
	}()
	check = func() {
		t.Helper()
		select {
		case <-done:
			mu.Lock()
			e := err
			mu.Unlock()
			t.Fatalf("runner exited early: %v", e)
		default:
		}
	}
	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Errorf("runner did not stop within 30s")
		}
	}
	return stop, check
}

// TestPostgresTLS drives the pipeline through a TLS-enabled Postgres with
// verify-full: the structured source.postgres.ssl block (#149) must apply to
// both the snapshot query and the replication connection.
func TestPostgresTLS(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const slot = "urutau_e2e_tls"
	db := pgConnTo(t, "postgres://repl:replpass@127.0.0.1:5434/shop?sslmode=require")
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50)

	s := structuredPostgresSpec(t, &spec.PostgresSource{
		Host:     "127.0.0.1",
		Port:     5434,
		Database: "shop",
		Username: "repl",
		Password: "replpass",
		SSL: &spec.SSLConfig{
			Mode: "verify-full",
			CA:   "postgres/tls/server.crt",
		},
	}, slot)

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	checkRun()

	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (101, 'tls-live', 1.5, true)`)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 101`, "tls-live")
	checkRun()
	t.Log("TLS ok: snapshot + live replication over verify-full")
}

// TestPostgresSSH drives the pipeline through an SSH bastion: the structured
// source.postgres.ssh block (#148) must tunnel both the snapshot query and
// the replication connection, with host-key verification.
func TestPostgresSSH(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const slot = "urutau_e2e_ssh"
	db := pgConn(t) // the plain postgres, reached directly for seeding
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50)

	s := structuredPostgresSpec(t, &spec.PostgresSource{
		// The bastion resolves "postgres" on the compose network; the test
		// process reaches the bastion on 127.0.0.1:2222.
		Host:     "postgres",
		Port:     5432,
		Database: "shop",
		Username: "repl",
		Password: "replpass",
		SSH: &spec.SSHConfig{
			Host:       "127.0.0.1",
			Port:       2222,
			Username:   "tunnel",
			PrivateKey: "postgres/ssh/client_ed25519",
			KnownHosts: "postgres/ssh/known_hosts",
		},
	}, slot)

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	time.Sleep(2 * time.Second)
	checkRun()
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	checkRun()

	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (101, 'ssh-live', 1.5, true)`)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 101`, "ssh-live")
	checkRun()
	t.Log("SSH ok: snapshot + live replication through the bastion")
}

// TestPostgresReconnect kills the replication backend mid-stream and proves
// the reader reconnects and resumes from the committed position (#166).
func TestPostgresReconnect(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s := loadPostgresPipeline(t)
	const slot = "urutau_e2e_reconnect"
	s.Source.SlotName = slot
	if err := s.Validate(); err != nil {
		t.Fatalf("validate spec: %v", err)
	}

	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50)

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	checkRun()

	terminateWalsender(t, db, slot)

	// The reader reconnects (1s backoff) and resumes; a row written after
	// the kill must still land, with no loss and no duplicate.
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (300, 'after-reconnect', 2.0, true)`)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 300`, "after-reconnect")
	checkRun()
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 300`, int64(1))
	t.Log("reconnect ok: stream survived a terminated replication backend")
}

// terminateWalsender waits for the slot's walsender to be active, then kills
// it, simulating a lost replication connection.
func terminateWalsender(t *testing.T, db *sql.DB, slot string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var pid sql.NullInt64
		err := db.QueryRow(
			`SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&pid)
		if err == nil && pid.Valid {
			pgExec(t, db, fmt.Sprintf(`SELECT pg_terminate_backend(%d)`, pid.Int64))
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("no active walsender to terminate")
}
