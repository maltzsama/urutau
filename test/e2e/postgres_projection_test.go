package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/maltzsama/urutau/spec"
)

// TestPostgresColumnFilter drives a snapshot and a live stream through a
// source column projection (#162): only the selected columns reach the
// target, and the excluded ones are absent from the table schema.
func TestPostgresColumnFilter(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const slot = "urutau_e2e_colfilter"
	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50)

	s := loadPostgresPipeline(t)
	s.Source.SlotName = slot
	s.Tables[0].ColumnFilter = []string{"id", "v"}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	time.Sleep(2 * time.Second)
	checkRun()
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	checkRun()

	// The excluded columns must not exist in the target schema.
	if _, err := trinoQuery(ctx, `SELECT amount FROM orders LIMIT 1`); err == nil {
		t.Fatal("amount must not exist in a column-filtered target")
	}
	if _, err := trinoQuery(ctx, `SELECT active FROM orders LIMIT 1`); err == nil {
		t.Fatal("active must not exist in a column-filtered target")
	}

	// A selected column replicates live.
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (101, 'cf-live', 1.5, true)`)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 101`, "cf-live")
	checkRun()
	t.Log("column filter ok: only id+v land; amount/active absent")
}

// TestPostgresFilter drives the snapshot and the live stream through a
// structured source filter (#163): only rows matching the predicate reach
// the target, and a row that leaves the filter is removed.
func TestPostgresFilter(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const slot = "urutau_e2e_filter"
	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50) // active = i%2==0 → 25 active

	s := loadPostgresPipeline(t)
	s.Source.SlotName = slot
	s.Tables[0].Filter = &spec.Filter{
		Predicate: &spec.Predicate{Column: "active", Op: spec.OpEq, Value: true},
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	time.Sleep(2 * time.Second)
	checkRun()
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(25))
	checkRun()

	// An insert that matches the filter lands; one that does not is dropped.
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (101, 'f-active', 1.0, true)`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(26))
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (102, 'f-inactive', 1.0, false)`)
	time.Sleep(2 * time.Second)
	assertCount(t, ctx, `SELECT count(*) FROM orders`, int64(26))

	// A row that leaves the filter (active → inactive) is removed.
	pgExec(t, db, `UPDATE orders SET active = false WHERE id = 0`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(25))
	checkRun()
	t.Log("filter ok: only matching rows land; an out-of-filter update removes the row")
}

// TestDistributedPostgresProjection proves the projection/filter reach the
// worker's snapshot chunk SELECT in distributed mode: the coordinator ships
// columnFilter + filter in the assignment, and the worker's source resolves
// them.
func TestDistributedPostgresProjection(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	const slot = "urutau_e2e_dist_proj"
	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50) // active = i%2==0 → 25 active

	s := loadPostgresPipeline(t)
	s.Source.SlotName = slot
	s.Tables[0].ColumnFilter = []string{"id", "v"}
	s.Tables[0].Filter = &spec.Filter{
		Predicate: &spec.Predicate{Column: "active", Op: spec.OpEq, Value: true},
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	w1 := s.Tables[0].WorkerGroupNames(s.Pipeline)[0]
	stop, waitDone := bootPipeline(t, ctx, addr, s, w1)
	defer func() { stop(); _ = waitDone() }()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(25))
	if _, err := trinoQuery(ctx, `SELECT amount FROM orders LIMIT 1`); err == nil {
		t.Fatal("amount must not exist in the distributed column-filtered target")
	}

	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (101, 'dp-live', 1.0, true)`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(26))
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (102, 'dp-off', 1.0, false)`)
	time.Sleep(2 * time.Second)
	assertCount(t, ctx, `SELECT count(*) FROM orders`, int64(26))
	t.Log("distributed projection ok: worker snapshot honours columnFilter + filter")
}
