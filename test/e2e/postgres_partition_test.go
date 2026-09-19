package e2e

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/maltzsama/urutau/spec"
)

// TestPostgresPartitionedSnapshot drives the CTID strategy over a range-
// partitioned table: the parent has no storage, so the leaf partitions' pages
// drive a proportional split (#151).
func TestPostgresPartitionedSnapshot(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders_part`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	pgExec(t, db, `INSERT INTO orders_part (id, v, amount, active)
		SELECT g, 'part-' || g, (g % 7)::double precision, g % 2 = 0
		FROM generate_series(0, 1999) g`)

	s := loadPostgresPipeline(t)
	s.Source.SlotName = "urutau_e2e_part"
	s.Tables[0].Source = "public.orders_part"
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	time.Sleep(2 * time.Second)
	checkRun()
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(2000))
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 1500`, "part-1500")
	checkRun()
	t.Log("partitioned CTID snapshot ok: 2000 rows across two leaf partitions")
}

// TestDistributedPostgresWorkers covers workers>1 range partitioning (#174):
// a single-column PK table is split across two worker groups, each owning a
// contiguous key range for both the snapshot and the live stream.
func TestDistributedPostgresWorkers(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active)
		SELECT g, 'seed-' || g, (g % 7)::double precision, g % 2 = 0
		FROM generate_series(0, 499) g`)

	s := loadPostgresPipeline(t)
	s.Source.SlotName = "urutau_e2e_workers"
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 2}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	names := s.Tables[0].WorkerGroupNames(s.Pipeline)
	if len(names) != 2 {
		t.Fatalf("want 2 worker groups, got %d", len(names))
	}
	stop, waitDone := bootPipeline(t, ctx, addr, s, names[0], names[1])
	defer func() { stop(); _ = waitDone() }()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(500))
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 250`, "seed-250")

	// Live DML on both sides of the partition boundary must still converge.
	pgExec(t, db, `UPDATE orders SET v = 'live-low' WHERE id = 100`)
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (10000, 'live-high', 1.0, true)`)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 100`, "live-low")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 10000`, "live-high")
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(501))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 10000`, int64(1))
	t.Log("workers>1 ok: range-partitioned snapshot and live stream")
}

// TestDistributedPostgresEmptyTable partitions an EMPTY table across two
// workers: with no data to sample, the ranges must come from the key type's
// domain (disjoint, never CTID, never overlapping). It then inserts rows and
// checks each is captured exactly once — an overlap would duplicate them.
func TestDistributedPostgresEmptyTable(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)

	s := loadPostgresPipeline(t)
	s.Source.SlotName = "urutau_e2e_empty"
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 2}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	names := s.Tables[0].WorkerGroupNames(s.Pipeline)
	if len(names) != 2 {
		t.Fatalf("want 2 worker groups, got %d", len(names))
	}
	stop, waitDone := bootPipeline(t, ctx, addr, s, names[0], names[1])
	defer func() { stop(); _ = waitDone() }()

	time.Sleep(2 * time.Second)

	pgExec(t, db, `INSERT INTO orders (id, v, amount, active)
		SELECT g, 'empty-' || g, 1.0, true FROM generate_series(0, 99) g`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(100))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 50`, int64(1))
	t.Log("empty-table workers>1 ok: disjoint domain ranges, no duplicates")
}
