package e2e

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/maltzsama/urutau/spec"
)

// reserveAddr grabs a free localhost port for a coordinator listener.
func reserveAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// TestDistributedPartitionedUpsert runs a MySQL → Iceberg table split across
// THREE worker groups (the WK-001 staging path: each worker writes data files,
// the coordinator commits the binlog batch's cycle as one unit). It proves the
// snapshot, live DML routed to every partition, and a restart mid-stream all
// converge to the source state with no loss and no duplicate.
func TestDistributedPartitionedUpsert(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	addr := reserveAddr(t)
	s := loadPipeline(t)
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 3}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	groups := s.Tables[0].WorkerGroupNames(s.Pipeline)
	if len(groups) != 3 {
		t.Fatalf("WorkerGroupNames = %v, want 3 groups", groups)
	}

	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 200)

	stop, waitDone := bootPipeline(t, ctx, addr, s, groups...)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(200))

	// Live DML touching every partition's key range (ranges for ids 0..199
	// split at 66 and 132).
	for i := 200; i < 230; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'live%d', %d.0)", i, i, i))
	}
	dml(t, db, `UPDATE orders SET v = 'upd-p0' WHERE id = 5`)
	dml(t, db, `UPDATE orders SET v = 'upd-p1' WHERE id = 100`)
	dml(t, db, `UPDATE orders SET v = 'upd-p2' WHERE id = 180`)
	dml(t, db, `DELETE FROM orders WHERE id = 10`)
	dml(t, db, `DELETE FROM orders WHERE id = 120`)
	dml(t, db, `DELETE FROM orders WHERE id = 190`)

	// 200 seeded - 3 deleted + 30 inserted.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(227))
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 5`, "upd-p0")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 100`, "upd-p1")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 180`, "upd-p2")
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 10`)
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 120`)
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 190`)
	t.Log("partitioned upsert ok: snapshot + live DML across all 3 partitions")

	stop()
	if err := waitDone(); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// DML during downtime; the restart resumes from the committed position.
	dml(t, db, `UPDATE orders SET v = 'after-down-p1' WHERE id = 70`)
	dml(t, db, `UPDATE orders SET v = 'after-down-p2' WHERE id = 210`)
	dml(t, db, `DELETE FROM orders WHERE id = 140`)
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (300, 'resumed', 3.0)`)

	stop2, waitDone2 := bootPipeline(t, ctx, addr, s, groups...)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 70`, "after-down-p1")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 210`, "after-down-p2")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 300`, "resumed")
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 140`)
	// 227 - 1 deleted + 1 inserted.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(227))

	// No duplicate rows: the upsert key is unique after a restart.
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM orders`, 227)
	t.Log("partitioned upsert ok: restart resumed with no loss and no duplicate")

	stop2()
	if err := waitDone2(); err != nil {
		t.Fatalf("second run: %v", err)
	}
}

// TestDistributedPartitionedAppend runs a MySQL → Iceberg append table split
// across THREE worker groups. Deletes are dropped (append semantics), and a
// restart mid-stream must not duplicate or lose a row.
func TestDistributedPartitionedAppend(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	addr := reserveAddr(t)
	s := loadPipeline(t)
	s.Tables[0].Workers = &spec.WorkerSpec{Number: 3}
	s.Tables[0].WriteMode = spec.WriteModeAppend
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	groups := s.Tables[0].WorkerGroupNames(s.Pipeline)

	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 200)

	stop, waitDone := bootPipeline(t, ctx, addr, s, groups...)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(200))

	// Inserts across every partition. A DELETE is dropped in append mode:
	// the row stays.
	for i := 200; i < 230; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'live%d', %d.0)", i, i, i))
	}
	dml(t, db, `DELETE FROM orders WHERE id = 100`)

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(230))
	assertTrino(t, ctx, `SELECT v FROM orders WHERE id = 100`, "seed100")
	t.Log("partitioned append ok: snapshot + live inserts across all 3 partitions")

	stop()
	if err := waitDone(); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// More inserts during downtime; a restart must resume without replaying
	// the already-committed ones.
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (300, 'resumed', 3.0)`)

	stop2, waitDone2 := bootPipeline(t, ctx, addr, s, groups...)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(231))
	assertTrino(t, ctx, `SELECT v FROM orders WHERE id = 300`, "resumed")
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM orders`, 231)
	t.Log("partitioned append ok: restart resumed with no loss and no duplicate")

	stop2()
	if err := waitDone2(); err != nil {
		t.Fatalf("second run: %v", err)
	}
}
