package e2e

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/coordinator"
	"github.com/maltzsama/urutau/internal/worker"
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

// TestDistributedPartitionedWorkerKilled kills ONE of the three worker groups
// of a partitioned table mid-stream — the WK-001 §2.1 regression: a worker's
// death must not silently discard its own batches while the others carry on.
//
// A worker session loss is a job failure (fail loud), never a silent reset:
// the run terminates and the restart replays every partition from the
// coordinator's committed position. The killed partition's uncommitted batch
// and the two live partitions must all converge — no loss, no duplicate.
func TestDistributedPartitionedWorkerKilled(t *testing.T) {
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

	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 200)

	// boot starts the coordinator and the three worker groups over real
	// sockets. It returns the coordinator's exit channel, a per-worker kill
	// switch, and each worker's exit channel.
	boot := func() (<-chan error, []func(), []<-chan error) {
		cCtx, cStop := context.WithCancel(ctx)
		t.Cleanup(cStop)
		cErr := make(chan error, 1)
		go func() {
			cErr <- coordinator.Run(cCtx, coordinator.Config{
				Spec: s, ListenAddr: addr, ServerID: 1102,
				Heartbeat: 5 * time.Second, ChunkSize: 10,
				WindowTimeout: 2 * time.Minute, CaughtUpPoll: 300 * time.Millisecond,
				WaitWorker: 2 * time.Minute,
			})
		}()
		kills := make([]func(), len(groups))
		wErrs := make([]<-chan error, len(groups))
		for i, name := range groups {
			wCtx, wStop := context.WithCancel(ctx)
			t.Cleanup(wStop)
			kills[i] = wStop
			ch := make(chan error, 1)
			wErrs[i] = ch
			go func(name string, ch chan<- error) {
				ch <- worker.RunRemote(wCtx, worker.RemoteConfig{
					Coordinator: addr, Name: name, Namespace: "raw", Sink: workerSink(),
					MaxRows: 100, MaxInterval: time.Second,
				})
			}(name, ch)
		}
		return cErr, kills, wErrs
	}

	cErr, kills, wErrs := boot()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(200))
	// Live inserts extend past the snapshot's max id, so they land in the
	// open-ended last range; the updates below carry the per-partition proof.
	for i := 200; i < 230; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'live%d', %d.0)", i, i, i))
	}
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(230))

	// Kill partition 0's worker. The job must fail loud, not limp along with
	// the dead partition's uncommitted batch dropped.
	kills[0]()
	select {
	case err := <-cErr:
		if err == nil {
			t.Fatal("coordinator returned nil after a worker was killed — a silent loss")
		}
		t.Logf("killed worker failed the job loud: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("coordinator did not fail after a worker was killed")
	}
	// Every process of the failed run must exit before the restart binds the
	// same address.
	for _, ch := range wErrs {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
			t.Fatal("a worker did not exit after the job failed")
		}
	}

	// DML during the outage: the restart replays every partition from the
	// committed position, so none of this is lost.
	dml(t, db, `UPDATE orders SET v = 'p0-after' WHERE id = 5`)   // range [0,66)
	dml(t, db, `UPDATE orders SET v = 'p1-after' WHERE id = 100`) // range [66,132)
	dml(t, db, `UPDATE orders SET v = 'p2-after' WHERE id = 180`) // range [132,∞)
	for i := 300; i < 310; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO orders (id, v, amount) VALUES (%d, 'gap%d', %d.0)", i, i, i))
	}

	boot()
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 5`, "p0-after")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 100`, "p1-after")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 180`, "p2-after")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 300`, "gap300")
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(240))
	assertCount(t, ctx, `SELECT count(DISTINCT id) FROM orders`, 240)
	t.Log("partitioned recovery ok: one worker killed, fail loud, restart, no loss, no duplicate")
}
