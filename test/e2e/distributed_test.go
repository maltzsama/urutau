package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/maltzsama/urutau/internal/coordinator"
	foziceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/spec"
	"github.com/maltzsama/urutau/internal/worker"
)

// TestDistributedPipeline runs the split architecture end to end over real
// sockets: a coordinator process (source reader + DBLog snapshot + Flight
// server) and a worker process (Iceberg writer fed by Flight). Same proof
// as the collapsed pipeline: snapshot brings the pre-existing rows, live
// INSERT/UPDATE/DELETE replicate, and a restart resumes from the committed
// position with no loss and no duplicate.
func TestDistributedPipeline(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// A free port for the coordinator's gRPC + Flight listener.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	s := loadPipeline(t)
	// Name the worker group so the Hello ("w1") matches the registry.
	s.Tables[0].Worker = "w1"
	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 30)

	boot := func(workers ...string) (stop func(), waitDone func() error) {
		return bootPipeline(t, ctx, addr, s, workers...)
	}

	stop, waitDone := boot("w1")

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(30))
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (101, 'live1', 1.5)`)
	dml(t, db, `UPDATE orders SET v = 'upd' WHERE id = 1`)
	dml(t, db, `DELETE FROM orders WHERE id = 2`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(30)) // 30 - 1 (del) + 1 (live)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 1`, "upd")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 101`, "live1")
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 2`)

	t.Log("stream ok: snapshot + live DML through coordinator → Flight → worker")

	stop()
	if err := waitDone(); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// DML during downtime; the restart resumes from the committed position.
	dml(t, db, `UPDATE orders SET v = 'after-down' WHERE id = 1`)
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (200, 'resumed', 9.0)`)

	stop2, waitDone2 := boot("w1")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 1`, "after-down")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 200`, "resumed")
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(31))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 1`, int64(1))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 200`, int64(1))

	t.Log("resume ok: no loss, no duplicate after downtime DML")
	stop2()
	if err := waitDone2(); err != nil {
		t.Fatalf("second run: %v", err)
	}
}

func sinkConfig() foziceberg.Config {
	return foziceberg.Config{
		URI:          env("FOZ_E2E_CATALOG", "http://localhost:8181/api/catalog"),
		Warehouse:    env("FOZ_E2E_WAREHOUSE", "quickstart_catalog"),
		ClientID:     "root",
		ClientSecret: "s3cr3t",
		Scope:        "PRINCIPAL_ROLE:ALL",
	}
}

// dropIcebergNamed drops a target table from the catalog.
func dropIcebergNamed(t *testing.T, ctx context.Context, target string) {
	t.Helper()
	cat, err := foziceberg.NewCatalog(ctx, sinkConfig())
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	ns, name, _ := strings.Cut(target, ".")
	_ = cat.DropTable(ctx, table.Identifier{ns, name})
}

// TestWorkerSuicide kills the coordinator and asserts the worker exits with
// an error (channel death) rather than committing anything further — the
// no-zombie invariant (design §13 item 10): three streams on one conn, so a
// coordinator death takes Session/Control/Flight together.
func TestWorkerSuicide(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	s := loadPipeline(t)
	s.Tables[0].Worker = "w1"
	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 20)

	wCtx, wStop := context.WithCancel(ctx)
	defer wStop()
	cCtx, cStop := context.WithCancel(ctx)

	wErr := make(chan error, 1)
	cErr := make(chan error, 1)
	go func() {
		wErr <- worker.RunRemote(wCtx, worker.RemoteConfig{Coordinator: addr, Name: "w1", Namespace: "raw", Sink: sinkConfig(), MaxRows: 100, MaxInterval: time.Second})
	}()
	go func() {
		cErr <- coordinator.Run(cCtx, coordinator.Config{Spec: s, ListenAddr: addr, ServerID: 1102, Heartbeat: 5 * time.Second, ChunkSize: 10, WindowTimeout: 2 * time.Minute, CaughtUpPoll: 300 * time.Millisecond, WaitWorker: 2 * time.Minute})
	}()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(20))

	// Kill the coordinator. The worker's streams share one conn; whether it
	// surfaces the shutdown signal or a dead channel, it must EXIT — the
	// no-zombie invariant (design §13 item 10). We assert exit, not the
	// error vs nil distinction: our in-process shutdown is graceful by
	// design (the coordinator drains workers first), so a clean nil is fine.
	cStop()
	select {
	case err := <-cErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("coordinator: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("coordinator did not exit")
	}
	select {
	case err := <-wErr:
		if err != nil {
			t.Logf("suicide ok: worker exited with error: %v", err)
		} else {
			t.Log("suicide ok: worker exited cleanly (graceful drain on shutdown)")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not exit after coordinator death (keepalive/EOF failed)")
	}
}

// TestWorkerGracefulShutdown stops the coordinator cleanly (it sends
// Shutdown{drain} over Control) and asserts the worker drains and exits 0 —
// no loss, no error (design §13 item 10c). A row inserted just before the
// shutdown must still land.
func TestWorkerGracefulShutdown(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	s := loadPipeline(t)
	s.Tables[0].Worker = "w1"
	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 20)

	wCtx, wStop := context.WithCancel(ctx)
	defer wStop()
	cCtx, cStop := context.WithCancel(ctx)

	wErr := make(chan error, 1)
	cErr := make(chan error, 1)
	// MaxInterval is long so the in-flight row sits in the worker's batch
	// buffer (not committed by a timer) when the shutdown signal arrives;
	// the drain must commit it.
	go func() {
		wErr <- worker.RunRemote(wCtx, worker.RemoteConfig{Coordinator: addr, Name: "w1", Namespace: "raw", Sink: sinkConfig(), MaxRows: 10000, MaxInterval: 30 * time.Second})
	}()
	go func() {
		cErr <- coordinator.Run(cCtx, coordinator.Config{Spec: s, ListenAddr: addr, ServerID: 1102, Heartbeat: 5 * time.Second, ChunkSize: 10, WindowTimeout: 2 * time.Minute, CaughtUpPoll: 300 * time.Millisecond, WaitWorker: 2 * time.Minute})
	}()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(20))
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (500, 'in-flight', 5.0)`)
	// Let the worker pull the row over Flight so it is genuinely in flight
	// (buffered, uncommitted) when the shutdown arrives.
	time.Sleep(2 * time.Second)

	// Clean shutdown: the coordinator drains its workers before exiting.
	cStop()
	select {
	case err := <-cErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("coordinator: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("coordinator did not exit")
	}
	select {
	case err := <-wErr:
		if err != nil {
			t.Fatalf("worker did not drain cleanly: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not exit after graceful shutdown")
	}

	// The in-flight row committed during the drain.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(21))
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 500`, "in-flight")
	t.Log("graceful shutdown ok: in-flight row drained and committed")
}

// bootPipeline starts N workers plus the coordinator over real sockets. It
// returns stop (cancels the coordinator) and waitDone (blocks until every
// process exited). Early exits are reported via t.Errorf.
func bootPipeline(t *testing.T, ctx context.Context, addr string, s *spec.Spec, workers ...string) (func(), func() error) {
	t.Helper()
	n := len(workers) + 1 // + coordinator
	done := make(chan error, n)
	for _, name := range workers {
		wCtx, wStop := context.WithCancel(ctx)
		go func(name string) {
			done <- worker.RunRemote(wCtx, worker.RemoteConfig{
				Coordinator: addr,
				Name:        name,
				Namespace:   "raw",
				Sink:        sinkConfig(),
				MaxRows:     100,
				MaxInterval: 2 * time.Second,
			})
		}(name)
		t.Cleanup(wStop)
	}
	cCtx, cStop := context.WithCancel(ctx)
	go func() {
		done <- coordinator.Run(cCtx, coordinator.Config{
			Spec:          s,
			ListenAddr:    addr,
			ServerID:      1102, // distinct from the collapsed runs' 1101
			Heartbeat:     5 * time.Second,
			ChunkSize:     10,
			WindowTimeout: 2 * time.Minute,
			CaughtUpPoll:  300 * time.Millisecond,
			WaitWorker:    2 * time.Minute,
		})
	}()

	// Report early exits immediately instead of swallowing them until the
	// assertions give up.
	exitedCh := make(chan struct{})
	go func() {
		for range n {
			// errors.Is: Worker.Run joins the sentinel, which breaks a
			// plain != comparison.
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("process exited early: %v", err)
			}
		}
		close(exitedCh)
	}()

	return func() { cStop() }, func() error {
		<-exitedCh
		return nil
	}
}

// TestDistributedMultiWorker runs two worker groups off one source: orders
// (worker w1) and order_items (worker w2), snapshot chunk SELECTs executed on
// the workers, concurrent update bursts that force DBLog window drops, and
// live DML — final state must converge per table (invariant 5).
func TestDistributedMultiWorker(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	s := loadPipeline(t)
	// Two tables, two worker groups: orders→w1, order_items→w2.
	s.Tables = append(s.Tables, spec.Table{
		Source:            "shop.order_items",
		Target:            "raw.order_items",
		PrimaryKey:        []string{"order_id", "line_no"},
		CreateIfNotExists: true,
		Worker:            "w2",
	})
	s.Tables[0].Worker = "w1"
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	db := mysqlConn(t)
	resetBinlog(t, db)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS order_items (
		order_id BIGINT NOT NULL,
		line_no  INT    NOT NULL,
		sku      VARCHAR(64) NOT NULL,
		qty      INT    NOT NULL,
		PRIMARY KEY (order_id, line_no))`); err != nil {
		t.Fatalf("create order_items: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM order_items`); err != nil {
		t.Fatalf("clear order_items: %v", err)
	}
	dropIcebergTable(t, ctx)
	dropIcebergNamed(t, ctx, "raw.order_items")
	dropAll(t, db)
	seedOrders(t, db, 0, 200)
	for i := 0; i < 150; i++ {
		dml(t, db, fmt.Sprintf("INSERT INTO order_items (order_id, line_no, sku, qty) VALUES (%d, 1, 'sku%d', %d)", i, i, i%7+1))
	}

	stop, waitDone := bootPipeline(t, ctx, addr, s, "w1", "w2")

	// Concurrent burst: touch orders while chunks are being SELECTed by w1.
	// Bounded in time (not iterations): under -race the pipeline is slower,
	// an open-ended burst would run for minutes, flood the worker past its
	// drain rate, and time the converge below out. ~1s of updates is plenty
	// to force DBLog window drops.
	stopBurst := make(chan struct{})
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		for i := 0; ; i++ {
			select {
			case <-stopBurst:
				return
			case <-time.After(20 * time.Millisecond):
			}
			if _, err := db.Exec(fmt.Sprintf(`UPDATE orders SET v='u%d' WHERE id <= 199`, i)); err != nil {
				t.Logf("burst: %v", err)
				return
			}
		}
	}()
	time.Sleep(time.Second)
	close(stopBurst)
	<-burstDone

	// The snapshot must surface all rows and the stream catch up to the
	// last burst state before live DML.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(200))
	convergeOrders(t, ctx, db)

	waitTrino(t, ctx, `SELECT count(*) FROM order_items`, int64(150))

	// Live DML on both tables.
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (500, 'live', 5.0)`)
	dml(t, db, `UPDATE order_items SET qty = 99 WHERE order_id = 1`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(201))
	waitTrino(t, ctx, `SELECT qty FROM order_items WHERE order_id = 1`, int64(99))
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 500`, "live")

	// Final state must mirror the source for both tables.
	convergeOrderItems(t, ctx, db)

	t.Log("multi-worker ok: two groups, worker-side chunk SELECTs, DBLog under load")
	stop()
	if err := waitDone(); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// convergeOrderItems polls until order_items in Trino mirrors the source.
func convergeOrderItems(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		src, err := sourceItems(db)
		ice, terr := trinoQuery(ctx, `SELECT order_id, line_no, sku, qty FROM order_items ORDER BY order_id`)
		if err == nil && terr == nil && equalItems(src, ice) {
			t.Logf("order_items converged: %d rows", len(src))
			return
		}
		if time.Now().After(deadline) {
			src, _ := sourceItems(db)
			ice, _ := trinoQuery(ctx, `SELECT order_id, line_no, sku, qty FROM order_items ORDER BY order_id`)
			t.Fatalf("order_items never converged: source=%d iceberg=%d", len(src), len(ice))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// convergeOrders polls until orders in Trino mirrors the source.
func convergeOrders(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(180 * time.Second)
	for {
		src, err := sourceState(db)
		ice, terr := trinoQuery(ctx, `SELECT id, v, amount FROM orders ORDER BY id`)
		if err == nil && terr == nil && stateEqual(t, src, ice) {
			t.Logf("orders converged: %d rows", len(src))
			return
		}
		if time.Now().After(deadline) {
			src, _ := sourceState(db)
			ice, _ := trinoQuery(ctx, `SELECT id, v, amount FROM orders ORDER BY id`)
			t.Fatalf("orders never converged: source=%d iceberg=%d", len(src), len(ice))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func sourceItems(db *sql.DB) ([][]any, error) {
	rows, err := db.Query(`SELECT order_id, line_no, sku, qty FROM order_items ORDER BY order_id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out [][]any
	for rows.Next() {
		var oid int64
		var ln int
		var sku string
		var qty int
		if err := rows.Scan(&oid, &ln, &sku, &qty); err != nil {
			return nil, err
		}
		out = append(out, []any{oid, int64(ln), sku, int64(qty)})
	}
	return out, rows.Err()
}

func equalItems(src, ice [][]any) bool {
	if len(src) != len(ice) {
		return false
	}
	for i := range src {
		for c := 0; c < 4; c++ {
			if src[i][c] != ice[i][c] {
				return false
			}
		}
	}
	return true
}
