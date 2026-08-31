package e2e

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/coordinator"
	foziceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
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
	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 30)

	boot := func() (stop func(), waitDone func() error) {
		wCtx, wStop := context.WithCancel(ctx)
		cCtx, cStop := context.WithCancel(ctx)
		done := make(chan error, 2)
		go func() {
			done <- worker.RunRemote(wCtx, worker.RemoteConfig{
				Coordinator: addr,
				Name:        "w1",
				Namespace:   "raw",
				Sink:        sinkConfig(),
				MaxRows:     100,
				MaxInterval: 2 * time.Second,
			})
		}()
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

		// Report early exits immediately instead of swallowing them until
		// the assertions give up.
		var exited int
		exitedCh := make(chan struct{})
		go func() {
			for range 2 {
				// errors.Is: Worker.Run joins the sentinel, which breaks a
				// plain != comparison.
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("process exited early: %v", err)
				}
				exited++
			}
			close(exitedCh)
		}()

		return func() { wStop(); cStop() }, func() error {
			<-exitedCh
			return nil
		}
	}

	stop, waitDone := boot()

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

	stop2, waitDone2 := boot()
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
