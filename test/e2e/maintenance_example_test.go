package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/internal/runner"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/spec"
)

// TestMaintenanceExample drives examples/iceberg-maintenance.yaml against
// the real MySQL + Iceberg stack: enough small commits for compaction to
// have real work, then polls until the table's data-file count drops below
// what an un-compacted run would leave. A separate table (orders_maintained)
// and serverId (1102) from TestMySQLPipeline's (orders / 1101), so the two
// tests never collide if run concurrently or interleaved.
func TestMaintenanceExample(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	s := loadMaintenanceExample(t)
	db := mysqlConn(t)

	dropIcebergTableNamed(t, ctx, "raw", "orders_maintained")
	dropAll(t, db)
	seedOrders(t, db, 0, 20)

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(runCtx, s, testConfig())
	}()

	// A handful of separately-committed live changes on top of the
	// snapshot, so compaction (minInputFiles: 2) has more than one small
	// file to work with — the same one-commit-per-write shape ordinary CDC
	// traffic produces. nCommits is the un-compacted file count the
	// assertion below measures against.
	const nCommits = 4 // 1 snapshot + 3 live changes
	waitTrino(t, ctx, `SELECT count(*) FROM orders_maintained`, int64(20))
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (301, 'm1', 1.0)`)
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (302, 'm2', 2.0)`)
	dml(t, db, `UPDATE orders SET v = 'm3' WHERE id = 1`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders_maintained`, int64(22))
	// The UPDATE does not change the row count, so the count alone can pass
	// while v is still stale — confirm the write actually landed.
	waitTrino(t, ctx, `SELECT v FROM orders_maintained WHERE id = 1`, "m3")

	// Wait for compaction WHILE the runner is still up: it runs on its own
	// interval, and stopping first could freeze the table before it ever
	// fired — making the assertion pass or fail on timing, not on compaction.
	waitCompacted(t, ctx, "orders_maintained", nCommits)

	stop()
	if err := <-runErr; err != nil && err != context.Canceled {
		t.Fatalf("run: %v", err)
	}
}

// loadMaintenanceExample loads examples/iceberg-maintenance.yaml, the same
// way loadPipeline loads mysql-iceberg.yaml.
func loadMaintenanceExample(t *testing.T) *spec.Spec {
	t.Helper()
	return loadExampleSpec(t, "iceberg-maintenance.yaml", func(s *spec.Spec) {
		s.Sink.URI = env("URUTAU_E2E_CATALOG", s.Sink.URI)
		s.Sink.Warehouse = env("URUTAU_E2E_WAREHOUSE", s.Sink.Warehouse)
	})
}

// waitCompacted polls tableName's data-file count until sink.maintenance.
// compaction has visibly run: strictly fewer files than the un-compacted
// count (the number of separate commits this test makes), or the test fails
// naming what it saw. A strict reduction is the signal — a threshold close to
// the commit count could pass without compaction ever firing.
func waitCompacted(t *testing.T, ctx context.Context, tableName string, unCompacted int) {
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

	deadline := time.Now().Add(30 * time.Second)
	var lastCount int
	for time.Now().Before(deadline) {
		tbl, err := cat.LoadTable(ctx, table.Identifier{"raw", tableName})
		if err != nil {
			t.Fatalf("load table: %v", err)
		}
		tasks, err := tbl.Scan().PlanFiles(ctx)
		if err != nil {
			t.Fatalf("plan files: %v", err)
		}
		lastCount = len(tasks)
		if lastCount < unCompacted {
			t.Logf("maintenance ok: %s compacted to %d data files (from %d commits)", tableName, lastCount, unCompacted)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("%s still has %d data files after 30s (want fewer than %d) — sink.maintenance.compaction did not run", tableName, lastCount, unCompacted)
}
