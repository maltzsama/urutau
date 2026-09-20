package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/spec"
)

// TestMySQLColumnFilterAndFilter covers #183: a MySQL table's columnFilter and
// filter are applied to the snapshot AND the live stream, so the backfill and
// the CDC agree. Before the fix both were silently ignored.
func TestMySQLColumnFilterAndFilter(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s := loadPipeline(t)
	s.Tables[0].ColumnFilter = []string{"id", "v"}
	s.Tables[0].Filter = &spec.Filter{Predicate: &spec.Predicate{Column: "id", Op: spec.OpGte, Value: 200}}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	db := mysqlConn(t)
	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 100, 200) // ids 100..299; the filter keeps 200..299

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- runner.Run(runCtx, s, testConfig()) }()
	defer stop()

	// Only ids >= 200 survive the filter.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(100))

	// The amount column is excluded by the projection.
	if _, err := trinoQuery(ctx, `SELECT amount FROM orders`); err == nil {
		t.Fatal("amount must be excluded by columnFilter")
	}

	// The live stream honours the same filter: 350 is kept, 50 is dropped.
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (350, 'keep', 1.0)`)
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (50, 'drop', 1.0)`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(101))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 50`, int64(0))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 350`, int64(1))

	stop()
	if err := <-runErr; err != nil && err != context.Canceled {
		t.Fatalf("run: %v", err)
	}
	t.Log("mysql filter+columnFilter ok: backfill and CDC agree")
}
