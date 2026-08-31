package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/runner"
	"github.com/maltzsama/urutau/internal/spec"
)

// TestFilteredPipeline drives the row filter end to end: the snapshot only
// carries rows inside the filter (WHERE pushdown), and the live stream
// applies the membership transition matrix — a row leaving the filter must
// vanish (delete), a row entering must appear (insert), and inserts or
// deletes outside the filter must be dropped.
func TestFilteredPipeline(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s := loadFilteredPipeline(t)
	db := mysqlConn(t)

	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 50)
	// A pre-existing row outside the filter: the snapshot pushdown must
	// leave it out.
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (50, 'gone', 50.0)`)

	runCtx, stop := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- runner.Run(runCtx, s, testConfig())
	}()
	defer stop()

	// Snapshot: 50 members, the excluded row never lands.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 50`)
	t.Log("snapshot pushdown ok: rows outside the filter are not backfilled")

	// Live: a row leaving the filter must vanish.
	dml(t, db, `UPDATE orders SET v = 'gone' WHERE id = 5`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(49))
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 5`)
	t.Log("exit ok: leaving the filter reflects as a delete")

	// An insert outside the filter must never appear.
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (100, 'gone', 100.0)`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(49))
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 100`)
	t.Log("insert-out ok: rows outside the filter are dropped")

	// A row re-entering the filter must appear.
	dml(t, db, `UPDATE orders SET v = 'back' WHERE id = 5`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	assertTrino(t, ctx, `SELECT v FROM orders WHERE id = 5`, "back")
	t.Log("enter ok: re-entering the filter reflects as an insert")

	// A member insert then leaving must vanish too.
	dml(t, db, `INSERT INTO orders (id, v, amount) VALUES (101, 'kept', 101.0)`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(51))
	dml(t, db, `UPDATE orders SET v = 'gone' WHERE id = 101`)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	assertMissing(t, ctx, `SELECT v FROM orders WHERE id = 101`)
	t.Log("member lifecycle ok: insert then exit reflects exactly")

	stop()
	if err := <-runErr; err != nil && err != context.Canceled {
		t.Fatalf("run: %v", err)
	}
}

// loadFilteredPipeline loads the filtered variant of the e2e spec.
func loadFilteredPipeline(t *testing.T) *spec.Spec {
	t.Helper()
	s, err := spec.LoadYAML(strings.NewReader(filteredPipelineYAML()))
	if err != nil {
		t.Fatalf("load pipeline: %v", err)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("validate pipeline: %v", err)
	}
	return s
}

func filteredPipelineYAML() string {
	return `
pipeline: e2e-mysql-filtered
source:
  kind: mysql
  uri: mysql://repl:replpass@127.0.0.1:3306/shop
sink:
  uri: ` + env("FOZ_E2E_CATALOG", "http://localhost:8181/api/catalog") + `
  namespace: raw
  warehouse: ` + env("FOZ_E2E_WAREHOUSE", "quickstart_catalog") + `
  clientId: root
  clientSecret: s3cr3t
  scope: PRINCIPAL_ROLE:ALL
tables:
  - source: shop.orders
    target: raw.orders
    primaryKey: [id]
    createIfNotExists: true
    filter:
      col: v
      op: neq
      value: gone
`
}
