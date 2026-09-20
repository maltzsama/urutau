package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/spec"
)

// TestPostgresWal2json covers #153: the same pipeline runs over the wal2json
// logical decoding plugin instead of pgoutput. The structured source.postgres
// block selects the plugin; the JSON decoder feeds the same enqueue/commit
// path, so snapshot + live CDC work identically.
func TestPostgresWal2json(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const uri = "postgres://repl:replpass@127.0.0.1:5435/shop?sslmode=disable"
	db := pgConnTo(t, uri)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50)

	s := structuredPostgresSpec(t, &spec.PostgresSource{
		Host: "127.0.0.1", Port: 5435, Database: "shop",
		Username: "repl", Password: "replpass",
		CDC: &spec.CDCConfig{Plugin: "wal2json"},
	}, "urutau_e2e_w2j")

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	checkRun()

	pgExec(t, db, `INSERT INTO orders (id, v, amount, active) VALUES (101, 'w2j-live', 1.5, true)`)
	pgExec(t, db, `UPDATE orders SET v = 'w2j-updated' WHERE id = 10`)
	pgExec(t, db, `DELETE FROM orders WHERE id = 49`)
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 101`, "w2j-live")
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 10`, "w2j-updated")
	waitTrino(t, ctx, `SELECT count(*) FROM orders WHERE id = 49`, int64(0))
	checkRun()
	t.Log("wal2json ok: snapshot + live CDC through the JSON decoder")
}
