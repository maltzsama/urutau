package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/spec"
)

// TestPostgresDiscovery drives a discovery pipeline (#152): the spec lists no
// tables, the source enumerates public's base tables and partitioned parents,
// and each target is derived as <namespace>.<relname>. Leaf partitions must
// NOT be discovered — the parent covers their rows, so including them would
// replicate every row twice.
func TestPostgresDiscovery(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	pgExec(t, db, `TRUNCATE orders_part`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	dropIcebergNamed(t, ctx, "raw.orders_part")
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active)
		SELECT g, 'disc-' || g, 1.0, true FROM generate_series(0, 49) g`)
	pgExec(t, db, `INSERT INTO orders_part (id, v, amount, active)
		SELECT g, 'discp-' || g, 1.0, true FROM generate_series(0, 9) g`)

	s := loadPostgresPipeline(t)
	s.Source.URI = ""
	s.Source.SlotName = "urutau_e2e_discover"
	s.Source.Postgres = &spec.PostgresSource{
		Host: "127.0.0.1", Port: 5433, Database: "shop",
		Username: "repl", Password: "replpass",
		Discover: true, Schemas: []string{"public"},
	}
	s.Tables = nil
	if err := s.Validate(); err != nil {
		t.Fatalf("validate discovery spec: %v", err)
	}

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	// Both the base table and the partitioned parent are discovered.
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	waitTrino(t, ctx, `SELECT count(*) FROM orders_part`, int64(10))
	checkRun()

	// The leaf partitions are NOT discovered: their target table must not exist.
	if _, err := trinoQuery(ctx, `SELECT count(*) FROM orders_part_0`); err == nil {
		t.Fatal("orders_part_0 must not be discovered (its parent covers its rows)")
	}
	t.Log("discovery ok: base + partitioned parent, leaves excluded")
}
