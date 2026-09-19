package e2e

import (
	"context"
	"testing"
	"time"
)

// TestPostgresSnapshotMultiBatch is the regression for the iceberg
// cycle-committed guard: a snapshot larger than one worker batch produces
// several commits that all carry the SAME position (the snapshot's low
// watermark). The guard used to treat a position match as "already
// committed", so every batch after the first was skipped and only the first
// batch's rows landed. 500 rows at chunkSize 10 is 5 batches of 100.
func TestPostgresSnapshotMultiBatch(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const slot = "urutau_e2e_multibatch"
	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	pgExec(t, db, `INSERT INTO orders (id, v, amount, active)
		SELECT g, 'seed-' || g, (g % 7)::double precision, g % 2 = 0
		FROM generate_series(0, 499) g`)

	s := loadPostgresPipeline(t)
	s.Source.SlotName = slot
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	stop, checkRun := runPostgresPipeline(t, ctx, s)
	defer stop()

	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(500))
	waitTrino(t, ctx, `SELECT v FROM orders WHERE id = 499`, "seed-499")
	checkRun()
	t.Log("multi-batch snapshot: every batch landed")
}
