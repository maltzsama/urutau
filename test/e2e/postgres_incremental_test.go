package e2e

import (
	"context"
	"testing"
	"time"
)

// TestPostgresIncremental covers #157: a table in incremental mode reads rows
// past its stored cursor with no replication slot. A second run picks up only
// the rows added while it was down.
func TestPostgresIncremental(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db := pgConn(t)
	pgExec(t, db, `TRUNCATE orders`)
	dropE2ESlots(t, db)
	dropIcebergTable(t, ctx)
	seedPostgresOrders(t, db, 0, 50)

	s := loadPostgresPipeline(t)
	s.Source.SlotName = "urutau_e2e_incr"
	s.Tables[0].Mode = "incremental"
	s.Tables[0].Cursor = "id"
	if err := s.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	// First run: no stored cursor, so the pass reads all 50 rows.
	stop, checkRun := runPostgresPipeline(t, ctx, s)
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(50))
	checkRun()
	stop()

	// New rows while the pipeline is down.
	seedPostgresOrders(t, db, 50, 10)

	// Second run: the cursor resumes past id 49, so only the 10 new rows read.
	stop2, checkRun2 := runPostgresPipeline(t, ctx, s)
	defer stop2()
	waitTrino(t, ctx, `SELECT count(*) FROM orders`, int64(60))
	checkRun2()
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 50`, int64(1))
	assertCount(t, ctx, `SELECT count(*) FROM orders WHERE id = 10`, int64(1))
	t.Log("incremental ok: cursor resumes past committed rows, no slot")
}
