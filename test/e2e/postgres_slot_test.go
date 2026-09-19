package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/source/postgres"
	"github.com/maltzsama/urutau/position"
)

// TestPostgresSlotValidationAndAdvance covers #156 against a real slot: the
// stored resume is reconciled with the slot's confirmed_flush_lsn before the
// stream attaches, and the slot is advanced to the effective resume point.
func TestPostgresSlotValidationAndAdvance(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	db := pgConn(t)
	slot := "urutau_e2e_slotval"
	dropSlot(t, db, slot)
	pgExec(t, db, fmt.Sprintf(`SELECT pg_catalog.pg_create_logical_replication_slot('%s', 'pgoutput')`, slot))
	t.Cleanup(func() { dropSlot(t, db, slot) })

	confirmed, err := postgres.ConfirmedLSN(ctx, db, slot)
	if err != nil {
		t.Fatalf("confirmed lsn: %v", err)
	}

	// First boot (no resume) starts at the slot's point.
	got, err := postgres.ValidateSlotState(ctx, db, slot, nil, nil)
	if err != nil || got.Compare(confirmed) != 0 {
		t.Fatalf("first boot = %v, %v; want %v", got, err, confirmed)
	}

	// A resume behind the slot drifts forward to the slot's authoritative
	// point.
	behind := *confirmed - 1
	got, err = postgres.ValidateSlotState(ctx, db, slot, &behind, nil)
	if err != nil || got.Compare(confirmed) != 0 {
		t.Fatalf("behind resume = %v, %v; want %v", got, err, confirmed)
	}

	// A resume ahead of the slot means the slot was reset/replaced: fail.
	ahead := *confirmed + 1_000_000
	if _, err := postgres.ValidateSlotState(ctx, db, slot, &ahead, nil); err == nil {
		t.Fatal("a resume ahead of the slot must error")
	}

	// AdvanceSlot moves the slot's confirmed point to the target.
	pgExec(t, db, `SELECT pg_catalog.pg_switch_wal()`)
	var curStr string
	if err := db.QueryRowContext(ctx, `SELECT pg_catalog.pg_current_wal_lsn()::text`).Scan(&curStr); err != nil {
		t.Fatalf("current wal lsn: %v", err)
	}
	cur, err := position.ParseLSN(curStr)
	if err != nil {
		t.Fatalf("parse current lsn: %v", err)
	}
	if cur.Compare(confirmed) <= 0 {
		t.Fatalf("current WAL %s is not ahead of confirmed %s", cur, confirmed)
	}
	if err := postgres.AdvanceSlot(ctx, db, slot, *cur); err != nil {
		t.Fatalf("advance slot: %v", err)
	}
	after, err := postgres.ConfirmedLSN(ctx, db, slot)
	if err != nil {
		t.Fatalf("confirmed after advance: %v", err)
	}
	if after.Compare(cur) != 0 {
		t.Fatalf("confirmed after advance = %s, want %s", after, cur)
	}
	t.Log("slot validation + advance ok")
}
