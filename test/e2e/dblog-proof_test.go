package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/runner"
)

// TestDBLogConcurrentLoad proves the DBLog window under real contention:
// 500 pre-existing rows are chunked (chunk size 1 → 500 chunks, each
// covering exactly one id) while a tight update loop races the chunking.
// A window's [OpenWindow, ClearWindow] span (internal/snapshot/dblog.go)
// covers the chunk's own SELECT plus the caught-up wait; an update landing
// on THAT chunk's id inside that span decodes InWindow and must be
// discarded as stale. drainAndCompare (invariant 5, the mirrored final
// state) is always a hard assertion — it does not depend on the race
// firing. requireWindowRace is the timing-dependent half, isolated so a
// flake there reads as exactly that instead of muddying the state-mismatch
// signal (issue #76).
func TestDBLogConcurrentLoad(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	s := loadPipeline(t)
	db := mysqlConn(t)

	resetBinlog(t, db)
	dropIcebergTable(t, ctx)
	dropAll(t, db)
	seedOrders(t, db, 0, 500)

	// NewRunner returns only after the snapshot phase, so the update loop
	// below overlaps the chunking window by construction. MaxRows is raised
	// so the buffered events collapse into few Iceberg commits.
	snapDone := make(chan *runner.Runner, 1)
	runErr := make(chan error, 1)
	// Chunk size 1 stretches the phase to 500 chunk round trips, each its
	// own window over exactly one id — long enough, and granular enough,
	// for the update loop below to land inside many of them.
	cfg := testConfig()
	cfg.ChunkSize = 1
	cfg.MaxRows = 10000
	go func() {
		r, err := runner.NewRunner(ctx, s, cfg)
		if err != nil {
			runErr <- err
			snapDone <- nil
			return
		}
		snapDone <- r
		runErr <- r.Run(ctx)
	}()

	// Update loop: hammer every id for the full duration of the snapshot
	// phase, not a fixed wall-clock window — under full-suite load (slower
	// chunk round trips) the phase runs longer and the loop simply gets
	// more attempts, instead of a fixed 10s window covering a shrinking
	// fraction of a slower phase.
	//
	// A short sleep between updates is load-bearing, not incidental: with
	// none, the update rate outruns the binlog reader's own drain rate and
	// WaitCaughtUp's fixed `high` (captured once per chunk, source/dblog.go)
	// is never reached — the window then sits open until WindowTimeout and
	// the whole snapshot fails, rather than the race we're trying to prove.
	// 5ms (vs. the original 15ms) raises the attempt rate — and thus the
	// chance the race fires within one run — while staying well under that
	// failure mode (verified against live infra: 15ms headroom to spare).
	stopBurst := make(chan struct{})
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		for i := 0; ; i++ {
			select {
			case <-stopBurst:
				return
			default:
			}
			q := fmt.Sprintf(`UPDATE orders SET v = 'u%d' WHERE id <= 499`, i)
			if _, err := db.Exec(q); err != nil {
				t.Logf("burst %d: %v", i, err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	var r *runner.Runner
	select {
	case r = <-snapDone:
		if r == nil {
			t.Fatalf("runner setup failed: %v", <-runErr)
		}
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for the snapshot phase")
	}

	// Stop the update loop; the source is now quiescent. Drain the stream
	// until the Iceberg state equals the source, row by row — this is the
	// hard correctness assertion (invariant 5) and holds regardless of
	// whether the race below fired.
	close(stopBurst)
	<-burstDone
	drainAndCompare(t, ctx, db)

	requireWindowRace(t, r)
}

// requireWindowRace is the timing-dependent half of the proof: at least one
// stale snapshot row was discarded by an InWindow live event. Isolated from
// drainAndCompare (issue #76) so a failure here reads as "the race didn't
// fire this run" rather than corrupting the state-convergence signal, and
// so it fails immediately instead of hiding inside a 2-minute retry loop.
func requireWindowRace(t *testing.T, r *runner.Runner) {
	t.Helper()
	if n := r.DroppedByWindow("raw.orders"); n == 0 {
		t.Fatal("droppedByWindow == 0: the window never discarded a stale row — the proof failed")
	} else {
		t.Logf("dblog proof ok: droppedByWindow=%d", n)
	}
}

// drainAndCompare polls until the Iceberg table exactly mirrors the source:
// same id set, same v, same amount for every row. Fails fast on the first
// query error instead of swallowing it into the deadline loop (issue #76) —
// a broken connection should surface immediately, not after minutes of
// retries that can never succeed. The convergence deadline itself is left
// generous (real commit latency under full-suite load, not a race), since
// shortening it would trade one flake for another.
func drainAndCompare(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		src, err := sourceState(db)
		if err != nil {
			t.Fatalf("source query failed: %v", err)
		}
		ice, err := trinoQuery(ctx, `SELECT id, v, amount FROM orders ORDER BY id`)
		if err != nil {
			t.Fatalf("iceberg query failed: %v", err)
		}
		if stateEqual(t, src, ice) {
			t.Logf("state converged: %d rows mirrored", len(src))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("state never converged: source=%d rows, iceberg=%d rows", len(src), len(ice))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// sourceState reads id, v, amount from MySQL ordered by id.
func sourceState(db *sql.DB) ([][]any, error) {
	rows, err := db.Query(`SELECT id, v, amount FROM orders ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out [][]any
	for rows.Next() {
		var id int64
		var v string
		var amount float64
		if err := rows.Scan(&id, &v, &amount); err != nil {
			return nil, err
		}
		out = append(out, []any{id, v, amount})
	}
	return out, rows.Err()
}

// stateEqual compares the source rows against the Trino result. Types on
// the Trino side: id int64, v string, amount float64.
func stateEqual(t *testing.T, src, ice [][]any) bool {
	t.Helper()
	if len(src) != len(ice) {
		return false
	}
	for i := range src {
		idOK, _ := ice[i][0].(int64)
		vOK, _ := ice[i][1].(string)
		aOK, _ := ice[i][2].(float64)
		if src[i][0] != idOK || src[i][1] != vOK || src[i][2] != aOK {
			t.Logf("row %d mismatch: source=%v iceberg=%v", i, src[i], ice[i])
			return false
		}
	}
	return true
}
