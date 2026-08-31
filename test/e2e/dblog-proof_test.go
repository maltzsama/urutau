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
// 500 pre-existing rows are chunked (chunk size 10 → 50 chunks) while update
// bursts race the chunking. A burst committing while the reader still has to
// drain it forces the caught-up wait to hold the current chunk's window open
// — the burst's own events then arrive as InWindow changes whose keys are in
// that chunk's SELECT, so the stale snapshot rows must be discarded. That
// makes DroppedByWindow > 0 hard evidence the window worked; and the final
// Iceberg state must equal the source exactly (invariant 5).
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

	// NewRunner returns only after the snapshot phase, so the bursts below
	// overlap the chunking window by construction. MaxRows is raised so the
	// buffered wave events collapse into few Iceberg commits.
	snapDone := make(chan *runner.Runner, 1)
	runErr := make(chan error, 1)
	// Chunk size 1 stretches the phase to ~500 chunk round trips — long
	// enough to span the reader's dial/handshake and many wave commits —
	// so wave events are guaranteed to decode inside open windows.
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

	// Burst driver: 500-row update waves every 20ms for a fixed 10s window.
	// The reader parses faster than the binlog arrives, so checks pass
	// instantly between waves — but a wave committing inside a chunk's
	// [Buffer, caught-up check] span (~2ms) pends that check for one poll,
	// letting the relay buffer the wave events and release them
	// InWindow-tagged. At ~10% per chunk over 50 chunks, at least one
	// stall — and thus window drops — is all but certain, with no timeout
	// risk since each wave drains long before the next.
	stopBurst := make(chan struct{})
	burstDone := make(chan struct{})
	go func() {
		defer close(burstDone)
		deadline := time.Now().Add(10 * time.Second)
		for i := 0; time.Now().Before(deadline); i++ {
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
			time.Sleep(15 * time.Millisecond)
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

	// Stop the bursts; the source is now quiescent. Drain the stream until
	// the Iceberg state equals the source, row by row.
	close(stopBurst)
	<-burstDone
	drainAndCompare(t, ctx, db)

	// The window proof: at least one stale snapshot row was discarded by an
	// InWindow live event.
	if n := r.DroppedByWindow("raw.orders"); n == 0 {
		t.Fatal("droppedByWindow == 0: the window never discarded a stale row — the proof failed")
	}
	t.Logf("dblog proof ok: droppedByWindow=%d", r.DroppedByWindow("raw.orders"))
}

// drainAndCompare polls until the Iceberg table exactly mirrors the source:
// same id set, same v, same amount for every row.
func drainAndCompare(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for {
		src, err := sourceState(db)
		if err == nil {
			ice, terr := trinoQuery(ctx, `SELECT id, v, amount FROM orders ORDER BY id`)
			if terr == nil && stateEqual(t, src, ice) {
				t.Logf("state converged: %d rows mirrored", len(src))
				return
			}
		}
		if time.Now().After(deadline) {
			src, _ := sourceState(db)
			ice, _ := trinoQuery(ctx, `SELECT id, v, amount FROM orders ORDER BY id`)
			t.Fatalf("state never converged: source=%d rows, iceberg=%d rows", len(src), len(ice))
		}
		time.Sleep(500 * time.Millisecond)
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
