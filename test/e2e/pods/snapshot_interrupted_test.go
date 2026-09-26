package pods

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/faultinject"
)

// TestSnapshotInterruptedAfterStreamCommits is the #428 regression. The live
// stream commits to every table while the snapshot copies them one at a time,
// so a table gets a cdc.position before its own snapshot runs. The
// coordinator is killed right as it starts snapshotting such a table; the
// restarted coordinator must still snapshot it. Before the fix it took the
// position for a finished snapshot, and the table's pre-existing rows never
// reached the sink.
//
// Table A is large with a small chunk size, so its snapshot takes long enough
// to arm the fault; table B receives live writes throughout, so the stream
// commits to it during A's snapshot.
func TestSnapshotInterruptedAfterStreamCommits(t *testing.T) {
	requirePods(t)
	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-snap-interrupted"
	sfx := uniqueTarget("")[1:]
	srcA, srcB := "snapint_a_"+sfx, "snapint_b_"+sfx
	createOrdersLike(t, mysql, srcA, 20000)
	createOrdersLike(t, mysql, srcB, 500)
	t.Cleanup(func() {
		if !t.Failed() {
			_, _ = mysql.Exec("DROP TABLE IF EXISTS " + srcA)
			_, _ = mysql.Exec("DROP TABLE IF EXISTS " + srcB)
		}
	})

	// B's live writes: updates to seeded rows and fresh inserts.
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			_, _ = mysql.Exec("UPDATE "+srcB+" SET v = ? WHERE id = ?", fmt.Sprintf("live%d", i), i%500)
			_, _ = mysql.Exec("INSERT INTO "+srcB+" (id, v, amount) VALUES (?, ?, ?)", 100000+i, fmt.Sprintf("new%d", i), float64(i))
			time.Sleep(100 * time.Millisecond)
		}
	}()
	stopWrites := sync.OnceFunc(func() { stop(); wg.Wait() })
	t.Cleanup(stopWrites)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2330", []tableSpec{
		{Source: "shop." + srcA, Target: "raw." + srcA, PrimaryKey: []string{"id"}, Workers: 1},
		{Source: "shop." + srcB, Target: "raw." + srcB, PrimaryKey: []string{"id"}, Workers: 1},
	}, crOptions{SnapshotChunkSize: 100})
	applyPipeline(t, testNS, pipeline, cr)
	coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)[0]
	logs := followLogs(t, testNS, coord)
	before := podRestarts(t, testNS, coord)
	armFault(t, testNS, coord, faultinject.CoordinatorSnapshotTableStart, "raw."+srcB)
	line := waitFaultFired(t, testNS, coord, logs, faultinject.CoordinatorSnapshotTableStart, before, 10*time.Minute)
	t.Logf("fault fired: %s", line)

	// The scenario needs B to hold a position when the coordinator died:
	// without one, even the old code snapshots it.
	pos, err := committedPosition(context.Background(), trino, srcB)
	if err != nil || pos == "" {
		t.Fatalf("%s had no committed position when the coordinator died (%q, %v): the scenario was not reached", srcB, pos, err)
	}
	t.Logf("%s committed position at the crash: %s", srcB, pos)

	stopWrites()
	waitSettledTables(t, mysql, trino, srcA, srcA, 10*time.Minute)
	waitSettledTables(t, mysql, trino, srcB, srcB, 10*time.Minute)
	assertNoRaces(t, testNS, pipeline+"-")
}

// createOrdersLike creates a source table with the orders shape (id, v,
// amount) and n deterministic rows, inserted in bulk.
func createOrdersLike(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	if _, err := db.Exec("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, v VARCHAR(64) NOT NULL, amount DOUBLE)"); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	const per = 1000
	for lo := 0; lo < n; lo += per {
		hi := min(lo+per, n)
		var b strings.Builder
		args := make([]any, 0, 3*(hi-lo))
		b.WriteString("INSERT INTO " + table + " (id, v, amount) VALUES ")
		for i := lo; i < hi; i++ {
			if i > lo {
				b.WriteString(",")
			}
			b.WriteString("(?, ?, ?)")
			args = append(args, i, fmt.Sprintf("seed%d", i), float64(i))
		}
		if _, err := db.Exec(b.String(), args...); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
}
