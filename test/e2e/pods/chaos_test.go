package pods

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestPodChaosComposition is issue #357: live re-slicing, worker failure, and
// maintenance, composed under continuous load across three tables. Unlike the
// isolated scenarios, the faults overlap — a worker is SIGKILLed and the
// coordinator is restarted while a table is being re-sliced and maintenance
// runs. Every table must converge to its source exactly, with no data races.
func TestPodChaosComposition(t *testing.T) {
	requirePods(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-chaos"

	sources := []string{"chaos_a", "chaos_b", "chaos_c"}
	targets := make([]string, len(sources))
	tables := make([]tableSpec, len(sources))
	for i, src := range sources {
		createChaosTable(t, mysql, src)
		seedTable(t, mysql, src, 100)
		targets[i] = uniqueTarget("pod_chaos_" + src)
		tables[i] = tableSpec{Source: "shop." + src, Target: "raw." + targets[i], PrimaryKey: []string{"id"}, Workers: 1}
	}

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2307", tables, crOptions{Maintenance: true})
	applyPipeline(t, testNS, pipeline, cr)
	t.Log("applied: 3 tables, maintenance enabled")

	// Boot: coordinator + one worker per table.
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	stss := workerSTSs(t, testNS, pipeline)
	if len(stss) != len(sources) {
		t.Fatalf("want %d worker StatefulSets, got %v", len(sources), stss)
	}
	for _, sts := range stss {
		waitPodsByPrefix(t, testNS, sts+"-", 1, 5*time.Minute)
	}
	for i := range sources {
		waitConverged(t, ctx, trino, "SELECT count(*) FROM "+targets[i], 100, 5*time.Minute)
	}
	t.Log("all 3 tables converged")

	// Continuous load on all three across the whole sequence.
	stops := make([]func(), len(sources))
	for i := range sources {
		stops[i] = startWriterOn(t, mysql, sources[i], 150*time.Millisecond)
	}

	// chaos_a (the first worker StatefulSet) is the table that gets re-sliced.
	chaosSTS := stss[0]
	for _, n := range []int{3, 1, 2, 1} {
		scaleWorker(t, testNS, chaosSTS, n)
		waitPodsByPrefix(t, testNS, chaosSTS+"-", n, 5*time.Minute)
		t.Logf("chaos_a re-sliced to %d under load", n)
	}

	// SIGKILL a chaos_a worker mid-load: its session ends owing work.
	pods := waitPodsByPrefix(t, testNS, chaosSTS+"-", 1, 5*time.Minute)
	deletePod(t, testNS, pods[0])
	waitPodsByPrefix(t, testNS, chaosSTS+"-", 1, 6*time.Minute)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
	t.Log("chaos_a worker SIGKILLed and recreated")

	// Restart the coordinator at a non-trivial point.
	coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)[0]
	deletePod(t, testNS, coord)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
	for _, sts := range stss {
		waitPodsByPrefix(t, testNS, sts+"-", 1, 8*time.Minute)
	}
	t.Log("coordinator SIGKILLed and recreated")

	// Stop the writers and converge every table exactly.
	for _, s := range stops {
		s()
	}
	for i := range sources {
		waitSettledTables(t, mysql, trino, sources[i], targets[i], 15*time.Minute)
		assertSinkEqualsSource(t, readTable(t, mysql, sources[i]), readTableSink(t, trino, targets[i]))
	}
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("chaos composition ok: all 3 tables converged, no data races")
}

// createChaosTable creates a source table with the id/v/amount schema the
// generic oracle reads.
func createChaosTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS ` + table + ` (
		id     BIGINT       NOT NULL PRIMARY KEY,
		v      VARCHAR(128) NOT NULL,
		amount DOUBLE       NULL)`); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
}
