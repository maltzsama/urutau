package pods

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestProductionReadinessWorkload runs the stochastic three-table workload
// (issue #384) against the real MySQL → Iceberg pipeline and checks
// MySQL source state → oracle → Iceberg logical state exactly.
//
// URUTAU_E2E_PROFILE picks smoke (default) or full; URUTAU_E2E_TABLES
// (e.g. "accounts,items") narrows the run to some tables, for debugging only:
// the coverage checks still expect all three. URUTAU_E2E_SEED replays
// a run's random choices (the timing, and so the regime boundaries, still
// varies). The diagnostics file (seed, observed distributions, final diffs)
// is written under URUTAU_E2E_ARTIFACTS, or the system temp dir, pass or fail.
func TestProductionReadinessWorkload(t *testing.T) {
	requirePods(t)
	profile, err := selectedProfile()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := workloadSeed()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("production-readiness workload: profile=%s seed=%d (replay with URUTAU_E2E_SEED=%d)", profile.Name, seed, seed)

	ctx, cancel := context.WithTimeout(context.Background(), profile.Duration+profile.Settle+30*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	suffix := strconv.FormatInt(time.Now().UnixNano()%2176782336, 36)
	tables := selectTables(t, productionTables(suffix, profile))
	w := newWorkload(seed, profile, tables, mysql)
	t.Cleanup(func() {
		rep := w.report()
		if t.Failed() {
			rep.Failure = "see the test log"
		}
		if path, err := writeReport(rep); err != nil {
			t.Logf("write diagnostics: %v", err)
		} else {
			t.Logf("diagnostics: %s", path)
		}
	})

	if err := w.createTables(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// A failed run keeps its source tables, so the rows the diff names
		// can still be read back.
		if t.Failed() {
			t.Logf("keeping source tables %v for inspection", tableNames(tables))
			return
		}
		w.dropTables()
	})
	if err := w.seedInitial(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d rows per table", profile.InitialRows)

	const pipeline = "pod-pr-workload"
	specs := make([]tableSpec, len(tables))
	for i, tb := range tables {
		specs[i] = tableSpec{Source: "shop." + tb.Name, Target: "raw." + tb.Target, PrimaryKey: tb.PK, Workers: tb.Workers, Cast: tb.Cast}
	}
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2320", specs, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)

	// Live mutations start before the coordinator is up, so they overlap
	// the snapshot as well as the stream.
	w.start(ctx)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)
	t.Logf("coordinator up; workload live for %s", profile.Duration)
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(w.liveFrom.Add(profile.Duration))):
	}
	if err := w.stop(ctx); err != nil {
		t.Fatalf("stop workload: %v", err)
	}
	t.Logf("workload stopped; expected source position %s", w.finalPos)

	reconnect := func() error {
		return startPortForward(t, dataNS, "svc/trino", localTrinoPort, 8080, 30*time.Second)
	}
	checks, err := w.settle(ctx, trino, t.Logf, reconnect)
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	w.checks = checks
	for _, tb := range tables {
		rows, bytes, err := sinkCommits(ctx, trino, tb)
		if err != nil {
			t.Errorf("sink commit stats: %v", err)
			rows, bytes = newDist(), newDist()
		}
		w.sinkRows, w.sinkBytes = append(w.sinkRows, rows), append(w.sinkBytes, bytes)
	}

	for i, c := range checks {
		if !c.OracleVsSource.empty() {
			t.Errorf("%s: oracle diverged from MySQL (a workload bug): %s", tables[i].Name, c.OracleVsSource)
		}
		if !c.SourceVsSink.empty() {
			t.Errorf("%s: Iceberg != MySQL: %s", tables[i].Name, c.SourceVsSink)
		}
	}
	for _, p := range w.coverageProblems() {
		t.Errorf("coverage: %s", p)
	}
	w.mu.Lock()
	for _, e := range w.errs {
		t.Errorf("workload: %s", e)
	}
	w.mu.Unlock()
	assertNoRaces(t, testNS, pipeline+"-")
}

// selectTables applies URUTAU_E2E_TABLES, a comma-separated list of table
// kinds (accounts, items, events). Empty keeps all three.
func selectTables(t *testing.T, all []*prTable) []*prTable {
	t.Helper()
	v := os.Getenv("URUTAU_E2E_TABLES")
	if v == "" {
		return all
	}
	var out []*prTable
	for _, k := range strings.Split(v, ",") {
		found := false
		for _, tb := range all {
			if string(tb.Kind) == strings.TrimSpace(k) {
				out, found = append(out, tb), true
			}
		}
		if !found {
			t.Fatalf("URUTAU_E2E_TABLES: unknown table kind %q", k)
		}
	}
	return out
}

func tableNames(tables []*prTable) []string {
	out := make([]string, len(tables))
	for i, tb := range tables {
		out[i] = "shop." + tb.Name
	}
	return out
}
