package pods

import (
	"context"
	"testing"
	"time"
)

// TEMP repro for the multi-table re-slice stall (not committed): three tables
// under continuous load, re-slicing one repeatedly, then converge.
func TestMultiTableResliceConvergence(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const pipeline = "mt-reslice"
	sources := []string{"chaos_a", "chaos_b", "chaos_c"}
	targets := make([]string, len(sources))
	tables := make([]tableSpec, len(sources))
	for i, src := range sources {
		createChaosTable(t, mysql, src)
		seedTable(t, mysql, src, 100)
		targets[i] = uniqueTarget("mt_reslice_" + src)
		tables[i] = tableSpec{Source: "shop." + src, Target: "raw." + targets[i], PrimaryKey: []string{"id"}, Workers: 1}
	}
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2308", tables, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)
	t.Logf("targets: %v", targets)

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	stss := workerSTSs(t, testNS, pipeline)
	for _, sts := range stss {
		waitPodsByPrefix(t, testNS, sts+"-", 1, 5*time.Minute)
	}
	for i := range sources {
		waitConverged(t, ctx, trino, "SELECT count(*) FROM "+targets[i], 100, 5*time.Minute)
	}
	stops := make([]func(), len(sources))
	for i := range sources {
		stops[i] = startWriterOn(t, mysql, sources[i], 300*time.Millisecond)
	}
	chaosSTS := stss[0]
	for _, n := range []int{3, 1, 2, 1} {
		scaleWorker(t, testNS, chaosSTS, n)
		waitPodsByPrefix(t, testNS, chaosSTS+"-", n, 5*time.Minute)
		t.Logf("re-sliced to %d", n)
	}
	for _, s := range stops {
		s()
	}
	for i := range sources {
		waitSettledTables(t, mysql, trino, sources[i], targets[i], 15*time.Minute)
	}
	t.Log("converged")
}
