package pods

import (
	"context"
	"testing"
	"time"
)

// Pure scale-out (1→3) under load, then converge — isolates the scale-out
// path from the scale-in, asserting the coordinator's owner count (issue #372).
func TestScaleOutConvergence(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const pipeline = "scale-out"
	src := "chaos_a"
	createChaosTable(t, mysql, src)
	seedTable(t, mysql, src, 100)
	target := uniqueTarget("scale_out_" + src)
	tables := []tableSpec{{Source: "shop." + src, Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1}}
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2308", tables, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)

	coordPods := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	base := coordinatorMetricsBase(t, testNS, coordPods[0])
	stss := workerSTSs(t, testNS, pipeline)
	for _, sts := range stss {
		waitPodsByPrefix(t, testNS, sts+"-", 1, 5*time.Minute)
	}
	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 100, 5*time.Minute)

	stop := startWriterOn(t, mysql, src, 300*time.Millisecond)
	scaleWorker(t, testNS, stss[0], 3)
	waitPodsByPrefix(t, testNS, stss[0]+"-", 3, 5*time.Minute)
	waitOwnerCount(t, base, stss[0], 3, 5*time.Minute)
	t.Logf("scaled out to 3")
	time.Sleep(15 * time.Second)
	stop()
	waitSettledTables(t, mysql, trino, src, target, 15*time.Minute)
	t.Log("converged")
}
