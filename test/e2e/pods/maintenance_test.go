package pods

import (
	"context"
	"testing"
	"time"
)

// TestPodMaintenance enables background compaction and proves it runs in the
// deployment: the coordinator schedules an ephemeral maintenance worker Pod,
// its compaction counter advances while the pipeline streams, the committed
// position survives, and the sink still equals the source exactly.
func TestPodMaintenance(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-maint"
	target := uniqueTarget("pod_maint_orders")
	seedOrders(t, mysql, 150)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2304",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1}},
		crOptions{Maintenance: true})
	applyPipeline(t, testNS, pipeline, cr)
	t.Log("applied with maintenance enabled")

	coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)[0]
	sts := workerSTSs(t, testNS, pipeline)
	if len(sts) != 1 {
		t.Fatalf("want exactly one worker StatefulSet, got %v", sts)
	}
	waitPodsByPrefix(t, testNS, sts[0]+"-", 1, 4*time.Minute)
	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 150, 4*time.Minute)
	t.Log("converged; generating small files so compaction is due")

	stop := startWriter(t, mysql, 250*time.Millisecond)

	// The coordinator records maintenance metrics even though the maintenance
	// worker is a separate, ephemeral Pod — a compaction run proves the whole
	// path ran in the deployment.
	base := coordinatorMetricsBase(t, testNS, coord)
	waitMetricAbove(t, base, "urutau_iceberg_compaction_runs_total", 0, 4*time.Minute)
	t.Log("compaction ran")

	stop()

	waitSettled(t, mysql, trino, target, 8*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("maintenance ok: compaction ran as a Pod, position survived, sink equals source")
}
