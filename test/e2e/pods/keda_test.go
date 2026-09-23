package pods

import (
	"context"
	"testing"
	"time"
)

// TestPodKEDAScaling proves the autoscaling path in the deployment: the
// operator renders a KEDA ScaledObject for a table that sets workers.max, KEDA
// derives an HPA from it, and a sustained backlog on
// urutau_coordinator_pending_batches scales the worker StatefulSet above its
// baseline — which the coordinator then follows (issue #298). The sink must
// still equal the source exactly, with no data races.
func TestPodKEDAScaling(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-keda"
	target := uniqueTarget("pod_keda_orders")
	seedOrders(t, mysql, 200)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2305",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1, Max: 4}},
		crOptions{})
	applyPipeline(t, testNS, pipeline, cr)
	t.Log("applied with workers.max=4")

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)
	sts := workerSTSs(t, testNS, pipeline)
	if len(sts) != 1 {
		t.Fatalf("want exactly one worker StatefulSet, got %v", sts)
	}
	worker := sts[0]
	waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 200, 4*time.Minute)

	// The operator renders a ScaledObject; KEDA turns it into an HPA.
	waitResource(t, testNS, "scaledobject", worker, time.Minute)
	waitResource(t, testNS, "hpa", "keda-hpa-"+worker, 2*time.Minute)
	t.Log("ScaledObject rendered and KEDA HPA created")

	// A sustained backlog makes KEDA scale the worker StatefulSet above the
	// baseline; the coordinator follows the replica count.
	stop := startHeavyWriter(t, mysql)
	waitSTSReplicasAbove(t, testNS, worker, 1, 8*time.Minute)
	t.Log("KEDA scaled the worker StatefulSet above 1")
	stop()

	waitSettled(t, mysql, trino, target, 10*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("keda ok: ScaledObject -> HPA -> scale-up, sink equals source")
}
