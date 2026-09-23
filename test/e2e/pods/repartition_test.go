package pods

import (
	"context"
	"testing"
	"time"
)

// TestPodScaleOutIn re-slices a table across Pods the way KEDA/manual scaling
// does: it scales the worker StatefulSet and lets the coordinator follow it
// (issue #312). A continuous mixed INSERT/UPDATE/DELETE load runs across both
// flips, and the sink must equal the source exactly afterwards — no key lost,
// no delete resurrected — with the race detector live across the Pods.
func TestPodScaleOutIn(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const (
		pipeline = "pod-scale"
		target   = "raw.pod_scale_orders"
	)
	seedOrders(t, mysql, 200)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2302",
		[]tableSpec{{Source: "shop.orders", Target: target, PrimaryKey: []string{"id"}, Workers: 1}}, crOptions{})
	applyCR(t, cr)
	t.Log("applied; waiting for the coordinator and one worker")

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)
	sts := workerSTSs(t, testNS, pipeline)
	if len(sts) != 1 {
		t.Fatalf("want exactly one worker StatefulSet, got %v", sts)
	}
	worker := sts[0]
	waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	waitConverged(t, ctx, trino, "SELECT count(*) FROM pod_scale_orders", 200, 4*time.Minute)
	t.Log("converged with 1 worker")

	// Continuous mixed load across both re-slices.
	stop := startWriter(t, mysql)

	// Scale OUT 1 -> 3: the coordinator's reconcile loop re-slices to match.
	scaleWorker(t, testNS, worker, 3)
	waitPodsByPrefix(t, testNS, worker+"-", 3, 4*time.Minute)
	t.Log("scaled out to 3 worker Pods; letting the re-slice settle")
	time.Sleep(20 * time.Second) // reconcile interval (5s) + drain

	// Scale IN 3 -> 1.
	scaleWorker(t, testNS, worker, 1)
	waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	t.Log("scaled in to 1 worker Pod; letting the re-slice settle")
	time.Sleep(20 * time.Second)

	stop()

	waitSettled(t, mysql, trino, "pod_scale_orders", 5*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, "pod_scale_orders"))
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("scale-out/in ok: sink equals source exactly, no data races")
}
