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
	const pipeline = "pod-scale"
	target := uniqueTarget("pod_scale_orders")
	seedOrders(t, mysql, 200)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2302",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1}}, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)
	t.Log("applied; waiting for the coordinator and one worker")

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)
	sts := workerSTSs(t, testNS, pipeline)
	if len(sts) != 1 {
		t.Fatalf("want exactly one worker StatefulSet, got %v", sts)
	}
	worker := sts[0]
	waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 200, 4*time.Minute)
	t.Log("converged with 1 worker")

	// Mixed load while we scale OUT, then drained before scaling IN: a
	// re-slice flips only once the table owes nothing, and under a growing
	// backlog the drain cannot converge inside the coordinator's drain timeout
	// (the documented barrier limit). Scale-out under load is the interesting
	// direction; scale-in is validated once the backlog has drained.
	stop := startWriter(t, mysql, 400*time.Millisecond)

	// Scale OUT 1 -> 3: the coordinator's reconcile loop re-slices to match.
	scaleWorker(t, testNS, worker, 3)
	waitPodsByPrefix(t, testNS, worker+"-", 3, 4*time.Minute)
	t.Log("scaled out to 3 worker Pods under load; letting the re-slice settle")
	time.Sleep(30 * time.Second) // reconcile interval (5s) + drain

	// Stop the load and converge before scaling in.
	stop()
	waitSettled(t, mysql, trino, target, 8*time.Minute)
	t.Log("converged after scale-out; scaling in with the backlog drained")

	// Scale IN 3 -> 1.
	scaleWorker(t, testNS, worker, 1)
	waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	t.Log("scaled in to 1 worker Pod; letting the re-slice settle")

	waitSettled(t, mysql, trino, target, 8*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("scale-out/in ok: sink equals source exactly, no data races")
}

// TestPodScaleInUnderLoad is issue #363: scaling in while the workers still owe
// batches deletes their Pods mid-flight, stranding the queued batches in the
// coordinator. The drain for the re-slice then waits on them forever and the
// flip never commits. The coordinator must recover — fail the run for a clean
// replay and resume from the committed position — not stall.
func TestPodScaleInUnderLoad(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-scalein"
	target := uniqueTarget("pod_scalein_orders")
	seedOrders(t, mysql, 200)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2306",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1}}, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)
	sts := workerSTSs(t, testNS, pipeline)
	if len(sts) != 1 {
		t.Fatalf("want exactly one worker StatefulSet, got %v", sts)
	}
	worker := sts[0]
	waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 200, 4*time.Minute)

	// Scale out, then in, all under continuous load: the scaled-away workers
	// are mid-flight when their Pods are deleted, which is the #363 trigger.
	stop := startWriter(t, mysql, 150*time.Millisecond)
	scaleWorker(t, testNS, worker, 3)
	waitPodsByPrefix(t, testNS, worker+"-", 3, 4*time.Minute)
	time.Sleep(15 * time.Second)
	t.Log("scaled out to 3 under load; scaling in")
	scaleWorker(t, testNS, worker, 1)
	waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)

	// The coordinator may have failed the run for a clean replay (issue #363);
	// its StatefulSet restarts it. Wait for the coordinator and its worker to be
	// Ready again before draining the writer.
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
	waitPodsByPrefix(t, testNS, worker+"-", 1, 8*time.Minute)
	t.Log("coordinator + worker Ready after the scale-in")

	stop()

	waitSettled(t, mysql, trino, target, 10*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("scale-in under load ok: recovered from a lost worker, sink equals source, no races")
}
