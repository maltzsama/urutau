package pods

import (
	"context"
	"testing"
	"time"
)

// TestPodCrashRecovery kills real Pods while a pipeline runs: a worker Pod and
// then the coordinator Pod, each recreated by its StatefulSet. This is the
// crash the in-process suite could only approximate with a context cancel — a
// SIGKILL runs no deferred cleanup, so the recovery path (supervisor reset,
// resume from the position committed in the sink) is the only thing that can
// save the run. The sink must still equal the source exactly.
func TestPodCrashRecovery(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const (
		pipeline = "pod-crash"
		target   = "raw.pod_crash_orders"
	)
	seedOrders(t, mysql, 200)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2303",
		[]tableSpec{{Source: "shop.orders", Target: target, PrimaryKey: []string{"id"}, Workers: 1}}, crOptions{})
	applyCR(t, cr)
	t.Log("applied; waiting for the coordinator and one worker")

	coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)[0]
	sts := workerSTSs(t, testNS, pipeline)
	if len(sts) != 1 {
		t.Fatalf("want exactly one worker StatefulSet, got %v", sts)
	}
	worker := sts[0]
	workerPods := waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	waitConverged(t, ctx, trino, "SELECT count(*) FROM pod_crash_orders", 200, 4*time.Minute)
	t.Log("converged; starting the load")

	stop := startWriter(t, mysql)

	// 1. SIGKILL the worker Pod mid-load. The StatefulSet recreates it, and
	//    the coordinator's supervisor resets the partition it owned.
	deletePod(t, testNS, workerPods[0])
	waitPodsByPrefix(t, testNS, worker+"-", 1, 5*time.Minute)
	t.Log("worker Pod SIGKILLed and recreated")

	// 2. SIGKILL the coordinator Pod mid-load. It must resume from the
	//    position committed in the sink, not from the start.
	deletePod(t, testNS, coord)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	// The workers reconnect to the new coordinator; give them a moment.
	waitPodsByPrefix(t, testNS, worker+"-", 1, 5*time.Minute)
	t.Log("coordinator Pod SIGKILLed and recreated")

	stop()

	// The sink must converge to the source across both crashes.
	waitSettled(t, mysql, trino, "pod_crash_orders", 8*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, "pod_crash_orders"))
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("crash recovery ok: worker + coordinator SIGKILL, sink equals source exactly, no races")
}
