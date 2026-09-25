package pods

import (
	"context"
	"testing"
	"time"
)

// TestChaosMeshPodKillAffectsRealPipeline is issue #383's smoke test: it
// proves a real Chaos Mesh PodChaos experiment — not deletePod, not process
// control — kills a real Urutau worker Pod while MySQL -> Iceberg CDC is
// active, and that the pipeline recovers and still converges to the source
// exactly. TestPodCrashRecovery covers the same recovery path via deletePod;
// this test exists to prove the Chaos Mesh path specifically works end to
// end, since #385/#386/#388 build their fault scheduling on top of it.
func TestChaosMeshPodKillAffectsRealPipeline(t *testing.T) {
	requirePods(t)
	verifyChaosMeshReady(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-chaos-mesh-smoke"
	target := uniqueTarget("pod_chaos_mesh_orders")
	seedOrders(t, mysql, 200)

	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2308",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1}}, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)
	t.Log("applied; waiting for the coordinator and one worker")

	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 4*time.Minute)
	sts := workerSTSs(t, testNS, pipeline)
	if len(sts) != 1 {
		t.Fatalf("want exactly one worker StatefulSet, got %v", sts)
	}
	worker := sts[0]
	workerPods := waitPodsByPrefix(t, testNS, worker+"-", 1, 4*time.Minute)
	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 200, 4*time.Minute)
	t.Log("converged; starting the load")

	stop := startWriter(t, mysql, 250*time.Millisecond)

	targetPod := workerPods[0]
	uidBefore := kubectl(t, "-n", testNS, "get", "pod", targetPod, "-o", "jsonpath={.metadata.uid}")

	// A real Chaos Mesh PodChaos experiment, not deletePod: proves Chaos Mesh
	// itself is the mechanism that kills the Pod. pod-kill deletes the Pod
	// outright (like deletePod does) rather than restarting its container, so
	// the StatefulSet recreates it as a new Pod object with the same ordinal
	// name but a new UID — that UID change is the proof the kill was real.
	const experiment = "smoke-pod-kill"
	applyPodChaos(t, testNS, experiment, map[string]string{"urutau.io/worker": worker}, "pod-kill", "")
	t.Cleanup(func() { deleteChaosExperiment(t, testNS, "podchaos", experiment) })
	waitChaosExperimentInjected(t, testNS, "podchaos", experiment, 2*time.Minute)

	workerPods = waitPodsByPrefix(t, testNS, worker+"-", 1, 5*time.Minute)
	uidAfter := kubectl(t, "-n", testNS, "get", "pod", workerPods[0], "-o", "jsonpath={.metadata.uid}")
	if uidAfter == uidBefore {
		t.Fatalf("worker Pod %s UID did not change after PodChaos pod-kill (still %s) — Chaos Mesh never killed it", targetPod, uidBefore)
	}
	t.Log("Chaos Mesh PodChaos killed the real worker Pod")

	deleteChaosExperiment(t, testNS, "podchaos", experiment)
	stop()

	waitSettled(t, mysql, trino, target, 8*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
	assertNoRaces(t, testNS, pipeline+"-")
	t.Log("chaos mesh smoke ok: real PodChaos pod-kill, sink equals source exactly, no races")
}
