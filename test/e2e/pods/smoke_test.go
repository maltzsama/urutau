package pods

import (
	"context"
	"testing"
	"time"
)

// TestPodSmoke is the foundation's proof: the operator provisions the
// coordinator and worker Pods, they talk over the pod network, the snapshot
// converges into Iceberg, and the sink equals the source exactly — with the
// race detector live across the real multi-process topology.
func TestPodSmoke(t *testing.T) {
	requirePods(t)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	mysql, trino := setupPodEnv(t)

	// A unique target keeps every run a fresh snapshot: no committed position
	// to resume from, so a rerun never replays an earlier run's binlog.
	target := uniqueTarget("pod_smoke_orders")
	seedOrders(t, mysql, 50)

	cr := buildCR("pod-smoke", testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2301",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1}}, crOptions{})
	applyPipeline(t, testNS, "pod-smoke", cr)
	t.Log("CDCPipeline applied; waiting for the coordinator and worker Pods")

	// waitPodsByPrefix matches an ordinal Pod name (prefix + integer), which
	// the bare pipeline prefix "pod-smoke-" never is for either the
	// coordinator (…-coordinator-0) or the worker (…-raw-…-0): trimming it
	// leaves "coordinator-0"/"raw-…-0", neither a valid integer, so this
	// always waited out its own timeout even with both Pods Ready. Wait per
	// StatefulSet instead, the pattern every other pod e2e test uses.
	waitPodsByPrefix(t, testNS, "pod-smoke-coordinator-", 1, 4*time.Minute)
	stss := workerSTSs(t, testNS, "pod-smoke")
	if len(stss) != 1 {
		t.Fatalf("want 1 worker StatefulSet, got %v", stss)
	}
	waitPodsByPrefix(t, testNS, stss[0]+"-", 1, 4*time.Minute)
	t.Log("coordinator + worker Pods Ready")

	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 50, 4*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
	assertNoRaces(t, testNS, "pod-smoke-")

	t.Log("smoke ok: engine ran as Pods over the pod network, converged exactly, no data races")
}
