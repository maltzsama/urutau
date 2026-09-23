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

	waitPodsByPrefix(t, testNS, "pod-smoke-", 2, 4*time.Minute)
	t.Log("coordinator + worker Pods Ready")

	waitConverged(t, ctx, trino, "SELECT count(*) FROM "+target, 50, 4*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
	assertNoRaces(t, testNS, "pod-smoke-")

	t.Log("smoke ok: engine ran as Pods over the pod network, converged exactly, no data races")
}
