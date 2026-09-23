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

	ensureNamespace(t, testNS)
	ensureSecret(t, testNS, "pod-e2e-source", map[string]string{
		"uri": "mysql://repl:replpass@mysql.e2e.svc.cluster.local:3306/shop",
	})
	ensureSecret(t, testNS, "pod-e2e-catalog", map[string]string{
		"uri":          "http://polaris.e2e.svc.cluster.local:8181/api/catalog",
		"clientId":     "root",
		"clientSecret": "s3cr3t",
		"scope":        "PRINCIPAL_ROLE:ALL",
	})

	portForward(t, dataNS, "svc/mysql", localMySQLPort, 3306)
	portForward(t, dataNS, "svc/trino", localTrinoPort, 8080)
	mysql := openMySQL(t, localMySQLPort)
	trino := openTrino(t, localTrinoPort)

	// A distinct target keeps every run a fresh snapshot: no committed
	// position to resume from, and no dependence on a prior test's table.
	const target = "raw.pod_smoke_orders"
	seedOrders(t, mysql, 50)

	cr := buildCR("pod-smoke", testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2301",
		[]tableSpec{{Source: "shop.orders", Target: target, PrimaryKey: []string{"id"}, Workers: 1}})
	applyCR(t, cr)
	t.Log("CDCPipeline applied; waiting for the coordinator and worker Pods")

	waitPodsByPrefix(t, testNS, "pod-smoke-", 2, 4*time.Minute)
	t.Log("coordinator + worker Pods Ready")

	waitConverged(t, ctx, trino, "SELECT count(*) FROM pod_smoke_orders", 50, 4*time.Minute)
	assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, "pod_smoke_orders"))
	assertNoRaces(t, testNS, "pod-smoke-")

	t.Log("smoke ok: engine ran as Pods over the pod network, converged exactly, no data races")
}
