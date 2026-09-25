package pods

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/faultinject"
)

// The commit-boundary fault points (internal/faultinject) are compiled into
// the race e2e image only. These helpers arm one in a real Pod and wait for it
// to fire; the recovery each boundary must survive is documented in
// website/docs/architecture/commit-boundaries.md.

// armFault arms point in pod: the next time that process reaches the boundary
// for table (empty: any table), it SIGKILLs itself. The arm file lives in the
// container's /tmp, so the restarted container starts unarmed.
func armFault(t *testing.T, ns, pod string, point faultinject.Point, table string) {
	t.Helper()
	body := "point=" + string(point) + "\n"
	if table != "" {
		body += "table=" + table + "\n"
	}
	// The body travels on stdin: a shell-quoted argument would reach the
	// file with literal "\n" sequences and fail to parse.
	kubectlStdin(t, body, "-n", ns, "exec", "-i", pod, "--", "sh", "-c", "cat > "+faultinject.DefaultFile)
}

// podRestarts returns the pod's first container restart count.
func podRestarts(t *testing.T, ns, pod string) int {
	t.Helper()
	out := strings.TrimSpace(kubectl(t, "-n", ns, "get", "pod", pod, "-o",
		"jsonpath={.status.containerStatuses[0].restartCount}"))
	n, err := strconv.Atoi(out)
	if err != nil {
		t.Fatalf("restart count of %s: %q: %v", pod, out, err)
	}
	return n
}

// waitFaultFired waits until pod's container restarted past before and its
// previous container logged the FAULT INJECTED line for point, and returns
// that line — the diagnostic naming the table, batch and positions. The Pod
// must also come back Ready, since the recovery runs on the restarted
// container.
func waitFaultFired(t *testing.T, ns, pod string, point faultinject.Point, before int, timeout time.Duration) string {
	t.Helper()
	want := "FAULT INJECTED point=" + string(point)
	deadline := time.Now().Add(timeout)
	for {
		if podRestarts(t, ns, pod) > before {
			for _, line := range strings.Split(kubectlLogsBestEffort(ns, pod, true), "\n") {
				if strings.Contains(line, want) {
					return line
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("fault %s never fired in %s within %s (restarts %d, before %d)",
				point, pod, timeout, podRestarts(t, ns, pod), before)
		}
		time.Sleep(time.Second)
	}
}

// boundaryCase is one fault point and which Pod runs it.
type boundaryCase struct {
	point       faultinject.Point
	coordinator bool // the point runs in the coordinator; otherwise a worker
}

// runBoundaryCases drives one pipeline through each case in turn: arm the
// point, write load until it fires (the Pod restarts), then stop the load and
// require the sink to converge to the source exactly. Each case is a clean
// crash at a known step — the recovery path that follows is what the
// crash-recovery matrix (#354) checks.
func runBoundaryCases(t *testing.T, mysql, trino *sql.DB, pipeline, target string, workers int, cases []boundaryCase) {
	t.Helper()
	for _, bc := range cases {
		t.Run(string(bc.point), func(t *testing.T) {
			coord := waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)[0]
			stss := workerSTSs(t, testNS, pipeline)
			// The first ordinal: on a staged table every owner reaches the
			// worker-side boundaries, so any one of them exercises the point.
			pod := waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)[0]
			if bc.coordinator {
				pod = coord
			}

			before := podRestarts(t, testNS, pod)
			armFault(t, testNS, pod, bc.point, "raw."+target)
			stop := startWriter(t, mysql, 250*time.Millisecond)
			line := waitFaultFired(t, testNS, pod, bc.point, before, 4*time.Minute)
			stop()
			t.Logf("fired: %s", strings.TrimSpace(line))
			if !strings.Contains(line, "table=raw."+target) {
				t.Fatalf("diagnostic line does not name the table: %q", line)
			}

			waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 8*time.Minute)
			waitPodsByPrefix(t, testNS, stss[0]+"-", workers, 8*time.Minute)
			waitSettled(t, mysql, trino, target, 10*time.Minute)
			assertSinkEqualsSource(t, readOrders(t, mysql), readOrdersSink(t, trino, target))
		})
	}
}

// TestCommitBoundaryFaultsDirect fires every direct-path boundary (one owner:
// the worker commits data and cdc.position itself) and checks the pipeline
// recovers to an exact sink.
func TestCommitBoundaryFaultsDirect(t *testing.T) {
	requirePods(t)
	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-fault-direct"
	target := uniqueTarget("pod_fault_direct")
	seedOrders(t, mysql, 50)
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2309",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 1}}, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	waitPodsByPrefix(t, testNS, workerSTSs(t, testNS, pipeline)[0]+"-", 1, 5*time.Minute)
	waitSettled(t, mysql, trino, target, 5*time.Minute)

	runBoundaryCases(t, mysql, trino, pipeline, target, 1, []boundaryCase{
		{point: faultinject.WorkerBatchReceived},
		{point: faultinject.WorkerCommitBefore},
		{point: faultinject.IcebergUpsertBetweenDeleteAndAppend},
		{point: faultinject.WorkerCommittedBeforeAck},
		{point: faultinject.CoordinatorAckBeforeRecord, coordinator: true},
	})
	assertNoRaces(t, testNS, pipeline+"-")
}

// TestCommitBoundaryFaultsStaged fires every staged-path boundary (two
// owners: workers stage data files, the coordinator commits each cycle) and
// checks the pipeline recovers to an exact sink.
func TestCommitBoundaryFaultsStaged(t *testing.T) {
	requirePods(t)
	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-fault-staged"
	target := uniqueTarget("pod_fault_staged")
	seedOrders(t, mysql, 50)
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", "2310",
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: 2}}, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	waitPodsByPrefix(t, testNS, workerSTSs(t, testNS, pipeline)[0]+"-", 2, 5*time.Minute)
	waitSettled(t, mysql, trino, target, 5*time.Minute)

	runBoundaryCases(t, mysql, trino, pipeline, target, 2, []boundaryCase{
		{point: faultinject.WorkerBatchReceived},
		{point: faultinject.WorkerStagedBeforeShip},
		{point: faultinject.WorkerStagedShippedBeforeAck},
		{point: faultinject.CoordinatorCycleBeforeCommit, coordinator: true},
		{point: faultinject.CoordinatorCycleCommittedBeforeRecord, coordinator: true},
	})
	assertNoRaces(t, testNS, pipeline+"-")
}
