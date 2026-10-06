package pods

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// logFollower streams one Pod's container logs from the moment it starts, so
// a line survives the container restarting — possibly more than once — after
// it was written. kubectl logs --previous only keeps the LAST terminated
// container: a worker fault ends the coordinator's run, which can restart the
// worker a second time and replace the log that held the FAULT line. The
// stream ends when that container exits; the process writes the FAULT line
// before it exits, so the stream has it.
type logFollower struct {
	mu   sync.Mutex
	buf  strings.Builder
	done chan struct{}
}

// followLogs streams pod's container logs until the Pod stops running or the
// test ends. A `kubectl logs -f` stream can drop while the container is still
// alive (a transient kubectl/API hiccup): dismissing it as "the container
// died" fails waitFaultFired before the armed fault is even reached, so the
// follower reconnects while the Pod is Running and re-reads the whole log
// (--tail=-1) to not lose a line written during the gap. Duplicates are
// harmless: line() matches by substring.
func followLogs(t *testing.T, ns, pod string) *logFollower {
	t.Helper()
	f := &logFollower{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(f.done)
		for ctx.Err() == nil {
			cmd := exec.CommandContext(ctx, "kubectl", "-n", ns, "logs", "-f", "--tail=-1", pod)
			cmd.Stdout = f
			cmd.Stderr = f // surface why a stream ended instead of discarding it
			_ = cmd.Run()
			if ctx.Err() != nil || !podRunning(ns, pod) {
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-f.done
	})
	return f
}

// podRunning reports whether pod is still in the Running phase (a restarted
// container keeps the Pod Running; only a real teardown changes the phase).
func podRunning(ns, pod string) bool {
	out, err := exec.Command("kubectl", "-n", ns, "get", "pod", pod, "-o", "jsonpath={.status.phase}").Output()
	return err == nil && strings.TrimSpace(string(out)) == "Running"
}

func (f *logFollower) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(p)
}

// line returns the first streamed line containing want, or "".
func (f *logFollower) line(want string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, l := range strings.Split(f.buf.String(), "\n") {
		if strings.Contains(l, want) {
			return l
		}
	}
	return ""
}

// waitFaultFired waits until the followed container logged the FAULT INJECTED
// line for point and the Pod's container restarted past before (the process
// really died), and returns the line — the diagnostic naming the table, batch
// and positions.
func waitFaultFired(t *testing.T, ns, pod string, logs *logFollower, point faultinject.Point, before int, timeout time.Duration) string {
	t.Helper()
	want := "FAULT INJECTED point=" + string(point)
	deadline := time.Now().Add(timeout)
	for {
		restarts := podRestarts(t, ns, pod)
		if l := logs.line(want); l != "" && restarts > before {
			return l
		}
		select {
		case <-logs.done:
			// The followed container is gone. If it logged the line, the
			// restart count catches up on the next poll; if it did not, it
			// died for another reason and the arm file died with it.
			if logs.line(want) == "" {
				t.Fatalf("fault %s: the armed container in %s exited without firing it (restarts %d, before %d) — it died for another reason first",
					point, pod, restarts, before)
			}
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("fault %s never fired in %s within %s (restarts %d, before %d)",
				point, pod, timeout, restarts, before)
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
			// Follow before arming, so the FAULT line cannot be written
			// before the stream is attached.
			logs := followLogs(t, testNS, pod)
			armFault(t, testNS, pod, bc.point, "raw."+target)
			// Before the wait: a fault that never fires is a failure too.
			reportBoundaryOnFailure(t, mysql, trino, pipeline, bc.point, target)
			stop := startWriter(t, mysql, 250*time.Millisecond)
			line := waitFaultFired(t, testNS, pod, logs, bc.point, before, 4*time.Minute)
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

// The deterministic boundary set of the commit path (commit-boundaries.md):
// the direct path (one owner commits data and cdc.position) and the staged
// path (owners stage, the coordinator commits each cycle).
var (
	directBoundaries = []boundaryCase{
		{point: faultinject.WorkerBatchReceived},
		{point: faultinject.WorkerCommitBefore},
		{point: faultinject.WorkerCommittedBeforeAck},
		{point: faultinject.CoordinatorAckBeforeRecord, coordinator: true},
	}
	stagedBoundaries = []boundaryCase{
		{point: faultinject.WorkerBatchReceived},
		{point: faultinject.WorkerStagedBeforeShip},
		{point: faultinject.WorkerStagedShippedBeforeAck},
		{point: faultinject.CoordinatorCycleBeforeCommit, coordinator: true},
		{point: faultinject.CoordinatorCycleCommittedBeforeRecord, coordinator: true},
	}
)

// startBoundaryPipeline starts a pipeline over shop.orders with workers owners
// and waits until it has converged; it returns the sink table.
func startBoundaryPipeline(t *testing.T, mysql, trino *sql.DB, pipeline, serverID, base string, workers int) string {
	t.Helper()
	target := uniqueTarget(base)
	cr := buildCR(pipeline, testNS, raceImage(), "pod-e2e-source", "pod-e2e-catalog", serverID,
		[]tableSpec{{Source: "shop.orders", Target: "raw." + target, PrimaryKey: []string{"id"}, Workers: workers}}, crOptions{})
	applyPipeline(t, testNS, pipeline, cr)
	waitPodsByPrefix(t, testNS, pipeline+"-coordinator-", 1, 5*time.Minute)
	waitPodsByPrefix(t, testNS, workerSTSs(t, testNS, pipeline)[0]+"-", workers, 5*time.Minute)
	waitSettled(t, mysql, trino, target, 5*time.Minute)
	return target
}

// TestCommitBoundaryFaultsDirect fires every direct-path boundary (one owner:
// the worker commits data and cdc.position itself) and checks the pipeline
// recovers to an exact sink.
func TestCommitBoundaryFaultsDirect(t *testing.T) {
	requirePods(t)
	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-fault-direct"
	seedOrders(t, mysql, 50)
	target := startBoundaryPipeline(t, mysql, trino, pipeline, "2309", "pod_fault_direct", 1)
	runBoundaryCases(t, mysql, trino, pipeline, target, 1, directBoundaries)
	assertNoRaces(t, testNS, pipeline+"-")
}

// TestCommitBoundaryFaultsStaged fires every staged-path boundary (two
// owners: workers stage data files, the coordinator commits each cycle) and
// checks the pipeline recovers to an exact sink.
func TestCommitBoundaryFaultsStaged(t *testing.T) {
	requirePods(t)
	mysql, trino := setupPodEnv(t)
	const pipeline = "pod-fault-staged"
	seedOrders(t, mysql, 50)
	target := startBoundaryPipeline(t, mysql, trino, pipeline, "2310", "pod_fault_staged", 2)
	runBoundaryCases(t, mysql, trino, pipeline, target, 2, stagedBoundaries)
	assertNoRaces(t, testNS, pipeline+"-")
}

// boundaryDraw is one element of the randomized matrix's set.
type boundaryDraw struct {
	path string // direct, staged or snapshot
	bc   boundaryCase
}

// boundarySet is the complete deterministic boundary set the randomized
// matrix draws from.
func boundarySet() []boundaryDraw {
	var set []boundaryDraw
	for _, bc := range directBoundaries {
		set = append(set, boundaryDraw{"direct", bc})
	}
	for _, bc := range stagedBoundaries {
		set = append(set, boundaryDraw{"staged", bc})
	}
	return append(set, boundaryDraw{"snapshot", boundaryCase{point: faultinject.CoordinatorSnapshotTableStart, coordinator: true}})
}

// TestCommitBoundaryRandomized draws boundaries from the whole deterministic
// set — the direct path, the staged path and the snapshot start (P1) — with a
// seeded PRNG (issue #381). The fault is never a matter of timing: each draw
// arms one named point. The seed is logged and URUTAU_E2E_SEED replays the
// same draws; every draw is a subtest named after its boundary, so a failure
// names the exact boundary to reproduce with the deterministic tests.
// URUTAU_E2E_BOUNDARY_DRAWS sets how many (default 3).
func TestCommitBoundaryRandomized(t *testing.T) {
	requirePods(t)
	seed, err := workloadSeed()
	if err != nil {
		t.Fatal(err)
	}
	draws := 3
	if v := os.Getenv("URUTAU_E2E_BOUNDARY_DRAWS"); v != "" {
		if draws, err = strconv.Atoi(v); err != nil || draws < 1 {
			t.Fatalf("URUTAU_E2E_BOUNDARY_DRAWS=%q: want a positive integer", v)
		}
	}
	t.Logf("boundary draws: %d, seed=%d (replay with URUTAU_E2E_SEED=%d)", draws, seed, seed)
	set := boundarySet()

	mysql, trino := setupPodEnv(t)
	seedOrders(t, mysql, 50)
	rng := rand.New(rand.NewPCG(seed, 0xB0DA))
	var direct, staged string // started on their first draw
	for i := range draws {
		d := set[rng.IntN(len(set))]
		t.Logf("draw %d/%d: %s %s", i+1, draws, d.path, d.bc.point)
		switch d.path {
		case "direct":
			if direct == "" {
				direct = startBoundaryPipeline(t, mysql, trino, "pod-fault-rand-direct", "2331", "pod_fault_rand_direct", 1)
			}
			runBoundaryCases(t, mysql, trino, "pod-fault-rand-direct", direct, 1, []boundaryCase{d.bc})
		case "staged":
			if staged == "" {
				staged = startBoundaryPipeline(t, mysql, trino, "pod-fault-rand-staged", "2332", "pod_fault_rand_staged", 2)
			}
			runBoundaryCases(t, mysql, trino, "pod-fault-rand-staged", staged, 2, []boundaryCase{d.bc})
		case "snapshot":
			t.Run(fmt.Sprintf("%s#%d", d.bc.point, i+1), func(t *testing.T) {
				runSnapshotInterrupted(t, mysql, trino, fmt.Sprintf("pod-fault-rand-snap-%d", i+1), strconv.Itoa(2340+i))
			})
		}
		if t.Failed() {
			t.Fatalf("draw %d (%s %s) failed; replay with URUTAU_E2E_SEED=%d or run the deterministic test for that boundary", i+1, d.path, d.bc.point, seed)
		}
	}
	for _, p := range []string{"pod-fault-rand-direct-", "pod-fault-rand-staged-", "pod-fault-rand-snap-"} {
		assertNoRaces(t, testNS, p)
	}
}

// reportBoundaryOnFailure registers, for a failed boundary case, the report
// the matrix owes (issue #381): the boundary, MySQL's executed position and
// each sink table's committed position, logged; and the cluster and Iceberg
// state dumped under URUTAU_E2E_ARTIFACTS. Registered after the pipeline, so
// it runs while the Pods and tables still exist.
func reportBoundaryOnFailure(t *testing.T, mysql, trino *sql.DB, pipeline string, point faultinject.Point, tables ...string) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var executed string
		if err := mysql.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
			executed = "error: " + err.Error()
		}
		t.Logf("boundary %s failed: source gtid_executed=%s", point, executed)
		var prs []*prTable
		for _, tb := range tables {
			pos, err := committedPosition(ctx, trino, tb)
			t.Logf("boundary %s: sink %s cdc.position=%q (err %v)", point, tb, pos, err)
			prs = append(prs, &prTable{Name: tb, Target: tb})
		}
		base := os.Getenv("URUTAU_E2E_ARTIFACTS")
		if base == "" {
			base = filepath.Join(os.TempDir(), "urutau-e2e")
		}
		dir := filepath.Join(base, fmt.Sprintf("commit-boundary-%s-%d", point, time.Now().Unix()))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Logf("artifacts: %v", err)
			return
		}
		dumpDiagnostics(ctx, dir, testNS, pipeline, trino, prs)
		t.Logf("boundary %s: diagnostics under %s", point, dir)
	})
}
