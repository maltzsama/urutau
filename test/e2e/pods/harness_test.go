// Package pods runs the Urutau engine the way it is deployed: the operator
// provisions the coordinator and worker Pods, and these tests drive them over
// kubectl and the cluster network. Nothing here starts the engine in-process —
// a Pod is the unit under test, and the pod network is the path under test.
//
// The image is race-instrumented (build/Dockerfile.race), so every scenario is
// also a race-detector run across the real multi-process topology.
//
// Gate: URUTAU_E2E_PODS=1, against a cluster brought up by `make e2e-pods-up`.
// These tests do not run in GitHub CI (they need a cluster).
package pods

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/trinodb/trino-go-client/trino"
	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"
)

const (
	// dataNS is the namespace the in-cluster data services run in (test/e2e).
	dataNS = "e2e"
	// testNS is where this suite creates its Secrets and CDCPipelines. The
	// operator watches every namespace (no --watch-namespaces scoping).
	testNS = "pod-e2e"
	// raceImage is the image the operator and the Pods it provisions run.
	// Override with URUTAU_E2E_PODS_IMAGE.
	defaultRaceImage = "urutau:dev-race"

	// Fixed local ports for the data-service port-forwards. Tests run
	// serially, so a fixed pair is simpler than dynamic allocation.
	localMySQLPort    = 13306
	localTrinoPort    = 18080
	localRedpandaPort = 19092
)

func requirePods(t *testing.T) {
	t.Helper()
	if os.Getenv("URUTAU_E2E_PODS") == "" {
		t.Skip("URUTAU_E2E_PODS not set; skipping the pod e2e (needs a cluster from `make e2e-pods-up`)")
	}
}

func raceImage() string {
	if v := os.Getenv("URUTAU_E2E_PODS_IMAGE"); v != "" {
		return v
	}
	return defaultRaceImage
}

// ── kubectl ─────────────────────────────────────────────────────────────

// kubectl runs kubectl and returns trimmed stdout, failing the test on a
// non-zero exit with the captured stderr. Every cluster mutation goes through
// here, so the exact command is visible in a failure.
func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	return kubectlStdin(t, "", args...)
}

func kubectlStdin(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	cmd := exec.Command("kubectl", args...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl %s: %v\nstderr: %s", strings.Join(args, " "), err, errb.String())
	}
	return strings.TrimSpace(out.String())
}

// applyCR applies a manifest from stdin and waits for the API server to accept
// it (the validating webhook runs synchronously).
func applyCR(t *testing.T, manifest string) {
	t.Helper()
	kubectlStdin(t, manifest, "apply", "-f", "-")
}

// applyPipeline applies a CR and registers a cleanup that deletes it. The
// cleanup waits for the CR and its Pods to be gone: the scenarios share the
// shop.orders source, so a lingering pipeline would consume the next test's
// writes and contaminate its sink.
func applyPipeline(t *testing.T, ns, name, manifest string) {
	t.Helper()
	applyCR(t, manifest)
	t.Cleanup(func() {
		_ = exec.Command("kubectl", "-n", ns, "delete", "cdcpipelines", name,
			"--ignore-not-found", "--wait=true").Run()
		waitPodsGone(t, ns, name+"-", 2*time.Minute)
	})
}

// waitPodsGone polls until no Pod with the prefix remains, best effort — a
// cleanup must not fail an already-finished test.
func waitPodsGone(t *testing.T, ns, prefix string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		out, _ := exec.Command("kubectl", "-n", ns, "get", "pods", "-o",
			`jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`).Output()
		remaining := 0
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), prefix) {
				remaining++
			}
		}
		if remaining == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Logf("pods with prefix %q still present after %s", prefix, timeout)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ensureNamespace creates the namespace if it is absent (idempotent).
func ensureNamespace(t *testing.T, ns string) {
	t.Helper()
	y := kubectlStdin(t, "", "create", "namespace", ns, "--dry-run=client", "-o", "yaml")
	kubectlStdin(t, y, "apply", "-f", "-")
}

// ensureSecret creates (or replaces) a generic Secret (idempotent).
func ensureSecret(t *testing.T, ns, name string, kv map[string]string) {
	t.Helper()
	args := []string{"-n", ns, "create", "secret", "generic", name}
	for k, v := range kv {
		args = append(args, "--from-literal="+k+"="+v)
	}
	args = append(args, "--dry-run=client", "-o", "yaml")
	kubectlStdin(t, kubectlStdin(t, "", args...), "apply", "-f", "-")
}

// ── CR building ─────────────────────────────────────────────────────────

type tableSpec struct {
	Source     string
	Target     string
	PrimaryKey []string
	// Workers is the table's workers.number. KEDA scaling (Phase 4) changes
	// the StatefulSet's replicas, not this — this is the boot count.
	Workers int
	// Max, when > 0, sets workers.max so the operator renders a ScaledObject.
	Max int
}

// crOptions are the optional knobs the scenarios toggle.
type crOptions struct {
	// Maintenance enables background Iceberg maintenance (compaction) in the
	// inline sink spec, so an ephemeral maintenance worker Pod is scheduled.
	Maintenance bool
	// MaintenanceInterval is the compaction interval; empty means "1s".
	MaintenanceInterval string
}

// buildCR renders a CDCPipeline. The source and catalog URIs come from the
// Secrets; everything else lives in definition.inline, exactly as the sample.
func buildCR(name, ns, image, sourceSecret, catalogSecret, serverID string, tables []tableSpec, opts crOptions) string {
	rendered := make([]map[string]any, 0, len(tables))
	for _, tbl := range tables {
		t := map[string]any{
			"source":            tbl.Source,
			"target":            tbl.Target,
			"primaryKey":        tbl.PrimaryKey,
			"createIfNotExists": true,
		}
		if tbl.Workers > 0 || tbl.Max > 0 {
			w := map[string]any{}
			if tbl.Workers > 0 {
				w["number"] = tbl.Workers
			}
			if tbl.Max > 0 {
				w["max"] = tbl.Max
			}
			t["workers"] = w
		}
		rendered = append(rendered, t)
	}
	sink := map[string]any{
		"type": "iceberg+rest", "namespace": "raw", "warehouse": "quickstart_catalog",
	}
	if opts.Maintenance {
		// A 1s interval makes compaction due for every pass, so the
		// ephemeral maintenance worker Pod is scheduled promptly.
		interval := opts.MaintenanceInterval
		if interval == "" {
			interval = "1s"
		}
		sink["maintenance"] = map[string]any{
			"enabled":    true,
			"compaction": map[string]any{"minInputFiles": 2, "interval": interval},
		}
	}
	cr := map[string]any{
		"apiVersion": "urutau.io/v1alpha1",
		"kind":       "CDCPipeline",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"image": image,
			"secrets": map[string]any{
				"source":  sourceSecret,
				"catalog": catalogSecret,
			},
			// Race instrumentation roughly doubles the engine's footprint;
			// the defaults are sized for the shipped (non-race) image.
			"coordinator": map[string]any{"cpu": "1", "memory": "2Gi", "metricsAddr": ":8080"},
			"worker": map[string]any{
				"cpu": "500m", "cpu_overhead": "500m",
				"memory": "2Gi", "memory_overhead": "1Gi",
			},
			"definition": map[string]any{
				"inline": map[string]any{
					"pipeline": name,
					"source":   map[string]any{"kind": "mysql", "serverId": serverID},
					"sink":     sink,
					"tables":   rendered,
				},
			},
		},
	}
	b, err := yaml.Marshal(cr)
	if err != nil {
		panic(fmt.Sprintf("marshal CR: %v", err))
	}
	return string(b)
}

// kafkaTableSpec is one append-only Kafka→Iceberg table: a topic (the
// "source") landing into a target with no primary key. Kafka carries no
// before-image on deletes, so append tables must set onDelete: skip
// (spec/validate.go); there is no update/delete concept in this shape at all
// — a raw-format record is always an insert (internal/source/kafka/decoder
// Raw.Decode).
type kafkaTableSpec struct {
	Topic  string
	Target string
}

// buildKafkaCR renders a CDCPipeline whose source is Kafka (format: raw —
// see kafkaTableSpec's doc comment for why raw, not debezium, is the
// baseline shape for the resume/offset matrix). Kafka has no SQL
// introspection, so every table must declare columns: regardless of format
// or extraction (kafka.Source.Introspect fails boot otherwise) — here just
// "payload: string", the one column an undeclared-extraction raw topic
// decodes into (Raw.Decode), so this stays a schemaless, envelope-free
// append log, the shape unique to Kafka among Urutau's sources.
func buildKafkaCR(name, ns, image, sourceSecret, catalogSecret string, tables []kafkaTableSpec) string {
	rendered := make([]map[string]any, 0, len(tables))
	for _, tbl := range tables {
		rendered = append(rendered, map[string]any{
			"source":            tbl.Topic,
			"target":            tbl.Target,
			"writeMode":         "append",
			"onDelete":          "skip",
			"createIfNotExists": true,
			"columns":           map[string]any{"payload": "string"},
		})
	}
	sink := map[string]any{
		"type": "iceberg+rest", "namespace": "raw", "warehouse": "quickstart_catalog",
	}
	cr := map[string]any{
		"apiVersion": "urutau.io/v1alpha1",
		"kind":       "CDCPipeline",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec": map[string]any{
			"image": image,
			"secrets": map[string]any{
				"source":  sourceSecret,
				"catalog": catalogSecret,
			},
			"coordinator": map[string]any{"cpu": "1", "memory": "2Gi", "metricsAddr": ":8080"},
			"worker": map[string]any{
				"cpu": "500m", "cpu_overhead": "500m",
				"memory": "2Gi", "memory_overhead": "1Gi",
			},
			"definition": map[string]any{
				"inline": map[string]any{
					"pipeline": name,
					"source":   map[string]any{"kind": "kafka", "format": "raw"},
					"sink":     sink,
					"tables":   rendered,
				},
			},
		},
	}
	b, err := yaml.Marshal(cr)
	if err != nil {
		panic(fmt.Sprintf("marshal kafka CR: %v", err))
	}
	return string(b)
}

// ── waiting ─────────────────────────────────────────────────────────────

// waitPodsByPrefix polls until exactly want StatefulSet ordinal Pods whose
// name starts with prefix (i.e. "<prefix><n>") exist and every one is Ready, or
// fails after timeout. It returns their names. Prefix matching avoids coupling
// the harness to the operator's label scheme; the numeric-suffix check excludes
// a sibling that shares the prefix, like the ephemeral "<prefix>maint"
// maintenance Pod.
func waitPodsByPrefix(t *testing.T, ns, prefix string, want int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		out := kubectl(t, "-n", ns, "get", "pods", "-o",
			`jsonpath={range .items[*]}{.metadata.name} {.status.phase} {.status.containerStatuses[0].ready}{"\n"}{end}`)
		var ready []string
		for _, line := range strings.Split(out, "\n") {
			f := strings.Fields(line)
			if len(f) != 3 || !strings.HasPrefix(f[0], prefix) {
				continue
			}
			if _, err := strconv.Atoi(strings.TrimPrefix(f[0], prefix)); err != nil {
				continue // not an ordinal Pod (e.g. the "-maint" maintenance Pod)
			}
			if f[1] == "Running" && f[2] == "true" {
				ready = append(ready, f[0])
			}
		}
		last = out
		if len(ready) == want {
			return ready
		}
		if time.Now().After(deadline) {
			t.Fatalf("pods with prefix %q in %s: want %d Ready, got %d within %s\n%s", prefix, ns, want, len(ready), timeout, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// workerSTSs returns the worker StatefulSets of a pipeline (every StatefulSet
// except the coordinator's), read from the cluster so the DNS-sanitized name
// is whatever the operator actually created.
func workerSTSs(t *testing.T, ns, pipeline string) []string {
	t.Helper()
	out := kubectl(t, "-n", ns, "get", "sts", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	var names []string
	for _, line := range strings.Split(out, "\n") {
		name := strings.TrimSpace(line)
		if name == "" || name == pipeline+"-coordinator" || !strings.HasPrefix(name, pipeline+"-") {
			continue
		}
		names = append(names, name)
	}
	return names
}

// waitConverged polls a scalar Trino query until it equals want, or fails.
func waitConverged(t *testing.T, ctx context.Context, trino *sql.DB, query string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		var got int64
		err := trino.QueryRowContext(ctx, query).Scan(&got)
		if err == nil && got == want {
			return
		}
		last = fmt.Sprintf("got=%d err=%v", got, err)
		if time.Now().After(deadline) {
			t.Fatalf("convergence %q: want %d within %s (%s)", query, want, timeout, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ── faults ──────────────────────────────────────────────────────────────

// scaleWorker sets a worker StatefulSet's replicas — the KEDA/manual scale the
// coordinator follows (issue #298). The coordinator re-slices to match.
func scaleWorker(t *testing.T, ns, sts string, replicas int) {
	t.Helper()
	// A race in a Pod about to be scaled away must be caught before its logs
	// vanish with it.
	assertNoRaces(t, ns, sts+"-")
	kubectl(t, "-n", ns, "scale", "statefulset", sts, fmt.Sprintf("--replicas=%d", replicas))
}

// deletePod SIGKILLs a Pod by deleting it; the owning StatefulSet recreates it.
// --grace-period=0 --force sends SIGKILL, so no deferred cleanup runs — the
// real crash the in-process suite could only fake with a cancel.
func deletePod(t *testing.T, ns, pod string) {
	t.Helper()
	// A race in the Pod being killed must be caught before its logs vanish.
	assertNoRacesForPod(t, ns, pod)
	kubectl(t, "-n", ns, "delete", "pod", pod, "--grace-period=0", "--force", "--wait=true")
}

// waitResource polls until `kubectl get <kind> <name> -n ns` succeeds — used
// for the operator-rendered ScaledObject and the HPA KEDA derives from it.
func waitResource(t *testing.T, ns, kind, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if err := exec.Command("kubectl", "-n", ns, "get", kind, name, "-o", "name").Run(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s/%s never appeared in %s within %s", kind, name, ns, timeout)
		}
		time.Sleep(time.Second)
	}
}

// stsReplicas returns a StatefulSet's desired replicas, or -1 on error.
func stsReplicas(t *testing.T, ns, sts string) int {
	t.Helper()
	out := kubectl(t, "-n", ns, "get", "sts", sts, "-o", "jsonpath={.spec.replicas}")
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return -1
	}
	return n
}

// waitSTSReplicasAbove polls until a StatefulSet's replicas exceed want — the
// observable effect of KEDA scaling it.
func waitSTSReplicasAbove(t *testing.T, ns, sts string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	for {
		last = stsReplicas(t, ns, sts)
		if last > want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("StatefulSet %s replicas never exceeded %d within %s (last=%d)", sts, want, timeout, last)
		}
		time.Sleep(2 * time.Second)
	}
}

// startHeavyWriter sustains a fast insert load so the per-table backlog stays
// above KEDA's threshold long enough for a scale-up. Stop with the returned
// function; the source remains the oracle for the final comparison.
func startHeavyWriter(t *testing.T, db *sql.DB) (stop func()) {
	t.Helper()
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stopCh:
				return
			default:
			}
			id := int64(100000 + i)
			if _, err := db.Exec("INSERT INTO orders (id, v, amount) VALUES (?, ?, ?)", id, fmt.Sprintf("burst%d", i), float64(i)); err != nil {
				t.Logf("heavy writer: %v", err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			close(stopCh)
			<-done
		})
	}
	t.Cleanup(stop)
	return stop
}

// ── logs and races ──────────────────────────────────────────────────────

// kubectlLogsBestEffort returns a Pod's logs across all its containers, best
// effort: a Pod still creating, or already gone, has none — not a test failure.
func kubectlLogsBestEffort(ns, pod string, previous bool) string {
	args := []string{"-n", ns, "logs", pod, "--all-containers", "--tail=-1"}
	if previous {
		args = append(args, "--previous")
	}
	cmd := exec.Command("kubectl", args...)
	var out strings.Builder
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return ""
	}
	return out.String()
}

// assertNoRacesForPod scans one Pod's current and previous container logs for
// the race detector's report and fails if one is present.
func assertNoRacesForPod(t *testing.T, ns, pod string) {
	t.Helper()
	logs := kubectlLogsBestEffort(ns, pod, false) + "\n" + kubectlLogsBestEffort(ns, pod, true)
	if i := strings.Index(logs, "WARNING: DATA RACE"); i >= 0 {
		end := i + 2000
		if end > len(logs) {
			end = len(logs)
		}
		t.Fatalf("data race in %s/%s:\n%s", ns, pod, logs[i:end])
	}
}

// assertNoRaces scans every surviving Pod with the prefix for a race. Pods
// deliberately deleted or scaled away are scanned at their deletion sites
// (deletePod, scaleWorker) before their logs vanish.
func assertNoRaces(t *testing.T, ns, prefix string) {
	t.Helper()
	out := kubectl(t, "-n", ns, "get", "pods", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	for _, line := range strings.Split(out, "\n") {
		pod := strings.TrimSpace(line)
		if pod == "" || !strings.HasPrefix(pod, prefix) {
			continue
		}
		assertNoRacesForPod(t, ns, pod)
	}
}

// ── data services (port-forward + drivers) ──────────────────────────────

// forwardStates tracks the running port-forward per local port, so a re-forward
// after the target Pod restarted mid-test (a coordinator run restart drops the
// old connection) releases the port before rebinding instead of colliding with
// its dead predecessor.
type forwardState struct{ cancel context.CancelFunc }

var (
	forwardMu     sync.Mutex
	forwardStates = map[int]*forwardState{}
)

// portForward starts a kubectl port-forward and waits until the local port
// accepts a connection. The process is killed when the test ends. Calling it
// again for the same local port replaces the previous forward.
func portForward(t *testing.T, ns, resource string, local, remote int) {
	t.Helper()
	if err := startPortForward(t, ns, resource, local, remote, 30*time.Second); err != nil {
		t.Fatal(err)
	}
}

// startPortForward is portForward returning its failure instead of failing the
// test, so a caller that polls through a Pod restart can retry within its own
// deadline. The forward must accept a connection within wait; on failure the
// kubectl process is stopped.
func startPortForward(t *testing.T, ns, resource string, local, remote int, wait time.Duration) error {
	t.Helper()
	releasePort(local)
	ctx, cancel := context.WithCancel(context.Background())
	st := &forwardState{cancel: cancel}
	cmd := exec.CommandContext(ctx, "kubectl", "-n", ns, "port-forward", resource,
		fmt.Sprintf("%d:%d", local, remote))
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("port-forward %s %d:%d: %w", resource, local, remote, err)
	}
	forwardMu.Lock()
	forwardStates[local] = st
	forwardMu.Unlock()
	stop := func() {
		forwardMu.Lock()
		if forwardStates[local] == st {
			delete(forwardStates, local)
		}
		forwardMu.Unlock()
		cancel()
		_ = cmd.Wait()
	}
	deadline := time.Now().Add(wait)
	addr := fmt.Sprintf("127.0.0.1:%d", local)
	for {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Cleanup(stop)
			return nil
		}
		if time.Now().After(deadline) {
			stop()
			return fmt.Errorf("port-forward %s: %s never came up within %s: %w", resource, addr, wait, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// releasePort cancels any forward on local and waits for the port to free, so a
// replacement forward can bind it.
func releasePort(local int) {
	forwardMu.Lock()
	st := forwardStates[local]
	delete(forwardStates, local)
	forwardMu.Unlock()
	if st == nil {
		return
	}
	st.cancel()
	addr := fmt.Sprintf("127.0.0.1:%d", local)
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(100 * time.Millisecond)
	}
}

// coordinatorMetricsBase port-forwards the coordinator Pod's metrics port and
// returns the base URL. Call once per test (the local port is fixed).
func coordinatorMetricsBase(t *testing.T, ns, coordPod string) string {
	t.Helper()
	portForward(t, ns, "pod/"+coordPod, coordinatorMetricsPort, 8080)
	return coordinatorMetricsURL
}

const (
	coordinatorMetricsPort = 19091
	coordinatorMetricsURL  = "http://127.0.0.1:19091"
)

// statuszWorkerNames fetches /statusz and returns the coordinator's registered
// worker names — the owner layout the re-slice mutates, which the source/sink
// convergence check alone does not prove. An error means the forward dropped
// (the coordinator restarted under the fault); the caller re-forwards.
func statuszWorkerNames(base string) ([]string, error) {
	resp, err := http.Get(base + "/statusz")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var st struct {
		Workers map[string]json.RawMessage `json:"workers"`
	}
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(st.Workers))
	for n := range st.Workers {
		names = append(names, n)
	}
	return names, nil
}

// waitOwnerCount polls /statusz until the coordinator holds exactly want
// registered workers whose name carries the worker StatefulSet prefix
// (sts+"-"), proving the coordinator re-sliced to match the replica count
// rather than merely the Pods becoming Ready. A coordinator restart under the
// re-slice drops the metrics port-forward, so it re-forwards on read errors.
func waitOwnerCount(t *testing.T, ns, coordPod, sts string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	forwarded := false
	got := -1
	var lastErr error
	for {
		if !forwarded {
			wait := min(time.Until(deadline), 30*time.Second)
			if err := startPortForward(t, ns, "pod/"+coordPod, coordinatorMetricsPort, 8080, wait); err != nil {
				lastErr = err
				t.Logf("coordinator metrics forward failed (retrying): %v", err)
			} else {
				forwarded = true
			}
		}
		if forwarded {
			names, err := statuszWorkerNames(coordinatorMetricsURL)
			if err != nil {
				lastErr = err
				forwarded = false // the coordinator restarted under the fault: re-forward
				t.Logf("statusz read failed (re-forwarding): %v", err)
			} else {
				got = 0
				for _, n := range names {
					if strings.HasPrefix(n, sts+"-") {
						got++
					}
				}
				if got == want {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("coordinator owner count for %s = %d, want %d within %s (last error: %v)", sts, got, want, timeout, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// metricValue fetches /metrics and returns the value of the first sample whose
// metric name matches, or -1 when absent.
func metricValue(t *testing.T, base, metric string) float64 {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, metric) {
			continue
		}
		f := strings.Fields(line)
		if len(f) >= 2 {
			if v, err := strconv.ParseFloat(f[len(f)-1], 64); err == nil {
				return v
			}
		}
	}
	return -1
}

// waitMetricAbove polls the coordinator's metrics until the named metric
// exceeds want, or fails after timeout.
func waitMetricAbove(t *testing.T, base, metric string, want float64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last float64
	for {
		last = metricValue(t, base, metric)
		if last > want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric %s never exceeded %v within %s (last=%v)", metric, want, timeout, last)
		}
		time.Sleep(time.Second)
	}
}

// openMySQL dials the in-cluster MySQL through a port-forward.
func openMySQL(t *testing.T, port int) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("root:rootpass@tcp(127.0.0.1:%d)/shop?parseTime=true", port))
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping mysql: %v", err)
	}
	return db
}

// openTrino dials the in-cluster Trino through a port-forward.
func openTrino(t *testing.T, port int) *sql.DB {
	t.Helper()
	db, err := sql.Open("trino", fmt.Sprintf("http://user@127.0.0.1:%d?catalog=iceberg&schema=raw", port))
	if err != nil {
		t.Fatalf("open trino: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping trino: %v", err)
	}
	return db
}

// ── the oracle ──────────────────────────────────────────────────────────

// rowState is one row's full value state for the exact-map comparison.
type rowState struct {
	V      string
	Amount sql.NullFloat64
}

// readTable reads (id → state) from any source table.
func readTable(t *testing.T, db *sql.DB, table string) map[int64]rowState {
	t.Helper()
	return readOrdersFrom(t, db, "SELECT id, v, amount FROM "+table)
}

// readTableSink reads (id → state) from any sink table (Trino, schema raw).
func readTableSink(t *testing.T, db *sql.DB, table string) map[int64]rowState {
	t.Helper()
	return readOrdersFrom(t, db, "SELECT id, v, amount FROM "+table)
}

// readOrders reads (id → state) from the source (MySQL shop.orders).
func readOrders(t *testing.T, db *sql.DB) map[int64]rowState {
	t.Helper()
	return readTable(t, db, "orders")
}

// readOrdersSink reads (id → state) from a sink table (Trino, schema raw).
func readOrdersSink(t *testing.T, db *sql.DB, table string) map[int64]rowState {
	t.Helper()
	return readTableSink(t, db, table)
}

func readOrdersFrom(t *testing.T, db *sql.DB, query string) map[int64]rowState {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("read %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]rowState{}
	for rows.Next() {
		var id int64
		var st rowState
		if err := rows.Scan(&id, &st.V, &st.Amount); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		out[id] = st
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %q: %v", query, err)
	}
	return out
}

// assertSinkEqualsSource is the pass condition: the sink's exact key→state map
// must equal the source's. Never a COUNT(*) — a count hides one row lost and
// one duplicated.
func assertSinkEqualsSource(t *testing.T, source, sink map[int64]rowState) {
	t.Helper()
	missing, extra, wrong := diffSinkSource(source, sink)
	if len(missing) == 0 && len(extra) == 0 && len(wrong) == 0 {
		return
	}
	t.Fatalf("sink != source: source=%d sink=%d missing=%v extra=%v wrongValue=%v",
		len(source), len(sink), cap10(missing), cap10(extra), cap10(wrong))
}

// diffSinkSource returns the ids the sink is missing, has extra (a resurrected
// delete), or holds with a stale value, relative to the source.
func diffSinkSource(source, sink map[int64]rowState) (missing, extra, wrong []int64) {
	for id, want := range source {
		got, ok := sink[id]
		switch {
		case !ok:
			missing = append(missing, id)
		case got != want:
			wrong = append(wrong, id)
		}
	}
	for id := range sink {
		if _, ok := source[id]; !ok {
			extra = append(extra, id)
		}
	}
	return missing, extra, wrong
}

// waitSettled polls until the sink's exact state equals the source's, or fails
// with the residual diff. It is the convergence wait for a live writer.
func waitSettled(t *testing.T, mysql, trino *sql.DB, table string, timeout time.Duration) {
	t.Helper()
	waitSettledTables(t, mysql, trino, "orders", table, timeout)
}

// waitSettledTables is waitSettled for an arbitrary source→sink table pair.
func waitSettledTables(t *testing.T, mysql, trino *sql.DB, srcTable, sinkTable string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		src := readTable(t, mysql, srcTable)
		sink := readTableSink(t, trino, sinkTable)
		missing, extra, wrong := diffSinkSource(src, sink)
		if len(missing) == 0 && len(extra) == 0 && len(wrong) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink %s never settled to source %s: source=%d sink=%d missing=%v extra=%v wrongValue=%v",
				sinkTable, srcTable, len(src), len(sink), cap10(missing), cap10(extra), cap10(wrong))
		}
		time.Sleep(time.Second)
	}
}

func cap10(ids []int64) []int64 {
	if len(ids) > 10 {
		return ids[:10]
	}
	return ids
}

// ── seeding ─────────────────────────────────────────────────────────────

// uniqueTarget returns a fresh sink table name for one test run. A fixed name
// would resume from a previous run's committed position and replay all the
// binlog since — including other tests' writes — instead of snapshotting the
// reseeded source, so the sink would diverge. A unique name forces a fresh
// snapshot every run. The spec target is "raw." + this. The suffix is short
// so the derived worker StatefulSet name stays well under the 63-byte label
// limit once its controller-revision-hash suffix is appended.
func uniqueTarget(base string) string {
	return fmt.Sprintf("%s_%s", base, strconv.FormatInt(time.Now().UnixNano()%2176782336, 36))
}

// seedTable clears a source table and inserts `count` rows 0..count-1 with a
// deterministic v/amount, so the oracle is fully known before the engine runs.
func seedTable(t *testing.T, db *sql.DB, table string, count int) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM " + table); err != nil {
		t.Fatalf("clear %s: %v", table, err)
	}
	for i := 0; i < count; i++ {
		if _, err := db.Exec("INSERT INTO "+table+" (id, v, amount) VALUES (?, ?, ?)", i, fmt.Sprintf("seed%d", i), float64(i)); err != nil {
			t.Fatalf("seed %s %d: %v", table, i, err)
		}
	}
}

// seedOrders clears the source and inserts `count` rows 0..count-1.
func seedOrders(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	seedTable(t, db, "orders", count)
}

// setupPodEnv is the shared fixture: the test namespace, the source/catalog
// Secrets, port-forwards to the in-cluster data services, and open drivers.
func setupPodEnv(t *testing.T) (mysql, trino *sql.DB) {
	t.Helper()
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
	return openMySQL(t, localMySQLPort), openTrino(t, localTrinoPort)
}

// setupPodEnvKafka is setupPodEnv for a Kafka-sourced pipeline: the Kafka
// source Secret (a broker address, same "uri" shape the operator already
// validates — see internal/operator/controller.go validateSecrets), the
// catalog Secret, a port-forward to the in-cluster Redpanda, and an open
// producer client. The Redpanda broker's OUTSIDE listener is what the
// port-forward reaches (test/e2e/k8s/redpanda.yaml), matching the
// docker-compose overlay's 19092 mapping.
func setupPodEnvKafka(t *testing.T) (produce *kgo.Client, trino *sql.DB) {
	t.Helper()
	ensureNamespace(t, testNS)
	ensureSecret(t, testNS, "pod-e2e-source-kafka", map[string]string{
		"uri": "redpanda.e2e.svc.cluster.local:9092",
	})
	ensureSecret(t, testNS, "pod-e2e-catalog", map[string]string{
		"uri":          "http://polaris.e2e.svc.cluster.local:8181/api/catalog",
		"clientId":     "root",
		"clientSecret": "s3cr3t",
		"scope":        "PRINCIPAL_ROLE:ALL",
	})
	portForward(t, dataNS, "svc/redpanda", localRedpandaPort, 19092)
	portForward(t, dataNS, "svc/trino", localTrinoPort, 8080)
	return openKafkaProducer(t, localRedpandaPort), openTrino(t, localTrinoPort)
}

// openKafkaProducer dials the in-cluster Redpanda's OUTSIDE listener through
// a port-forward, for the test to produce records the pipeline consumes.
func openKafkaProducer(t *testing.T, port int) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("127.0.0.1:%d", port)),
		// Redpanda's auto_create_topics_enabled only fires for a metadata
		// request that explicitly asks for it — the client library does not
		// set that flag by default, so producing to a topic this test just
		// invented (uniqueTarget's per-run suffix) failed every record with
		// UNKNOWN_TOPIC_OR_PARTITION until this was added (issue #394).
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("kafka producer: %v", err)
	}
	t.Cleanup(client.Close)
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("ping redpanda: %v", err)
	}
	return client
}

// produceRawRecords synchronously produces n records of {"seq": N} JSON
// payloads (N from start, start+1, ...) to topic, returning the seqs
// actually acknowledged by the broker. Using JSON keeps the payload
// self-describing for the oracle comparison without requiring a columns:
// declaration on the pipeline side (format: raw with no per-topic
// extraction lands the whole payload string verbatim in one column).
func produceRawRecords(t *testing.T, client *kgo.Client, topic string, start, n int) []int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	seqs := make([]int, 0, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var failed []error
	for i := 0; i < n; i++ {
		seq := start + i
		body, err := json.Marshal(map[string]int{"seq": seq})
		if err != nil {
			t.Fatalf("marshal seq %d: %v", seq, err)
		}
		wg.Add(1)
		client.Produce(ctx, &kgo.Record{Topic: topic, Value: body}, func(_ *kgo.Record, err error) {
			defer wg.Done()
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				// t.Fatal cannot be called from this callback — it does not
				// run on the test goroutine — so a produce failure is
				// collected here and fails the test AFTER wg.Wait() returns
				// below. Returning only the acknowledged seqs (silently
				// dropping the failed ones) would make an all-failed batch
				// pass the oracle vacuously: an empty "produced" set has
				// nothing missing from the sink (issue #394 caught this the
				// hard way — a topic-not-yet-created race looked like a
				// clean pass).
				failed = append(failed, fmt.Errorf("seq %d: %w", seq, err))
				return
			}
			seqs = append(seqs, seq)
		})
	}
	wg.Wait()
	if len(failed) > 0 {
		t.Fatalf("produce %s: %d/%d records failed, first: %v", topic, len(failed), n, failed[0])
	}
	return seqs
}

// readKafkaRawSink reads every produced seq present in the sink table (one
// row per delivered record, column "payload" holding the JSON string) and
// returns how many times each seq appears — at-least-once means a seq can
// repeat, but every produced seq must be present at least once.
func readKafkaRawSink(t *testing.T, db *sql.DB, table string) map[int]int {
	t.Helper()
	rows, err := db.Query("SELECT payload FROM " + table)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int]int{}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			t.Fatalf("scan %s: %v", table, err)
		}
		var v struct {
			Seq int `json:"seq"`
		}
		if err := json.Unmarshal([]byte(payload), &v); err != nil {
			t.Fatalf("unmarshal payload %q: %v", payload, err)
		}
		out[v.Seq]++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %s: %v", table, err)
	}
	return out
}

// waitKafkaSettled polls until every produced seq is present at least once in
// the sink, or fails with the residual diff. Duplicate delivery (a seq
// present more than once) is not a failure — the produced set must merely be
// a subset of what actually landed.
func waitKafkaSettled(t *testing.T, trino *sql.DB, table string, produced []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		sink := readKafkaRawSink(t, trino, table)
		var missing []int
		for _, seq := range produced {
			if sink[seq] == 0 {
				missing = append(missing, seq)
			}
		}
		if len(missing) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink %s never settled: produced=%d sink=%d missing=%v",
				table, len(produced), len(sink), cap10Int(missing))
		}
		time.Sleep(time.Second)
	}
}

func cap10Int(ids []int) []int {
	if len(ids) > 10 {
		return ids[:10]
	}
	return ids
}

// startWriter runs a mixed INSERT/UPDATE/DELETE load against shop.orders.
func startWriter(t *testing.T, db *sql.DB, interval time.Duration) (stop func()) {
	t.Helper()
	return startWriterOn(t, db, "orders", interval)
}

// startWriterOn runs a mixed INSERT/UPDATE/DELETE load against any source table
// until the returned stop function is called. The source is the oracle, so the
// exact operations do not need to be tracked — a row the engine loses shows up
// as a key missing from the sink, and a resurrected delete as an extra key. The
// interval must keep the writer slower than the race-instrumented worker
// commits (roughly one row per commit here), or the sink lags past the settle.
func startWriterOn(t *testing.T, db *sql.DB, table string, interval time.Duration) (stop func()) {
	t.Helper()
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stopCh:
				return
			default:
			}
			var q string
			var args []any
			switch i % 3 {
			case 0: // monotonic insert: extends the key range
				id := int64(1000 + i/3)
				q, args = "INSERT INTO "+table+" (id, v, amount) VALUES (?, ?, ?)", []any{id, fmt.Sprintf("ins%d", i), float64(id)}
			case 1: // update an existing key
				id := int64((i / 3) % 200)
				q, args = "UPDATE "+table+" SET v = ? WHERE id = ?", []any{fmt.Sprintf("upd%d", i), id}
			case 2: // delete a key, which must not be resurrected
				id := int64(100 + (i/3)%50)
				q, args = "DELETE FROM "+table+" WHERE id = ?", []any{id}
			}
			if _, err := db.Exec(q, args...); err != nil {
				t.Logf("writer %q: %v", q, err)
			}
			time.Sleep(interval)
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			close(stopCh)
			<-done
		})
	}
	// Stop the writer before the test's DB cleanup closes the connection.
	t.Cleanup(stop)
	return stop
}
