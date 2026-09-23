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
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/trinodb/trino-go-client/trino"
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
	localMySQLPort = 13306
	localTrinoPort = 18080
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

// buildCR renders a CDCPipeline. The source and catalog URIs come from the
// Secrets; everything else lives in definition.inline, exactly as the sample.
func buildCR(name, ns, image, sourceSecret, catalogSecret, serverID string, tables []tableSpec) string {
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
					"sink": map[string]any{
						"type": "iceberg+rest", "namespace": "raw", "warehouse": "quickstart_catalog",
					},
					"tables": rendered,
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

// ── waiting ─────────────────────────────────────────────────────────────

// waitPodsByPrefix polls until exactly want Pods whose name starts with prefix
// exist and every one is Ready, or fails after timeout. It returns their names.
// Prefix matching avoids coupling the harness to the operator's label scheme.
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
	kubectl(t, "-n", ns, "scale", "statefulset", sts, fmt.Sprintf("--replicas=%d", replicas))
}

// deletePod SIGKILLs a Pod by deleting it; the owning StatefulSet recreates it.
// This is the real crash the in-process suite could only fake with a cancel.
func deletePod(t *testing.T, ns, pod string) {
	t.Helper()
	kubectl(t, "-n", ns, "delete", "pod", pod, "--wait=true")
}

// ── logs and races ──────────────────────────────────────────────────────

// podLogs returns a Pod's logs (best effort — a terminating Pod may be gone).
func podLogs(t *testing.T, ns, pod string) string {
	t.Helper()
	return kubectl(t, "-n", ns, "logs", pod, "--tail=-1")
}

// assertNoRaces scans the logs of every Pod with the prefix for the race
// detector's report and fails if one is present. This is the authoritative
// race signal: the engine logs it, and GORACE=halt_on_error makes it fatal.
func assertNoRaces(t *testing.T, ns, prefix string) {
	t.Helper()
	out := kubectl(t, "-n", ns, "get", "pods", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	for _, line := range strings.Split(out, "\n") {
		pod := strings.TrimSpace(line)
		if pod == "" || !strings.HasPrefix(pod, prefix) {
			continue
		}
		logs := podLogs(t, ns, pod)
		if i := strings.Index(logs, "WARNING: DATA RACE"); i >= 0 {
			end := i + 2000
			if end > len(logs) {
				end = len(logs)
			}
			t.Fatalf("data race in %s/%s:\n%s", ns, pod, logs[i:end])
		}
	}
}

// ── data services (port-forward + drivers) ──────────────────────────────

// portForward starts a kubectl port-forward and waits until the local port
// accepts a connection. The process is killed when the test ends.
func portForward(t *testing.T, ns, resource string, local, remote int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "kubectl", "-n", ns, "port-forward", resource,
		fmt.Sprintf("%d:%d", local, remote))
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("port-forward %s %d:%d: %v", resource, local, remote, err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(30 * time.Second)
	addr := fmt.Sprintf("127.0.0.1:%d", local)
	for {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("port-forward %s: %s never came up: %v", resource, addr, err)
		}
		time.Sleep(200 * time.Millisecond)
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

// readOrders reads (id → state) from the source (MySQL shop.orders).
func readOrders(t *testing.T, db *sql.DB) map[int64]rowState {
	t.Helper()
	return readOrdersFrom(t, db, "SELECT id, v, amount FROM orders")
}

// readOrdersSink reads (id → state) from a sink table (Trino, schema raw).
func readOrdersSink(t *testing.T, db *sql.DB, table string) map[int64]rowState {
	t.Helper()
	return readOrdersFrom(t, db, "SELECT id, v, amount FROM "+table)
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
	var missing, extra, wrong []int64
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
	if len(missing) == 0 && len(extra) == 0 && len(wrong) == 0 {
		return
	}
	t.Fatalf("sink != source: source=%d sink=%d missing=%v extra=%v wrongValue=%v",
		len(source), len(sink), cap10(missing), cap10(extra), cap10(wrong))
}

func cap10(ids []int64) []int64 {
	if len(ids) > 10 {
		return ids[:10]
	}
	return ids
}

// ── seeding ─────────────────────────────────────────────────────────────

// seedOrders clears the source and inserts `count` rows 0..count-1 with a
// deterministic v/amount, so the oracle is fully known before the engine runs.
func seedOrders(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	if _, err := db.Exec("DELETE FROM orders"); err != nil {
		t.Fatalf("clear orders: %v", err)
	}
	for i := 0; i < count; i++ {
		if _, err := db.Exec("INSERT INTO orders (id, v, amount) VALUES (?, ?, ?)", i, fmt.Sprintf("seed%d", i), float64(i)); err != nil {
			t.Fatalf("seed orders %d: %v", i, err)
		}
	}
}
