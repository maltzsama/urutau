package coordinator

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/maltzsama/urutau/internal/observability"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/spec"
)

// fakeSessionStream is a scripted UrutauControl_SessionServer: Recv returns
// the queued worker messages in order, and Send records what the coordinator
// pushed.
//
// Once the queue is drained Recv models the real maintenance worker, which
// blocks in Recv waiting for an assignment and only returns when the
// coordinator ends the session. blockWhenDrained makes that explicit: with
// it set, a coordinator that waits on Recv instead of dismissing the worker
// deadlocks, which is the zombie-Pod bug rather than a passing test.
type fakeSessionStream struct {
	in               []*pb.WorkerMessage
	sent             []*pb.CoordinatorMessage
	ctx              context.Context
	blockWhenDrained bool
}

func (f *fakeSessionStream) Recv() (*pb.WorkerMessage, error) {
	if len(f.in) == 0 {
		if f.blockWhenDrained {
			<-make(chan struct{}) // a real worker parks here until dismissed
		}
		return nil, io.EOF
	}
	msg := f.in[0]
	f.in = f.in[1:]
	return msg, nil
}

func (f *fakeSessionStream) Send(m *pb.CoordinatorMessage) error {
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeSessionStream) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func (f *fakeSessionStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeSessionStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeSessionStream) SetTrailer(metadata.MD)       {}
func (f *fakeSessionStream) SendMsg(any) error            { return nil }
func (f *fakeSessionStream) RecvMsg(any) error            { return nil }

// testScheduler builds a maintenance scheduler with compaction enabled (so
// exactly one operation is due on the first Due call) and a fake Kubernetes
// client, so Pod provisioning and deletion are observable.
func testScheduler(t *testing.T) (*maintenanceScheduler, *fake.Clientset) {
	t.Helper()
	cs := fake.NewSimpleClientset()
	m := newMaintenanceScheduler(
		&Coordinator{log: slog.New(slog.DiscardHandler)},
		&spec.Maintenance{Enabled: true, Compaction: &spec.CompactionConfig{Interval: "1h"}},
	)
	m.k8s, m.namespace = cs, "ns"
	return m, cs
}

func TestMaintenanceWorkerNameSanitizes(t *testing.T) {
	cases := []struct {
		pipeline, table, want string
	}{
		{"shop", "raw.orders", "shop-raw-orders-maint"},
		{"Shop_Prod", "Raw.Orders", "shop-prod-raw-orders-maint"},
	}
	for _, c := range cases {
		if got := maintenanceWorkerName(c.pipeline, c.table); got != c.want {
			t.Errorf("maintenanceWorkerName(%q, %q) = %q, want %q", c.pipeline, c.table, got, c.want)
		}
	}
	long := maintenanceWorkerName(strings.Repeat("p", 80), "orders")
	if len(long) > 63 {
		t.Errorf("maintenanceWorkerName = %d chars, want <= 63 (DNS-1123)", len(long))
	}
	if strings.HasSuffix(long, "-") || strings.HasPrefix(long, "-") {
		t.Errorf("maintenanceWorkerName = %q, must not start/end with a dash", long)
	}
}

// The maintenance Pod clones the table's worker pod template, sets
// restartPolicy: Never (issue #105 — a Deployment's pod always restarts,
// which made a one-shot maintenance worker restart-loop forever instead of
// terminating), and adds the --maintenance flag: the worker connects to the
// coordinator for its assignment, so it needs no table/ops/config on the
// command line. The caller's template must not be mutated — it is reused
// for the table's data worker Deployments.
func TestMaintenanceWorkerPodIsEphemeral(t *testing.T) {
	tmpl := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    "worker",
				Image:   "urutau:test",
				Command: []string{"urutau-worker", "run", "--coordinator", "coord:50051"},
			}},
		},
	}
	owner := metav1.OwnerReference{Name: "coord-pod", Kind: "Pod"}
	pod := maintenanceWorkerPod("shop-raw-orders-maint", "raw", owner, tmpl)

	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("RestartPolicy = %v, want Never — a one-shot worker must not be restarted", pod.Spec.RestartPolicy)
	}
	if len(pod.OwnerReferences) != 1 || pod.OwnerReferences[0].Name != "coord-pod" {
		t.Errorf("OwnerReferences = %v, want the coordinator pod (GC on pod death)", pod.OwnerReferences)
	}
	ctr := pod.Spec.Containers[0]
	if !hasArg(ctr.Args, "--maintenance") {
		t.Errorf("args = %v, want --maintenance", ctr.Args)
	}
	if !hasArgPair(ctr.Args, "--name", "shop-raw-orders-maint") {
		t.Errorf("args = %v, want --name shop-raw-orders-maint", ctr.Args)
	}
	if len(tmpl.Spec.Containers[0].Args) != 0 {
		t.Errorf("input template was mutated: args = %v", tmpl.Spec.Containers[0].Args)
	}
}

func hasArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func hasArgPair(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

// The whole point of issue #105: a worker serves exactly one pass and its
// Pod is deleted, so the pod terminates instead of being restarted into a
// crash loop.
func TestSessionDeletesPodAfterOnePass(t *testing.T) {
	m, cs := testScheduler(t)
	m.register("w", "raw.orders")
	m.active["w"] = true
	if _, err := cs.CoreV1().Pods("ns").Create(context.Background(),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns"}},
		metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed pod: %v", err)
	}

	stream := &fakeSessionStream{in: []*pb.WorkerMessage{{
		Msg: &pb.WorkerMessage_MaintenanceResult{MaintenanceResult: &pb.MaintenanceResult{}},
	}}}
	if err := m.session(stream, &pb.Hello{WorkerName: "w", Maintenance: true}); err != nil {
		t.Fatalf("session: %v", err)
	}

	if len(stream.sent) != 1 || stream.sent[0].GetMaintenance() == nil {
		t.Fatalf("sent = %v, want exactly one MaintenanceAssignment", stream.sent)
	}
	if _, err := cs.CoreV1().Pods("ns").Get(context.Background(), "w", metav1.GetOptions{}); err == nil {
		t.Fatal("pod still exists after the pass; it must be deleted so the worker terminates")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active["w"] || m.connected["w"] {
		t.Fatalf("active=%v connected=%v, both must be cleared so the next turn can provision", m.active["w"], m.connected["w"])
	}
}

// A worker that connects with nothing due must be dismissed, not left
// blocked in Recv forever. Leaving it connected would pin active[name] and
// silently stop every future maintenance turn for that table.
func TestSessionDismissesWorkerWithNothingDue(t *testing.T) {
	m, cs := testScheduler(t)
	m.register("w", "raw.orders")
	m.active["w"] = true
	if _, err := cs.CoreV1().Pods("ns").Create(context.Background(),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "ns"}},
		metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed pod: %v", err)
	}
	// Everything already ran, so nothing is due for this worker.
	m.sched.MarkRun("raw.orders", m.sched.Due("raw.orders", m.cfg, time.Now()), time.Now())

	// blockWhenDrained: this worker never sends anything, exactly like a real
	// one parked in Recv. The coordinator must dismiss it on its own.
	stream := &fakeSessionStream{blockWhenDrained: true}
	done := make(chan error, 1)
	go func() { done <- m.session(stream, &pb.Hello{WorkerName: "w", Maintenance: true}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("session: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session never returned: the worker is parked in Recv with nothing due, so its Pod would linger and pin the table forever")
	}

	if len(stream.sent) != 0 {
		t.Fatalf("sent = %v, want no assignment when nothing is due", stream.sent)
	}
	if _, err := cs.CoreV1().Pods("ns").Get(context.Background(), "w", metav1.GetOptions{}); err == nil {
		t.Fatal("dismissed worker's pod still exists; it must be deleted")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active["w"] || m.connected["w"] {
		t.Fatal("dismissal must free the table, otherwise maintenance stops for good")
	}
}

// MarkRun happens on the worker's reported result, not when the assignment
// is sent: a worker killed mid-pass (OOM, node drain) must leave its
// operations due so the next turn retries them.
func TestSessionLeavesOpsDueWhenPassNeverReports(t *testing.T) {
	m, _ := testScheduler(t)
	m.register("w", "raw.orders")

	// No MaintenanceResult queued: the worker died after receiving the
	// assignment.
	if err := m.session(&fakeSessionStream{}, &pb.Hello{WorkerName: "w", Maintenance: true}); err != nil {
		t.Fatalf("session: %v", err)
	}

	if due := m.sched.Due("raw.orders", m.cfg, time.Now()); len(due) == 0 {
		t.Fatal("operations were marked run despite the pass never reporting a result")
	}
}

// A reported pass does mark the operations as run, so the next tick does not
// immediately provision another worker for the same turn.
func TestSessionMarksOpsRunOnReportedResult(t *testing.T) {
	m, _ := testScheduler(t)
	m.register("w", "raw.orders")

	stream := &fakeSessionStream{in: []*pb.WorkerMessage{{
		Msg: &pb.WorkerMessage_MaintenanceResult{MaintenanceResult: &pb.MaintenanceResult{}},
	}}}
	if err := m.session(stream, &pb.Hello{WorkerName: "w", Maintenance: true}); err != nil {
		t.Fatalf("session: %v", err)
	}

	if due := m.sched.Due("raw.orders", m.cfg, time.Now()); len(due) != 0 {
		t.Fatalf("due = %v after a reported pass, want none until the interval elapses", due)
	}
}

// tick provisions a Pod per due table, and only one: a table whose worker is
// still in flight is skipped rather than piling up duplicate Pods.
func TestTickProvisionsOnePodPerDueTableAndNoDuplicates(t *testing.T) {
	m, cs := testScheduler(t)
	m.register("w", "raw.orders")
	// loadWorkerPodTemplate reads the operator-rendered template from disk,
	// which is absent here, so provisioning fails and active is rolled back —
	// that rollback is exactly what must happen on a provisioning error.
	m.tick(time.Now())
	m.mu.Lock()
	stillActive := m.active["w"]
	m.mu.Unlock()
	if stillActive {
		t.Fatal("a failed provision must clear active, otherwise the table is stuck forever")
	}
	pods, err := cs.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods = %d, want 0 when the template is missing", len(pods.Items))
	}

	// With a worker already in flight, tick must not provision at all.
	m.mu.Lock()
	m.active["w"] = true
	m.mu.Unlock()
	m.tick(time.Now())
	pods, err = cs.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("pods = %d, want 0 while a pass is in flight", len(pods.Items))
	}
}

// Without Kubernetes provisioning (no worker pod template, so no client)
// tick is a no-op rather than a nil-pointer panic.
func TestTickWithoutKubernetesIsANoop(t *testing.T) {
	m := newMaintenanceScheduler(
		&Coordinator{log: slog.New(slog.DiscardHandler)},
		&spec.Maintenance{Enabled: true, Compaction: &spec.CompactionConfig{Interval: "1h"}},
	)
	m.register("w", "raw.orders")
	m.tick(time.Now())
	if due := m.sched.Due("raw.orders", m.cfg, time.Now()); len(due) == 0 {
		t.Fatal("tick must not consume the due turn when it cannot provision")
	}
}

// recordResult folds a maintenance worker's reported pass into the
// coordinator's registry — the worker is ephemeral, so this is the only place
// the counts survive to a scrape.
func TestRecordResultFoldsIntoRegistry(t *testing.T) {
	metrics := observability.New()
	m := &maintenanceScheduler{c: &Coordinator{metrics: metrics}}

	m.recordResult("raw.orders", &pb.MaintenanceResult{Ops: []*pb.MaintenanceOpResult{
		{Op: &pb.MaintenanceOpResult_Compaction{Compaction: &pb.CompactionResult{
			FilesRemoved: 3, FilesAdded: 1, BytesBefore: 100, BytesAfter: 40}}},
		{Op: &pb.MaintenanceOpResult_Expiry{Expiry: &pb.ExpiryResult{SnapshotsRemoved: 2}}},
		{Op: &pb.MaintenanceOpResult_Orphan{Orphan: &pb.OrphanResult{FilesDeleted: 5, BytesFreed: 500}}},
	}})

	cases := []struct {
		name string
		got  float64
		want float64
	}{
		{"compaction runs", testutil.ToFloat64(metrics.IcebergCompactionRuns), 1},
		{"compaction files removed", testutil.ToFloat64(metrics.IcebergCompactionFilesRemoved), 3},
		{"compaction files added", testutil.ToFloat64(metrics.IcebergCompactionFilesAdded), 1},
		{"compaction bytes before", testutil.ToFloat64(metrics.IcebergCompactionBytesBefore), 100},
		{"compaction bytes after", testutil.ToFloat64(metrics.IcebergCompactionBytesAfter), 40},
		{"expiry runs", testutil.ToFloat64(metrics.IcebergSnapshotExpiryRuns), 1},
		{"expiry snapshots", testutil.ToFloat64(metrics.IcebergSnapshotExpirySnapshots), 2},
		{"orphan runs", testutil.ToFloat64(metrics.IcebergOrphanCleanupRuns), 1},
		{"orphan files", testutil.ToFloat64(metrics.IcebergOrphanCleanupFiles), 5},
		{"orphan bytes", testutil.ToFloat64(metrics.IcebergOrphanCleanupBytes), 500},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}

// A failed operation still counts as a run — that counter is what makes a
// stalled maintainer visible — but records no counts.
func TestRecordResultFailedOpCountsRunOnly(t *testing.T) {
	metrics := observability.New()
	m := &maintenanceScheduler{c: &Coordinator{metrics: metrics}}
	m.recordResult("t", &pb.MaintenanceResult{Ops: []*pb.MaintenanceOpResult{
		{Op: &pb.MaintenanceOpResult_Compaction{Compaction: &pb.CompactionResult{FilesRemoved: 9, Error: "boom"}}},
	}})

	if got := testutil.ToFloat64(metrics.IcebergCompactionRuns); got != 1 {
		t.Errorf("compaction runs = %v, want 1", got)
	}
	if n := testutil.CollectAndCount(metrics.IcebergCompactionFilesRemoved); n != 0 {
		t.Errorf("failed compaction recorded %d files-removed series, want 0", n)
	}
}

// A nil registry (no --metrics-addr) must be a silent no-op, not a panic.
func TestRecordResultNilRegistryNoop(t *testing.T) {
	m := &maintenanceScheduler{c: &Coordinator{}}
	m.recordResult("t", &pb.MaintenanceResult{Ops: []*pb.MaintenanceOpResult{
		{Op: &pb.MaintenanceOpResult_Compaction{Compaction: &pb.CompactionResult{FilesRemoved: 1}}},
	}})
}
