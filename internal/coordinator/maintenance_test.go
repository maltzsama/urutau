package coordinator

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/maltzsama/urutau/internal/observability"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

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

// The maintenance Deployment clones the table's worker pod template and adds
// only the --maintenance flag: the worker connects to the coordinator for its
// assignment, so it needs no table/ops/config on the command line. The
// caller's template must not be mutated — it is reused for the table's data
// worker Deployments.
func TestMaintenanceWorkerDeploymentAddsFlag(t *testing.T) {
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
	dep := maintenanceWorkerDeployment("shop-raw-orders-maint", "raw", owner, tmpl)

	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Errorf("Replicas = %v, want 1", dep.Spec.Replicas)
	}
	if len(dep.OwnerReferences) != 1 || dep.OwnerReferences[0].Name != "coord-pod" {
		t.Errorf("OwnerReferences = %v, want the coordinator pod (GC on pod death)", dep.OwnerReferences)
	}
	ctr := dep.Spec.Template.Spec.Containers[0]
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
