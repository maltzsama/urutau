package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/maintenance"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// maintenanceScheduler owns the ephemeral maintenance workers, one per
// table. Unlike the data workers, a maintenance worker is not kept running:
// tick provisions a bare Pod (restartPolicy: Never) for a table only when a
// turn is due, and session deletes that Pod once the worker's pass ends —
// so the pod that shows up in `kubectl get pods` exists only for the
// duration of one pass, not forever. The coordinator never runs maintenance
// in its own process, and never supervises these workers for acks (they
// have no data plane).
type maintenanceScheduler struct {
	c     *Coordinator
	cfg   *spec.Maintenance
	sched *maintenance.Schedule

	// k8s is nil when Kubernetes worker provisioning is off (no worker pod
	// template) — tick then has nothing to provision and is a no-op.
	k8s       kubernetes.Interface
	namespace string
	owner     metav1.OwnerReference

	mu        sync.Mutex
	tables    map[string]string // worker name -> table
	active    map[string]bool   // worker name -> Pod provisioned, not yet cleaned up
	connected map[string]bool   // worker name -> session in progress
}

func newMaintenanceScheduler(c *Coordinator, cfg *spec.Maintenance) *maintenanceScheduler {
	return &maintenanceScheduler{
		c:         c,
		cfg:       cfg,
		sched:     maintenance.NewSchedule(),
		tables:    map[string]string{},
		active:    map[string]bool{},
		connected: map[string]bool{},
	}
}

// startMaintenance wires the maintenance scheduler. It registers one
// maintenance worker per table and starts the tick loop that provisions an
// ephemeral Pod for a table whenever its next turn comes due. Without
// Kubernetes (no worker pod template) it still registers the workers but
// there is nothing to launch — the collapsed runner's in-process path is
// the only option there, and it is not this code.
//
// Metrics: the pass runs in the worker's process, which is ephemeral, so the
// worker reports the outcome back over the control stream and recordResult
// folds it into the coordinator's long-lived registry.
func (c *Coordinator) startMaintenance(refs []core.TableRef, workerTarget map[string]string) error {
	if !c.cfg.Spec.Sink.MaintenanceEnabled() {
		return nil
	}
	if _, ok := c.snk.(sink.Maintainable); !ok {
		return fmt.Errorf("coordinator: sink %q does not support table maintenance (sink.maintenance is Iceberg-only)", c.cfg.Spec.Sink.Type)
	}
	m := newMaintenanceScheduler(c, c.cfg.Spec.Sink.Maintenance)
	for _, ref := range refs {
		m.register(maintenanceWorkerName(c.cfg.Spec.Pipeline, ref.Target), ref.Target)
	}
	c.maint = m
	if !workerPodTemplateAvailable(workerTarget) {
		c.log.Warn("coordinator: maintenance enabled but no worker pod template — no maintenance worker will run (Kubernetes provisioning is off)")
		return nil
	}
	clientset, ns, owner, err := inClusterClient(context.Background())
	if err != nil {
		return fmt.Errorf("k8s maintenance provisioning: %w", err)
	}
	m.k8s, m.namespace, m.owner = clientset, ns, owner
	go m.run(c.runCtx)
	return nil
}

// maintenanceSession routes a maintenance worker's Hello to the scheduler.
// A maintenance worker that connects when maintenance is disabled (or was
// never configured) is rejected rather than served a data assignment.
func (c *Coordinator) maintenanceSession(stream pb.UrutauControl_SessionServer, hello *pb.Hello) error {
	if c.maint == nil {
		return fmt.Errorf("coordinator: maintenance worker %q connected but maintenance is disabled", hello.WorkerName)
	}
	return c.maint.session(stream, hello)
}

// register records a maintenance worker name and the table it maintains.
func (m *maintenanceScheduler) register(name, table string) {
	m.mu.Lock()
	m.tables[name] = table
	m.mu.Unlock()
}

// session serves one connected maintenance worker for exactly one pass: it
// hands over the operations due for its table, records the result the worker
// reports, and returns when the worker closes the stream (it exits after the
// pass). The worker's Pod is deleted on the way out, so it terminates
// instead of being restarted.
//
// A worker that finds nothing due is dismissed immediately rather than left
// blocked in Recv: its Pod only exists because tick saw a turn due, so
// nothing due here means the turn was already served (a duplicate Pod, or a
// coordinator restart that lost the in-memory schedule). Returning ends the
// session, the worker exits, and the deferred cleanup frees the table to be
// provisioned again — leaving it connected would pin active[name] forever
// and silently stop all further maintenance for that table.
func (m *maintenanceScheduler) session(stream pb.UrutauControl_SessionServer, hello *pb.Hello) error {
	name := hello.WorkerName
	m.mu.Lock()
	table, known := m.tables[name]
	if !known {
		m.mu.Unlock()
		return fmt.Errorf("coordinator: unknown maintenance worker %q", name)
	}
	if m.connected[name] {
		m.mu.Unlock()
		return fmt.Errorf("coordinator: maintenance worker %q already connected", name)
	}
	m.connected[name] = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.connected, name)
		delete(m.active, name)
		m.mu.Unlock()
		// The worker exits as soon as this session ends; delete its Pod so it
		// terminates instead of lingering.
		if m.k8s != nil {
			if err := deletePod(context.Background(), m.k8s, m.namespace, name); err != nil {
				m.c.log.Warn("coordinator: maintenance: delete worker pod", "worker", name, "err", err)
			}
		}
	}()
	m.c.log.Info("coordinator: maintenance worker connected", "worker", name, "table", table)

	// This Pod exists only because tick saw a turn due, so serve it now
	// rather than making a freshly-started worker idle until the next tick.
	m.mu.Lock()
	due := m.sched.Due(table, m.cfg, time.Now())
	m.mu.Unlock()
	if len(due) == 0 {
		m.c.log.Info("coordinator: maintenance: nothing due, dismissing worker", "worker", name, "table", table)
		return nil
	}
	msg, err := m.assignment(table, due)
	if err != nil {
		return fmt.Errorf("coordinator: maintenance: build assignment for %s: %w", table, err)
	}
	if err := stream.Send(msg); err != nil {
		return err
	}

	// The worker reports a MaintenanceResult when the pass ends, then closes
	// the stream. MarkRun happens on that report, not on the send above: a
	// worker that dies mid-pass (OOM, node drain) must leave the operations
	// due rather than have them counted as run.
	for {
		in, err := stream.Recv()
		if err != nil {
			return nil // the worker finished (or died); either way it is gone
		}
		if res := in.GetMaintenanceResult(); res != nil {
			m.recordResult(table, res)
			m.mu.Lock()
			m.sched.MarkRun(table, due, time.Now())
			m.mu.Unlock()
		}
	}
}

// run provisions ephemeral maintenance workers for due turns until ctx is
// done.
func (m *maintenanceScheduler) run(ctx context.Context) {
	ticker := time.NewTicker(maintenance.CheckInterval(m.cfg))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.tick(now)
		}
	}
}

// tick provisions one ephemeral Pod per table whose next turn has come due
// and that has no worker in flight already. Handing over the assignment is
// session's job, not tick's — the worker gets it the moment it connects.
// A table whose previous pass is still running is skipped; its operations
// stay due, so nothing is lost.
func (m *maintenanceScheduler) tick(now time.Time) {
	if m.k8s == nil {
		return
	}
	m.mu.Lock()
	type toProvision struct{ name, table string }
	var provision []toProvision
	for name, table := range m.tables {
		if m.active[name] || m.connected[name] {
			continue
		}
		if len(m.sched.Due(table, m.cfg, now)) == 0 {
			continue
		}
		m.active[name] = true
		provision = append(provision, toProvision{name, table})
	}
	m.mu.Unlock()

	for _, p := range provision {
		if err := m.provisionWorkerPod(p.name, p.table); err != nil {
			m.c.log.Warn("coordinator: maintenance: provision worker pod", "worker", p.name, "table", p.table, "err", err)
			m.mu.Lock()
			delete(m.active, p.name)
			m.mu.Unlock()
		}
	}
}

// provisionWorkerPod creates the ephemeral Pod for one due table's
// maintenance worker. The Pod connects to the coordinator on its own, is
// served its assignment by tick (or session, if it races the next tick),
// and is deleted by session once its pass ends.
func (m *maintenanceScheduler) provisionWorkerPod(name, table string) error {
	tmpl, err := loadWorkerPodTemplate(table)
	if err != nil {
		return err
	}
	pod := maintenanceWorkerPod(name, m.namespace, m.owner, tmpl)
	if err := createPod(context.Background(), m.k8s, m.namespace, pod); err != nil {
		return err
	}
	m.c.log.Info("coordinator: maintenance worker pod provisioned", "worker", name, "table", table)
	return nil
}

// assignment renders one table's due operations into a wire assignment. The
// config carries the operation parameters; the intervals are ignored by the
// worker because the coordinator already decided what is due.
func (m *maintenanceScheduler) assignment(table string, ops []sink.MaintenanceOp) (*pb.CoordinatorMessage, error) {
	opNames := make([]string, len(ops))
	for i, o := range ops {
		opNames[i] = string(o)
	}
	cfgJSON, err := json.Marshal(m.cfg)
	if err != nil {
		return nil, err
	}
	return &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Maintenance{
		Maintenance: &pb.MaintenanceAssignment{
			TargetTable:       table,
			Ops:               opNames,
			MaintenanceConfig: cfgJSON,
		},
	}}, nil
}

// recordResult folds one maintenance pass's reported outcome into the
// coordinator's Prometheus registry. The pass ran in the worker's process,
// which exits right after — its own /metrics is gone by the time Prometheus
// would scrape it — so the counts are recorded here, mirroring how an Ack
// drives CommitsTotal. A nil registry (no --metrics-addr) is a no-op.
func (m *maintenanceScheduler) recordResult(table string, res *pb.MaintenanceResult) {
	if m.c.metrics == nil {
		return
	}
	for _, op := range res.Ops {
		switch r := op.Op.(type) {
		case *pb.MaintenanceOpResult_Compaction:
			m.c.metrics.IcebergCompactionRuns.WithLabelValues(table).Inc()
			if r.Compaction.Error == "" {
				m.c.metrics.IcebergCompactionFilesRemoved.WithLabelValues(table).Add(float64(r.Compaction.FilesRemoved))
				m.c.metrics.IcebergCompactionFilesAdded.WithLabelValues(table).Add(float64(r.Compaction.FilesAdded))
				m.c.metrics.IcebergCompactionBytesBefore.WithLabelValues(table).Add(float64(r.Compaction.BytesBefore))
				m.c.metrics.IcebergCompactionBytesAfter.WithLabelValues(table).Add(float64(r.Compaction.BytesAfter))
			}
		case *pb.MaintenanceOpResult_Expiry:
			m.c.metrics.IcebergSnapshotExpiryRuns.WithLabelValues(table).Inc()
			if r.Expiry.Error == "" {
				m.c.metrics.IcebergSnapshotExpirySnapshots.WithLabelValues(table).Add(float64(r.Expiry.SnapshotsRemoved))
			}
		case *pb.MaintenanceOpResult_Orphan:
			m.c.metrics.IcebergOrphanCleanupRuns.WithLabelValues(table).Inc()
			if r.Orphan.Error == "" {
				m.c.metrics.IcebergOrphanCleanupFiles.WithLabelValues(table).Add(float64(r.Orphan.FilesDeleted))
				m.c.metrics.IcebergOrphanCleanupBytes.WithLabelValues(table).Add(float64(r.Orphan.BytesFreed))
			}
		}
	}
}

// maintenanceWorkerName derives a DNS-1123 worker name from the pipeline and
// table target ("<pipeline>-<target>-maint"), sanitizing the dots a target
// carries and truncating to Kubernetes' 63-character limit.
func maintenanceWorkerName(pipeline, table string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(pipeline + "-" + table + "-maint") {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	if s == "" {
		s = "urutau-maintenance"
	}
	if len(s) > 63 {
		s = strings.Trim(s[:63], "-")
	}
	return s
}
