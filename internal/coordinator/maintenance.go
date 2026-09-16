package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/maintenance"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// maintenanceScheduler owns the ephemeral maintenance workers. It provisions
// one worker per table — a Deployment, exactly like the data workers, from
// the table's own pod template — and pushes a MaintenanceAssignment to a
// connected worker when a maintenance turn is due. The worker runs the pass
// and exits; its Deployment restarts it for the next turn. The coordinator
// never runs maintenance in its own process, and never supervises these
// workers for acks (they have no data plane).
type maintenanceScheduler struct {
	c     *Coordinator
	cfg   *spec.Maintenance
	sched *maintenance.Schedule

	mu     sync.Mutex
	tables map[string]string                      // worker name -> table
	out    map[string]chan *pb.CoordinatorMessage // worker name -> assignment channel
}

func newMaintenanceScheduler(c *Coordinator, cfg *spec.Maintenance) *maintenanceScheduler {
	return &maintenanceScheduler{
		c:      c,
		cfg:    cfg,
		sched:  maintenance.NewSchedule(),
		tables: map[string]string{},
		out:    map[string]chan *pb.CoordinatorMessage{},
	}
}

// startMaintenance wires the maintenance scheduler. It registers one
// maintenance worker per table, provisions their Deployments when Kubernetes
// worker provisioning is on, and starts the push loop. Without Kubernetes
// (no worker pod template) it still registers the workers but there is
// nothing to launch — the collapsed runner's in-process path is the only
// option there, and it is not this code.
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
	if err := c.provisionMaintenanceWorkers(m); err != nil {
		return err
	}
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

// session serves one connected maintenance worker: it waits for the push
// loop to hand it a due assignment, sends it, and returns when the worker
// closes the stream (it exits after its pass).
func (m *maintenanceScheduler) session(stream pb.UrutauControl_SessionServer, hello *pb.Hello) error {
	name := hello.WorkerName
	m.mu.Lock()
	table, known := m.tables[name]
	if !known {
		m.mu.Unlock()
		return fmt.Errorf("coordinator: unknown maintenance worker %q", name)
	}
	if _, dup := m.out[name]; dup {
		m.mu.Unlock()
		return fmt.Errorf("coordinator: maintenance worker %q already connected", name)
	}
	out := make(chan *pb.CoordinatorMessage, 1)
	m.out[name] = out
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.out, name)
		m.mu.Unlock()
	}()
	m.c.log.Info("coordinator: maintenance worker connected", "worker", name, "table", table)

	// The worker sends its Hello and, after the pass, a MaintenanceResult;
	// detect it leaving (the stream closing) so the deferred deregistration
	// runs and its restart reconnects cleanly.
	done := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
			if res := msg.GetMaintenanceResult(); res != nil {
				m.recordResult(table, res)
			}
		}
	}()

	for {
		select {
		case msg := <-out:
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-done:
			return nil // the worker finished its pass and exited
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// run pushes due assignments to connected maintenance workers until ctx is
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

// tick sends each connected worker the operations due for its table. A
// worker that already has an assignment in flight (channel full) is skipped
// until the next tick — the operation stays due, so nothing is lost.
func (m *maintenanceScheduler) tick(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, out := range m.out {
		table := m.tables[name]
		due := m.sched.Due(table, m.cfg, now)
		if len(due) == 0 {
			continue
		}
		msg, err := m.assignment(table, due)
		if err != nil {
			m.c.log.Warn("coordinator: maintenance: build assignment", "table", table, "err", err)
			continue
		}
		select {
		case out <- msg:
			m.sched.MarkRun(table, due, now)
		default:
		}
	}
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
// coordinator's Prometheus registry and the dashboard's per-table aggregate.
// The pass ran in the worker's process, which exits right after — its own
// /metrics is gone by the time Prometheus would scrape it — so the counts are
// recorded here, mirroring how an Ack drives CommitsTotal. Both sinks are
// optional (a nil registry records no metrics; the aggregate is always kept).
func (m *maintenanceScheduler) recordResult(table string, res *pb.MaintenanceResult) {
	now := time.Now()
	for _, op := range res.Ops {
		switch r := op.Op.(type) {
		case *pb.MaintenanceOpResult_Compaction:
			if m.c.metrics != nil {
				m.c.metrics.IcebergCompactionRuns.WithLabelValues(table).Inc()
				if r.Compaction.Error == "" {
					m.c.metrics.IcebergCompactionFilesRemoved.WithLabelValues(table).Add(float64(r.Compaction.FilesRemoved))
					m.c.metrics.IcebergCompactionFilesAdded.WithLabelValues(table).Add(float64(r.Compaction.FilesAdded))
					m.c.metrics.IcebergCompactionBytesBefore.WithLabelValues(table).Add(float64(r.Compaction.BytesBefore))
					m.c.metrics.IcebergCompactionBytesAfter.WithLabelValues(table).Add(float64(r.Compaction.BytesAfter))
				}
			}
			m.c.recordMaintStats(table, "compaction", now, func(s *maintStats) {
				s.filesRemoved += r.Compaction.FilesRemoved
				s.filesAdded += r.Compaction.FilesAdded
				s.bytesBefore += r.Compaction.BytesBefore
				s.bytesAfter += r.Compaction.BytesAfter
			})
		case *pb.MaintenanceOpResult_Expiry:
			if m.c.metrics != nil {
				m.c.metrics.IcebergSnapshotExpiryRuns.WithLabelValues(table).Inc()
				if r.Expiry.Error == "" {
					m.c.metrics.IcebergSnapshotExpirySnapshots.WithLabelValues(table).Add(float64(r.Expiry.SnapshotsRemoved))
				}
			}
			m.c.recordMaintStats(table, "snapshot_expiry", now, func(s *maintStats) {
				s.snapshots += r.Expiry.SnapshotsRemoved
			})
		case *pb.MaintenanceOpResult_Orphan:
			if m.c.metrics != nil {
				m.c.metrics.IcebergOrphanCleanupRuns.WithLabelValues(table).Inc()
				if r.Orphan.Error == "" {
					m.c.metrics.IcebergOrphanCleanupFiles.WithLabelValues(table).Add(float64(r.Orphan.FilesDeleted))
					m.c.metrics.IcebergOrphanCleanupBytes.WithLabelValues(table).Add(float64(r.Orphan.BytesFreed))
				}
			}
			m.c.recordMaintStats(table, "orphan_cleanup", now, func(s *maintStats) {
				s.filesDeleted += r.Orphan.FilesDeleted
				s.bytesFreed += r.Orphan.BytesFreed
			})
		}
	}
}

// provisionMaintenanceWorkers ensures one maintenance worker Deployment per
// table, cloning that table's own worker pod template — the same template
// the data workers use, so catalog credentials, image, ServiceAccount and
// resources all match.
func (c *Coordinator) provisionMaintenanceWorkers(m *maintenanceScheduler) error {
	m.mu.Lock()
	names := make(map[string]string, len(m.tables))
	for name, table := range m.tables {
		names[name] = table
	}
	m.mu.Unlock()

	clientset, ns, owner, err := inClusterClient(context.Background())
	if err != nil {
		return fmt.Errorf("k8s maintenance provisioning: %w", err)
	}
	for name, table := range names {
		tmpl, err := loadWorkerPodTemplate(table)
		if err != nil {
			return fmt.Errorf("k8s maintenance provisioning: %s: %w", table, err)
		}
		dep := maintenanceWorkerDeployment(name, ns, owner, tmpl)
		if err := ensureDeployment(context.Background(), clientset, ns, dep); err != nil {
			return fmt.Errorf("k8s maintenance provisioning: %s: %w", name, err)
		}
		c.log.Info("coordinator: maintenance worker deployment ensured", "worker", name, "table", table)
	}
	return nil
}

// maintenanceWorkerDeployment is the data worker Deployment plus the
// --maintenance flag, so the same pod template serves both.
func maintenanceWorkerDeployment(name, namespace string, owner metav1.OwnerReference, tmpl corev1.PodTemplateSpec) *appsv1.Deployment {
	dep := workerDeployment(name, namespace, owner, tmpl)
	for i := range dep.Spec.Template.Spec.Containers {
		dep.Spec.Template.Spec.Containers[i].Args = append(dep.Spec.Template.Spec.Containers[i].Args, "--maintenance")
	}
	return dep
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
