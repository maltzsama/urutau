package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/maintenance"
	"github.com/maltzsama/urutau/internal/observability"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"

	"github.com/maltzsama/urutau/internal/coordk8s"
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
	connected map[string]bool   // worker name -> session in progress
	// lastTurn is when each worker's Pod was last provisioned: the table
	// whose turn is oldest goes first, so one that is always due does not
	// keep the others waiting (tick).
	lastTurn map[string]time.Time

	// cfgJSON is cfg as every assignment carries it, marshalled on first use.
	cfgOnce sync.Once
	cfgJSON []byte
	cfgErr  error
}

func newMaintenanceScheduler(c *Coordinator, cfg *spec.Maintenance) *maintenanceScheduler {
	return &maintenanceScheduler{
		c:         c,
		cfg:       cfg,
		sched:     maintenance.NewSchedule(),
		tables:    map[string]string{},
		connected: map[string]bool{},
		lastTurn:  map[string]time.Time{},
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
	// c.maint is published only once the scheduler is whole: sessions read it
	// from other goroutines, and one that found it before its Kubernetes
	// client was set would skip deleting its worker's Pod.
	if !coordk8s.WorkerPodTemplateAvailable(workerTarget) {
		c.log.Warn("coordinator: maintenance enabled but no worker pod template — no maintenance worker will run (Kubernetes provisioning is off)")
		c.maint = m
		return nil
	}
	ctx, cancel := maintenanceK8sContext()
	defer cancel()
	clientset, ns, owner, err := coordk8s.InClusterClient(ctx)
	if err != nil {
		return fmt.Errorf("k8s maintenance provisioning: %w", err)
	}
	m.k8s, m.namespace, m.owner = clientset, ns, owner
	c.maint = m
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

// maintenanceK8sTimeout bounds every Kubernetes call the maintenance scheduler
// makes. The client sets no request timeout of its own, so a hung API server
// would otherwise hold a call forever: in tick, which is serial, that stops
// maintenance for every table; in a session's teardown it keeps the table
// marked connected, so tick skips it for good. With the bound a hung server
// costs one tick or one teardown.
const maintenanceK8sTimeout = 30 * time.Second

// maintenanceK8sContext is the context of one maintenance Kubernetes call.
func maintenanceK8sContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), maintenanceK8sTimeout)
}

// maintenancePassTimeout is how long one maintenance pass may hold its table.
// Compactions are long, so the bound is generous; past it the worker is
// taken for hung. A variable so tests can shorten it.
var maintenancePassTimeout = time.Hour

// boundPass starts the watchdog of one pass and returns its stop function.
// When the pass outlasts maintenancePassTimeout the worker's Pod is deleted:
// its stream dies, the session returns, and the operations it had not
// reported stay due, as for a worker that died mid-pass (#457). Without a
// Kubernetes client there is no Pod to delete and no watchdog.
func (m *maintenanceScheduler) boundPass(name, table string) (stop func()) {
	if m.k8s == nil {
		return func() {}
	}
	t := time.AfterFunc(maintenancePassTimeout, func() {
		m.c.log.Warn("coordinator: maintenance: pass outlasted its bound; deleting the worker pod",
			"worker", name, "table", table, "bound", maintenancePassTimeout)
		ctx, cancel := maintenanceK8sContext()
		defer cancel()
		if err := coordk8s.DeletePod(ctx, m.k8s, m.namespace, name); err != nil {
			m.c.log.Warn("coordinator: maintenance: delete hung worker pod", "worker", name, "err", err)
		}
	})
	return func() { t.Stop() }
}

// unexpectedSessionEnd reports whether a maintenance session's Recv error is
// something other than its two ordinary ends: the worker closing the stream
// after its pass (io.EOF), and a cancelled context (shutdown, or the worker's
// Pod being deleted).
func unexpectedSessionEnd(err error) bool {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return false
	}
	return status.Code(err) != codes.Canceled
}

// isWorker reports whether name is a registered maintenance worker.
func (m *maintenanceScheduler) isWorker(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.tables[name]
	return ok
}

// session serves one connected maintenance worker for exactly one pass: it
// hands over the operations due for its table, records the result the worker
// reports, and returns when the worker closes the stream (it exits after the
// pass). The worker's Pod is deleted on the way out, so it terminates
// instead of being restarted.
//
// A worker that finds nothing due is dismissed immediately rather than left
// blocked in Recv: its Pod only exists because tick saw a turn due, so
// nothing due here means the turn was already served: a duplicate Pod, or
// another session that marked the table between tick and this worker's
// connect. (A coordinator restart is not a cause: the schedule lives in
// memory, so a restart makes every operation due again.) Returning ends the
// session, the worker exits, and the deferred cleanup removes its Pod —
// leaving it connected would park the Pod forever and silently stop all
// further maintenance for that table.
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
		// Delete the Pod BEFORE releasing the table: while connected[name] is
		// set, tick skips this table, so the Pod cannot be adopted as a fresh
		// provision mid-teardown. Clearing first would let a tick in that
		// window see the Pod, count it as its own, and then watch this delete
		// take it away.
		if m.k8s != nil {
			ctx, cancel := maintenanceK8sContext()
			if err := coordk8s.DeletePod(ctx, m.k8s, m.namespace, name); err != nil {
				m.c.log.Warn("coordinator: maintenance: delete worker pod", "worker", name, "err", err)
			}
			cancel()
		}
		m.mu.Lock()
		delete(m.connected, name)
		m.mu.Unlock()
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
	sent := time.Now()
	// A pass that never ends would hold the table for good: while its session
	// is connected tick skips the table, and a worker wedged on the catalog
	// or behind a dead connection never closes its stream.
	defer m.boundPass(name, table)()

	// The worker reports each operation in its own MaintenanceResult as the
	// operation ends, then closes the stream when the pass does. MarkRun
	// happens on those reports, not on the send above: a worker that dies
	// mid-pass (OOM, node drain) leaves the operations it had not reported
	// due, and keeps the ones it had (#457).
	//
	// Only the operations reported as succeeded are marked. The worker
	// reports failures too (its own registry dies with the process) and goes
	// on past them, so a failed expiry is recorded, stays due, and does not
	// hold the cleanup and compaction after it.
	for {
		in, err := stream.Recv()
		if err != nil {
			// The worker finished (or died); either way it is gone. A clean
			// end is the normal one; anything else is said out loud, or a
			// dropped stream would look exactly like a pass that ended.
			if unexpectedSessionEnd(err) {
				m.c.log.Warn("coordinator: maintenance: session ended unexpectedly",
					"worker", hello.WorkerName, "table", table, "err", err)
			}
			return nil
		}
		if res := in.GetMaintenanceResult(); res != nil {
			m.logPass(name, table, reportedOps(res), time.Since(sent), res)
			m.recordResult(table, res)
			if ran := succeededOps(res); len(ran) > 0 {
				m.mu.Lock()
				m.sched.MarkRun(table, ran, time.Now())
				m.mu.Unlock()
			}
		}
		if log := in.GetLog(); log != nil {
			m.c.onWorkerLog(name, log)
		}
	}
}

// reportedOps returns the operations a worker's report covers, in order.
func reportedOps(res *pb.MaintenanceResult) []sink.MaintenanceOp {
	var ops []sink.MaintenanceOp
	walkOps(res, func(op sink.MaintenanceOp, _ string) { ops = append(ops, op) })
	return ops
}

// succeededOps returns the operations a worker's report says completed
// without error — the ones that may be marked as run. An operation the
// worker never reached contributes no result at all, and one that failed
// carries an Error, so neither is returned.
func succeededOps(res *pb.MaintenanceResult) []sink.MaintenanceOp {
	var ops []sink.MaintenanceOp
	walkOps(res, func(op sink.MaintenanceOp, errText string) {
		if errText == "" {
			ops = append(ops, op)
		}
	})
	return ops
}

// walkOps calls fn with each operation of a worker's report and its error
// text, empty on success. It is the one place that maps the wire's oneof to
// an operation: a new operation is added here, not in every function that
// asks which operations a report holds.
func walkOps(res *pb.MaintenanceResult, fn func(op sink.MaintenanceOp, errText string)) {
	for _, op := range res.Ops {
		switch r := op.Op.(type) {
		case *pb.MaintenanceOpResult_Compaction:
			fn(sink.MaintenanceCompaction, r.Compaction.Error)
		case *pb.MaintenanceOpResult_Expiry:
			fn(sink.MaintenanceSnapshotExpiry, r.Expiry.Error)
		case *pb.MaintenanceOpResult_Orphan:
			fn(sink.MaintenanceOrphanCleanup, r.Orphan.Error)
		}
	}
}

// run provisions ephemeral maintenance workers for due turns until ctx is
// done.
func (m *maintenanceScheduler) run(ctx context.Context) {
	// One pass at once: after a restart every operation is due, and waiting
	// out a full check interval first would leave the tables idle for it.
	m.tick(time.Now())
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

// tick provisions the ephemeral Pod of one table whose next turn has come
// due, when no maintenance worker of the pipeline is in flight. Handing over
// the assignment is session's job, not tick's — the worker gets it the moment
// it connects.
//
// One pass at a time: a compaction holds gigabytes for a large table, and
// every table of a pipeline comes due together after a restart or a quiet
// period. Their passes side by side took 4-5 GiB each on 1M-row tables and
// exhausted the node (#831). The tables take turns, the one served longest
// ago first.
//
// A table with a live session keeps the others waiting; its operations stay
// due, so nothing is lost. Otherwise the live Pod, not a local flag, decides: a Pod
// still starting is left alone, and one that will never open a session
// (Failed, Succeeded without reporting, ImagePullBackOff, unschedulable) is
// deleted so the next tick provisions a fresh one. A bare flag cannot do
// this — a Pod that dies before connecting never reaches session's cleanup,
// and the flag would pin the table until the coordinator restarted.
func (m *maintenanceScheduler) tick(now time.Time) {
	if m.k8s == nil {
		return
	}
	m.mu.Lock()
	if len(m.connected) > 0 {
		m.mu.Unlock()
		return
	}
	candidates := make(map[string]string, len(m.tables))
	turns := make(map[string]time.Time, len(m.tables))
	for name, table := range m.tables {
		if len(m.sched.Due(table, m.cfg, now)) == 0 {
			continue
		}
		candidates[name] = table
		turns[name] = m.lastTurn[name]
	}
	m.mu.Unlock()

	// Oldest turn first, then by name, so the order of the Pods and of their
	// log lines is the same from one tick to the next.
	names := make([]string, 0, len(candidates))
	for name := range candidates {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		if ti, tj := turns[names[i]], turns[names[j]]; !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return names[i] < names[j]
	})
	for _, name := range names {
		table := candidates[name]
		inFlight, err := m.provisionWorkerPod(name, table)
		if err != nil {
			m.c.log.Warn("coordinator: maintenance: provision worker pod", "worker", name, "table", table, "err", err)
		}
		if inFlight {
			// This table's worker is on its way: the others wait for its
			// pass to end.
			return
		}
	}
}

// provisionWorkerPod brings one due table's maintenance worker Pod into
// existence, reconciling against whatever is already there rather than
// trusting a local flag:
//
//   - no Pod: create it.
//   - a Pod that has not finished starting: leave it, it may still connect.
//   - a Pod that can no longer connect: delete it and create a replacement
//     on the next tick, so a failed image pull or an unschedulable Pod does
//     not stall the table forever.
//
// inFlight reports whether the table now has a Pod that may still connect.
func (m *maintenanceScheduler) provisionWorkerPod(name, table string) (inFlight bool, err error) {
	ctx, cancel := maintenanceK8sContext()
	defer cancel()
	existing, err := coordk8s.GetPod(ctx, m.k8s, m.namespace, name)
	if err != nil {
		return false, err
	}
	if existing != nil {
		if coordk8s.PodCanStillConnect(existing) {
			return true, nil
		}
		m.c.log.Info("coordinator: maintenance: replacing worker pod that cannot connect",
			"worker", name, "table", table, "phase", existing.Status.Phase)
		// The table used its turn: the next tick serves another before it.
		m.noteTurn(name)
		return false, coordk8s.DeletePod(ctx, m.k8s, m.namespace, name)
	}
	tmpl, err := coordk8s.LoadWorkerPodTemplate(table)
	if err != nil {
		return false, err
	}
	if err := coordk8s.CreatePod(ctx, m.k8s, m.namespace, coordk8s.MaintenanceWorkerPod(name, m.namespace, m.owner, tmpl)); err != nil {
		return false, err
	}
	m.noteTurn(name)
	m.c.log.Info("coordinator: maintenance worker pod provisioned", "worker", name, "table", table)
	return true, nil
}

// noteTurn records that name's table was just given a turn.
func (m *maintenanceScheduler) noteTurn(name string) {
	m.mu.Lock()
	m.lastTurn[name] = time.Now()
	m.mu.Unlock()
}

// assignment renders one table's due operations into a wire assignment. The
// config carries the operation parameters; the intervals are ignored by the
// worker because the coordinator already decided what is due.
func (m *maintenanceScheduler) assignment(table string, ops []sink.MaintenanceOp) (*pb.CoordinatorMessage, error) {
	opNames := make([]string, len(ops))
	for i, o := range ops {
		opNames[i] = string(o)
	}
	// The config never changes after construction: marshal it once.
	m.cfgOnce.Do(func() { m.cfgJSON, m.cfgErr = json.Marshal(m.cfg) })
	if m.cfgErr != nil {
		return nil, m.cfgErr
	}
	cfgJSON := m.cfgJSON
	return &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Maintenance{
		Maintenance: &pb.MaintenanceAssignment{
			TargetTable:       table,
			Ops:               opNames,
			MaintenanceConfig: cfgJSON,
		},
	}}, nil
}

// logPass records a maintenance worker's report in the coordinator's log: its
// table, the operations reported, each one's outcome or error, and how long
// the worker had held the table by then (no other pass runs on it
// meanwhile). reported is what the worker's result covers, not what was due:
// an operation that was due and never reported (the worker died mid-pass) is
// absent from it, and that absence is the point of the line. The worker's Pod is deleted after the pass, so this line is
// what shows why a table is not being maintained (issue #457). A report with
// a failed operation is a warning.
func (m *maintenanceScheduler) logPass(worker, table string, reported []sink.MaintenanceOp, took time.Duration, res *pb.MaintenanceResult) {
	attrs := []any{"table", table, "worker", worker, "ops", reported, "took", took.Round(time.Millisecond)}
	failed := false
	for _, op := range res.Ops {
		switch r := op.Op.(type) {
		case *pb.MaintenanceOpResult_Compaction:
			attrs = append(attrs, "compaction_error", r.Compaction.Error,
				"files_removed", r.Compaction.FilesRemoved, "files_added", r.Compaction.FilesAdded)
			failed = failed || r.Compaction.Error != ""
		case *pb.MaintenanceOpResult_Expiry:
			attrs = append(attrs, "expiry_error", r.Expiry.Error, "snapshots_removed", r.Expiry.SnapshotsRemoved)
			failed = failed || r.Expiry.Error != ""
		case *pb.MaintenanceOpResult_Orphan:
			attrs = append(attrs, "orphan_error", r.Orphan.Error, "files_deleted", r.Orphan.FilesDeleted)
			failed = failed || r.Orphan.Error != ""
		}
	}
	if failed {
		m.c.log.Warn("coordinator: maintenance pass", attrs...)
		return
	}
	m.c.log.Info("coordinator: maintenance pass", attrs...)
}

// recordResult folds one maintenance pass's reported outcome into the
// coordinator's Prometheus registry and the dashboard's per-table aggregate.
// The pass ran in the worker's process, which exits right after — its own
// /metrics is gone by the time Prometheus would scrape it — so the counts are
// recorded here, mirroring how an Ack drives CommitsTotal. Both sinks are
// optional (a nil registry records no metrics; the aggregate is always kept).
func (m *maintenanceScheduler) recordResult(table string, res *pb.MaintenanceResult) {
	now := time.Now()
	metrics := m.c.metrics
	for _, op := range res.Ops {
		switch r := op.Op.(type) {
		case *pb.MaintenanceOpResult_Compaction:
			failed := m.countRun(table, "compaction", r.Compaction.Error, metricsRuns(metrics, "compaction"))
			if metrics != nil && !failed {
				metrics.IcebergCompactionFilesRemoved.WithLabelValues(table).Add(float64(r.Compaction.FilesRemoved))
				metrics.IcebergCompactionFilesAdded.WithLabelValues(table).Add(float64(r.Compaction.FilesAdded))
				metrics.IcebergCompactionBytesBefore.WithLabelValues(table).Add(float64(r.Compaction.BytesBefore))
				metrics.IcebergCompactionBytesAfter.WithLabelValues(table).Add(float64(r.Compaction.BytesAfter))
			}
			m.c.recordMaintStats(table, "compaction", now, failed, func(s *maintStats) {
				s.filesRemoved += r.Compaction.FilesRemoved
				s.filesAdded += r.Compaction.FilesAdded
				s.bytesBefore += r.Compaction.BytesBefore
				s.bytesAfter += r.Compaction.BytesAfter
			})
		case *pb.MaintenanceOpResult_Expiry:
			failed := m.countRun(table, "snapshot_expiry", r.Expiry.Error, metricsRuns(metrics, "snapshot_expiry"))
			if metrics != nil && !failed {
				metrics.IcebergSnapshotExpirySnapshots.WithLabelValues(table).Add(float64(r.Expiry.SnapshotsRemoved))
			}
			m.c.recordMaintStats(table, "snapshot_expiry", now, failed, func(s *maintStats) {
				s.snapshots += r.Expiry.SnapshotsRemoved
			})
		case *pb.MaintenanceOpResult_Orphan:
			failed := m.countRun(table, "orphan_cleanup", r.Orphan.Error, metricsRuns(metrics, "orphan_cleanup"))
			if metrics != nil && !failed {
				metrics.IcebergOrphanCleanupFiles.WithLabelValues(table).Add(float64(r.Orphan.FilesDeleted))
				metrics.IcebergOrphanCleanupBytes.WithLabelValues(table).Add(float64(r.Orphan.BytesFreed))
			}
			m.c.recordMaintStats(table, "orphan_cleanup", now, failed, func(s *maintStats) {
				s.filesDeleted += r.Orphan.FilesDeleted
				s.bytesFreed += r.Orphan.BytesFreed
			})
		}
	}
	m.c.pushDashState()
}

// countRun counts one operation's run in the registry and, when it failed, its
// failure. It reports whether the operation failed: the caller then records
// nothing else for it, in the registry or in the dashboard. A failed operation
// may report the work it did before failing; counting that in one place and
// not the other is how /metrics and the dashboard came to disagree.
func (m *maintenanceScheduler) countRun(table, op, errText string, runs *prometheus.CounterVec) bool {
	failed := errText != ""
	if m.c.metrics == nil {
		return failed
	}
	runs.WithLabelValues(table).Inc()
	if failed {
		m.c.metrics.IcebergMaintenanceFailures.WithLabelValues(table, op).Inc()
	}
	return failed
}

// metricsRuns is the run counter of one maintenance operation, nil without a
// registry.
func metricsRuns(m *observability.Metrics, op string) *prometheus.CounterVec {
	if m == nil {
		return nil
	}
	switch op {
	case "compaction":
		return m.IcebergCompactionRuns
	case "snapshot_expiry":
		return m.IcebergSnapshotExpiryRuns
	default:
		return m.IcebergOrphanCleanupRuns
	}
}

// maintenanceWorkerName is maintenance.WorkerName — the operator names these
// same Pods in this coordinator's RBAC Role, so the derivation is shared
// rather than reimplemented here.
func maintenanceWorkerName(pipeline, table string) string {
	return maintenance.WorkerName(pipeline, table)
}
