package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/observability"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// RunMaintenance connects to the coordinator as an ephemeral maintenance
// worker: it sends a maintenance Hello, waits for the coordinator to push a
// MaintenanceAssignment (one is sent when a maintenance turn is due), runs
// the requested operations once, and returns. The process then exits, and
// the coordinator's Deployment restarts it for the next turn — so a
// maintenance pass never competes with the coordinator's routing/commit
// path, and never takes the coordinator down with it.
//
// It never opens the data plane: no Flight, no acks. The coordinator knows a
// maintenance worker by its Hello marker, answers with the maintenance
// assignment instead of a data Assignment, and does not supervise it for
// acks.
func RunMaintenance(ctx context.Context, cfg RemoteConfig) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	conn, err := grpc.NewClient(cfg.Coordinator, dialOpts(cfg.TLS)...)
	if err != nil {
		return fmt.Errorf("worker: maintenance: dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	session, err := sessionWithRetry(ctx, conn, cfg.Logger)
	if err != nil {
		return fmt.Errorf("worker: maintenance: session: %w", err)
	}
	if err := session.Send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: &pb.Hello{
		WorkerName:  cfg.Name,
		Phase:       pb.WorkerPhase_WORKER_PHASE_STARTING,
		Epoch:       1,
		Maintenance: true,
	}}}); err != nil {
		return fmt.Errorf("worker: maintenance: hello: %w", err)
	}
	cfg.Logger.Info("worker: maintenance: awaiting assignment", "worker", cfg.Name)

	for {
		msg, err := session.Recv()
		if err != nil {
			return fmt.Errorf("worker: maintenance: %w", err)
		}
		assign := msg.GetMaintenance()
		if assign == nil {
			continue // ignore anything but the maintenance assignment
		}
		if err := runMaintenancePass(ctx, cfg, assign); err != nil {
			return err
		}
		return nil // one pass per process: exit, and the Deployment restarts us
	}
}

// runMaintenancePass opens the sink, runs the assigned operations once, and
// returns. The assignment carries the table, the operations (in order), and
// the operation parameters — the coordinator already decided which
// operations are due, so the intervals in the config are ignored.
func runMaintenancePass(ctx context.Context, cfg RemoteConfig, assign *pb.MaintenanceAssignment) error {
	var maint spec.Maintenance
	if len(assign.MaintenanceConfig) > 0 {
		if err := json.Unmarshal(assign.MaintenanceConfig, &maint); err != nil {
			return fmt.Errorf("worker: maintenance: config: %w", err)
		}
	}
	maint.Enabled = true
	ops := make([]sink.MaintenanceOp, len(assign.Ops))
	for i, o := range assign.Ops {
		ops[i] = sink.MaintenanceOp(o)
	}
	if len(ops) == 0 {
		return nil
	}

	snk, err := driver.OpenSinkConfig(ctx, sink.Config{
		Type:      cfg.Sink.Type,
		URI:       cfg.Sink.URI,
		Namespace: cfg.Namespace,
		Options:   cfg.Sink.Options,
	})
	if err != nil {
		return fmt.Errorf("worker: maintenance: open sink: %w", err)
	}
	defer func() { _ = snk.Close() }()

	maintainable, ok := snk.(sink.Maintainable)
	if !ok {
		return fmt.Errorf("worker: maintenance: sink %q does not support table maintenance", cfg.Sink.Type)
	}
	ref := core.TableRef{Target: assign.TargetTable}
	currentPosition := func() string {
		pos, err := snk.Position(ctx, ref)
		if err != nil {
			return ""
		}
		return pos
	}
	cfg.Logger.Info("worker: maintenance: start", "table", assign.TargetTable, "ops", ops)
	m := maintainable.Maintain(ref, maint, cfg.Logger, currentPosition, maintenanceMetricsFor(cfg.MetricsAddr))
	if err := m.RunOnce(ctx, ops); err != nil {
		return fmt.Errorf("worker: maintenance %s: %w", assign.TargetTable, err)
	}
	cfg.Logger.Info("worker: maintenance: done", "table", assign.TargetTable, "ops", ops)
	return nil
}

// maintenanceMetricsFor returns the Prometheus recorder for a maintenance
// pass, or nil when the worker serves no metrics. The pass runs in THIS
// process, so its counters live in the worker's own registry (served on
// MetricsAddr) — not the coordinator's, which never sees the pass.
func maintenanceMetricsFor(metricsAddr string) sink.MaintainerMetrics {
	if metricsAddr == "" {
		return nil
	}
	m := observability.New()
	go func() { _ = m.Serve(metricsAddr, nil) }()
	return maintenanceMetrics{m}
}

// maintenanceMetrics implements sink.MaintainerMetrics against the worker's
// own Prometheus registry.
type maintenanceMetrics struct {
	m *observability.Metrics
}

func (a maintenanceMetrics) CompactionRun(table string, filesRemoved, filesAdded int, bytesBefore, bytesAfter int64, err error) {
	a.m.IcebergCompactionRuns.WithLabelValues(table).Inc()
	if err != nil {
		return
	}
	a.m.IcebergCompactionFilesRemoved.WithLabelValues(table).Add(float64(filesRemoved))
	a.m.IcebergCompactionFilesAdded.WithLabelValues(table).Add(float64(filesAdded))
	a.m.IcebergCompactionBytesBefore.WithLabelValues(table).Add(float64(bytesBefore))
	a.m.IcebergCompactionBytesAfter.WithLabelValues(table).Add(float64(bytesAfter))
}

func (a maintenanceMetrics) SnapshotExpiryRun(table string, snapshotsRemoved int, err error) {
	a.m.IcebergSnapshotExpiryRuns.WithLabelValues(table).Inc()
	if err == nil {
		a.m.IcebergSnapshotExpirySnapshots.WithLabelValues(table).Add(float64(snapshotsRemoved))
	}
}

func (a maintenanceMetrics) OrphanCleanupRun(table string, filesDeleted int, bytesFreed int64, err error) {
	a.m.IcebergOrphanCleanupRuns.WithLabelValues(table).Inc()
	if err == nil {
		a.m.IcebergOrphanCleanupFiles.WithLabelValues(table).Add(float64(filesDeleted))
		a.m.IcebergOrphanCleanupBytes.WithLabelValues(table).Add(float64(bytesFreed))
	}
}
