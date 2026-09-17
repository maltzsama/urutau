package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// RunMaintenance connects to the coordinator as an ephemeral maintenance
// worker: it sends a maintenance Hello, waits for the coordinator to push a
// MaintenanceAssignment (one is sent when a maintenance turn is due), runs
// the requested operations once, reports the outcome, and returns. The
// process then exits for good — this Pod is one pass, and the coordinator
// provisions a fresh Pod for the next due turn. So a maintenance pass never
// competes with the coordinator's routing/commit path, and never takes the
// coordinator down with it.
//
// The coordinator may also close the stream without sending an assignment
// (nothing due after all); that dismissal ends the pass the same way, with
// the process exiting rather than waiting for work that will not come.
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
			if errors.Is(err, io.EOF) || status.Code(err) == codes.Canceled {
				// The coordinator dismissed us without an assignment: nothing
				// was due after all. Not a failure — exit 0 so the Pod reports
				// Completed rather than Error.
				cfg.Logger.Info("worker: maintenance: dismissed with no assignment", "worker", cfg.Name)
				return nil
			}
			return fmt.Errorf("worker: maintenance: %w", err)
		}
		assign := msg.GetMaintenance()
		if assign == nil {
			continue // ignore anything but the maintenance assignment
		}
		results, passErr := runMaintenancePass(ctx, cfg, assign)
		// Report the outcome before exiting: this process's own /metrics
		// dies with it, so a result not sent now is lost to the coordinator's
		// long-lived registry. The coordinator also marks the operations as
		// run only once this report lands, so a pass that dies before it
		// stays due. Report even on failure — the coordinator records the
		// failed operation too.
		sendErr := session.Send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_MaintenanceResult{
			MaintenanceResult: &pb.MaintenanceResult{Ops: results},
		}})
		if sendErr != nil && passErr == nil {
			return fmt.Errorf("worker: maintenance: report: %w", sendErr)
		}
		return passErr // one pass per process: exit for good
	}
}

// runMaintenancePass opens the sink, runs the assigned operations once, and
// returns the per-operation results plus the first error. The assignment
// carries the table, the operations (in order), and the operation
// parameters — the coordinator already decided which operations are due, so
// the intervals in the config are ignored.
func runMaintenancePass(ctx context.Context, cfg RemoteConfig, assign *pb.MaintenanceAssignment) ([]*pb.MaintenanceOpResult, error) {
	var maint spec.Maintenance
	if len(assign.MaintenanceConfig) > 0 {
		if err := json.Unmarshal(assign.MaintenanceConfig, &maint); err != nil {
			return nil, fmt.Errorf("worker: maintenance: config: %w", err)
		}
	}
	maint.Enabled = true
	ops := make([]sink.MaintenanceOp, len(assign.Ops))
	for i, o := range assign.Ops {
		ops[i] = sink.MaintenanceOp(o)
	}
	if len(ops) == 0 {
		return nil, nil
	}

	snk, err := driver.OpenSinkConfig(ctx, sink.Config{
		Type:      cfg.Sink.Type,
		URI:       cfg.Sink.URI,
		Namespace: cfg.Namespace,
		Options:   cfg.Sink.Options,
	})
	if err != nil {
		return nil, fmt.Errorf("worker: maintenance: open sink: %w", err)
	}
	defer func() { _ = snk.Close() }()

	maintainable, ok := snk.(sink.Maintainable)
	if !ok {
		return nil, fmt.Errorf("worker: maintenance: sink %q does not support table maintenance", cfg.Sink.Type)
	}
	ref := core.TableRef{Target: assign.TargetTable}
	currentPosition := func() string {
		pos, err := snk.Position(ctx, ref)
		if err != nil {
			return ""
		}
		return pos
	}
	collector := &resultCollector{}
	cfg.Logger.Info("worker: maintenance: start", "table", assign.TargetTable, "ops", ops)
	m := maintainable.Maintain(ref, maint, cfg.Logger, currentPosition, collector)

	// One RunOnce per operation, continuing past a failure. A batch call
	// stops at the first error, so a failing operation would keep every
	// later one in the assignment from ever running — and since the
	// coordinator only marks what this pass reports as succeeded, an
	// operation that never runs is never marked and is simply reassigned to
	// the next pass, where the same failure blocks it again. The operations
	// are independent (each reloads the table and acts on what it finds), so
	// the one that is broken should be the only one that is broken.
	var firstErr error
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			break
		}
		if err := m.RunOnce(ctx, []sink.MaintenanceOp{op}); err != nil {
			cfg.Logger.Warn("worker: maintenance: failed", "table", assign.TargetTable, "op", op, "err", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("worker: maintenance %s: %s: %w", assign.TargetTable, op, err)
			}
		}
	}
	if firstErr != nil {
		return collector.ops, firstErr
	}
	cfg.Logger.Info("worker: maintenance: done", "table", assign.TargetTable, "ops", ops)

	return collector.ops, nil
}

// resultCollector is a sink.MaintainerMetrics that captures each operation's
// result instead of writing it to a Prometheus registry. The worker's own
// registry would die with the process (the worker is ephemeral), so the
// results are shipped to the coordinator, whose registry is long-lived and
// scraped. RunOnce calls these synchronously and in order, so no locking.
type resultCollector struct {
	ops []*pb.MaintenanceOpResult
}

func (r *resultCollector) CompactionRun(_ string, filesRemoved, filesAdded int, bytesBefore, bytesAfter int64, err error) {
	r.ops = append(r.ops, &pb.MaintenanceOpResult{Op: &pb.MaintenanceOpResult_Compaction{
		Compaction: &pb.CompactionResult{
			FilesRemoved: int64(filesRemoved),
			FilesAdded:   int64(filesAdded),
			BytesBefore:  bytesBefore,
			BytesAfter:   bytesAfter,
			Error:        errText(err),
		},
	}})
}

func (r *resultCollector) SnapshotExpiryRun(_ string, snapshotsRemoved int, err error) {
	r.ops = append(r.ops, &pb.MaintenanceOpResult{Op: &pb.MaintenanceOpResult_Expiry{
		Expiry: &pb.ExpiryResult{
			SnapshotsRemoved: int64(snapshotsRemoved),
			Error:            errText(err),
		},
	}})
}

func (r *resultCollector) OrphanCleanupRun(_ string, filesDeleted int, bytesFreed int64, err error) {
	r.ops = append(r.ops, &pb.MaintenanceOpResult{Op: &pb.MaintenanceOpResult_Orphan{
		Orphan: &pb.OrphanResult{
			FilesDeleted: int64(filesDeleted),
			BytesFreed:   bytesFreed,
			Error:        errText(err),
		},
	}})
}

// errText renders an error for the wire, empty on success.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
