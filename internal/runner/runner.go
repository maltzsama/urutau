// Package runner wires the collapsed process: one binary runs the source
// reader, the DBLog snapshot, and the worker in a single process (local
// mode). The gRPC/Flight split arrives with multi-worker support.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Config carries the runtime knobs for a local run.
type Config struct {
	ServerID      uint32
	Heartbeat     time.Duration
	ChunkSize     int
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
	MaxRows       int
	MaxInterval   time.Duration
	// MaxParallelChunks caps concurrent chunk SELECTs during snapshot.
	// Must not exceed the ceiling the source driver declares; 0 means the
	// driver default (serial in the collapsed runner).
	MaxParallelChunks int
	// Eventlog, when set, records a per-run JSONL audit trail in S3
	// (lifecycle + commit events). Nil disables the trail.
	Eventlog *eventlog.Config
	Logger   *slog.Logger
}

// Run executes the collapsed pipeline for a validated spec until ctx is
// cancelled or a terminal error occurs. Convenience entry point for callers
// that don't need the *Runner handle — the e2e suite drives every scenario
// through it; the binary path (cmd/urutau) uses NewRunner directly.
func Run(ctx context.Context, s *spec.Spec, cfg Config) error {
	r, err := NewRunner(ctx, s, cfg)
	if err != nil {
		return err
	}
	return r.Run(ctx)
}

// ── Relay ────────────────────────────────────────────────────────────

// ── Positions and catalog ────────────────────────────────────────────

// ── Collapsed pipeline ──────────────────────────────────────────────

// Runner wraps the collapsed pipeline and exposes metrics like
// dropped rows by window (proof of caught-up state).
type Runner struct {
	w                     *worker.Worker
	enrichStages          []*enrich.Stage
	log                   *slog.Logger
	ev                    *eventlog.Run
	rdr                   source.Reader
	snk                   sink.Sink
	closeQuery            func()
	cancel                context.CancelFunc // stops the worker/relay goroutines (issue #487)
	workerErr, routerDone <-chan error
	// workerDone closes when the worker goroutine has returned, so Run waits
	// for an in-flight commit before releasing the sink (issue #559).
	workerDone <-chan struct{}

	// committedPositions tracks the latest durably-committed position per
	// target table. The minimum across tables is the confirmed position
	// reported to the source (Postgres slot advancement).
	posMu              sync.Mutex
	committedPositions map[string]position.Position
	minConfirmed       position.Position // recomputed on each commit
	// delivered is the last position read from the source per target table;
	// dispatched is the last across all tables. A table with delivered past
	// its committed has data still in flight, so the confirmed position holds
	// at its committed; a table with nothing in flight does not pin it, and
	// when nothing is in flight the confirmed may advance to dispatched.
	delivered  map[string]position.Position
	dispatched position.Position
}

// NewRunner sets up the collapsed pipeline (catalog, writers, worker,
// reader) and runs the DBLog snapshot phase for tables without a committed
// position. It returns once snapshots are done and the stream is live; Run
// then blocks until cancellation or a terminal error. Resources are owned
// by the Runner and released when Run returns. It opens the source and sink
// through the driver registry — the runner consumes only the contracts,
// never a concrete implementation.
func NewRunner(ctx context.Context, s *spec.Spec, cfg Config) (*Runner, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	// Reject collapsed partitioning BEFORE opening any adapter: NewRunner
	// owns the sink it opens, and an error after OpenSink would leak it
	// (newRunner installs the cleanup, and it never runs on this path).
	if err := rejectCollapsedPartitioning(s); err != nil {
		return nil, err
	}
	// The spec's source.serverId wins over cfg.ServerID when declared — the
	// pipeline's own server id travels with it; cfg.ServerID is only a
	// default for when the spec is silent. Spec.Validate (already run by
	// every caller that loaded this spec from YAML) rejects a non-numeric
	// serverId, so this only fails for a Spec built programmatically with
	// a bad value.
	serverID, err := s.ResolveServerID(cfg.ServerID)
	if err != nil {
		return nil, err
	}
	cfg.ServerID = serverID
	rep := &ddlReporter{}
	src, err := driver.OpenSource(s, source.Runtime{
		ServerID:         cfg.ServerID,
		Heartbeat:        cfg.Heartbeat,
		Logger:           cfg.Logger,
		OnDestructiveDDL: rep.ReportDestructiveDDL,
	})
	if err != nil {
		return nil, err
	}
	snk, err := driver.OpenSink(ctx, s)
	if err != nil {
		return nil, err
	}
	// The parallel-chunk setting may not exceed the ceiling the source
	// driver declares — fail fast at boot, not mid-snapshot.
	if err := driver.ValidateParallelism(s.Source.Kind, cfg.MaxParallelChunks); err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}
	return newRunner(ctx, s, cfg, src, snk, rep)
}

// NewRunnerWithAdapters runs the pipeline over already-open source/sink
// adapters — the external plugin path. The adapters implement the public
// source.Source / sink.Sink contracts; the caller owns parallelism
// validation (a plugin source declares no registry ceiling).
func NewRunnerWithAdapters(ctx context.Context, s *spec.Spec, cfg Config, src source.Source, snk sink.Sink) (*Runner, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return newRunner(ctx, s, cfg, src, snk, nil)
}

// rejectCollapsedPartitioning refuses workers>1 in the collapsed runner: it
// has a single in-process worker per table and would silently ignore the
// partition count, giving the user one worker when they declared N (WK-001
// C0). The distributed coordinator accepts it (and validates the sink
// capability); here it is a boot error.
func rejectCollapsedPartitioning(s *spec.Spec) error {
	for _, t := range s.Tables {
		if t.WorkerCount() > 1 {
			return fmt.Errorf("runner: %s: workers.number > 1 requires "+
				"distributed mode (coordinator + workers); the collapsed run is "+
				"single-worker", t.Target)
		}
	}
	return nil
}

// Run blocks until ctx is cancelled or a terminal error surfaces, then
// releases the pipeline resources and seals the audit trail.
func (r *Runner) Run(ctx context.Context) error {
	err := r.run(ctx)
	// Stop the worker, the relay and the maintenance loops that newRunner
	// started under runCtx before releasing their resources: a terminal error
	// from one of them would otherwise leave the peers running (issue #487).
	if r.cancel != nil {
		r.cancel()
	}
	// Wait for the worker goroutine to return before releasing the sink: a
	// commit in flight would otherwise use a closed sink (issue #559).
	if r.workerDone != nil {
		select {
		case <-r.workerDone:
		case <-time.After(10 * time.Second):
			if r.log != nil {
				r.log.Warn("runner: worker did not stop before releasing resources")
			}
		}
	}

	if r.ev != nil {
		reason := "error"
		if errors.Is(err, context.Canceled) {
			reason = "cancelled"
		}
		fields := map[string]any{"reason": reason}
		if err != nil {
			fields["error"] = err.Error() // the underlying error text (issue #351)
		}
		_ = r.ev.Emit(context.Background(), eventlog.KindJobStopped, fields)
		_ = r.ev.Close()
	}
	r.release()
	return err
}

func (r *Runner) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-r.workerErr:
			return fmt.Errorf("runner: worker: %w", err)
		case err := <-r.routerDone:
			return fmt.Errorf("runner: router: %w", err)
		}
	}
}

// DroppedByWindow returns the number of rows dropped by the window for a
// target table (the caught-up proof the e2e asserts).
func (r *Runner) DroppedByWindow(target string) int64 {
	return r.w.DroppedByWindow(target)
}
