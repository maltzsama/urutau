// Package runner wires the collapsed process: one binary runs the source
// reader, the DBLog snapshot, and the worker in a single process (local
// mode). The gRPC/Flight split arrives with multi-worker support.
package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	"github.com/maltzsama/urutau/internal/drivers"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/position"
	"github.com/maltzsama/urutau/internal/sink"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/spec"
	"github.com/maltzsama/urutau/internal/worker"
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
	// Eventlog, when set, records a per-run JSONL audit trail in S3
	// (lifecycle + commit events). Nil disables the trail.
	Eventlog *eventlog.Config
	Logger   *slog.Logger
	// Drivers, when set, is the source/sink driver assembly to use; nil
	// resolves the default registry from the spec (local mode).
	Drivers *drivers.Registry
}

// Run executes the collapsed pipeline for a validated spec until ctx is
// cancelled or a terminal error occurs.
// Run builds the collapsed pipeline, runs the snapshot phase, and blocks
// until ctx is cancelled or a terminal error occurs.
func Run(ctx context.Context, s *spec.Spec, cfg Config) error {
	r, err := NewRunner(ctx, s, cfg)
	if err != nil {
		return err
	}
	return r.Run(ctx)
}

// ── Relay ────────────────────────────────────────────────────────────

// relay pumps reader events into the worker's ingest channel and releases
// the DBLog chunk markers. Window tagging happens in the reader at decode
// time; the marker's Release first drains the pump, so every event decoded
// inside the window is already enqueued ahead of it.
type relay struct {
	ingest   chan<- change.Change
	window   *worker.Worker
	flushReq chan chan struct{}
}

func newRelay(ingest chan<- change.Change, window *worker.Worker) *relay {
	return &relay{ingest: ingest, window: window, flushReq: make(chan chan struct{}, 1)}
}

func (r *relay) Release(table string, chunkID uint32, at position.Position) {
	req := make(chan struct{})
	r.flushReq <- req
	<-req
	r.ingest <- change.Change{
		Table:    table,
		Position: at.String(),
		Window:   &change.Window{ChunkID: chunkID, Closes: true},
	}
}

func (r *relay) AddWindowRows(target string, chunkID uint32, rows []change.Change) error {
	return r.window.AddWindowRows(target, chunkID, rows)
}

// run routes reader events into the worker's ingest channel.
func (r *relay) run(ctx context.Context, out <-chan change.Change) error {
	for {
		select {
		case c, ok := <-out:
			if !ok {
				return nil
			}
			select {
			case r.ingest <- c:
			case <-ctx.Done():
				return ctx.Err()
			}
		case req := <-r.flushReq:
			// Drain everything already decoded into ingest, then ack.
		drain:
			for {
				select {
				case c, ok := <-out:
					if !ok {
						break drain
					}
					select {
					case r.ingest <- c:
					case <-ctx.Done():
						close(req)
						return ctx.Err()
					}
				default:
					break drain
				}
			}
			close(req)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ── Positions and catalog ────────────────────────────────────────────

func resumeFrom(ctx context.Context, reg *drivers.Registry, cat *rest.Catalog, s *spec.Spec, refs []core.TableRef) (position.Position, []core.TableRef, error) {
	var positions []position.Position
	var needsSnapshot []core.TableRef
	for _, ref := range refs {
		pos, err := drivers.CommittedPosition(ctx, cat, drivers.TargetIdent(s, ref.Target))
		if err != nil {
			return nil, nil, fmt.Errorf("runner: %s: %w", ref.Target, err)
		}
		if pos != "" {
			p, err := reg.ParsePosition(pos)
			if err != nil {
				return nil, nil, fmt.Errorf("runner: %s cdc.position %q: %w", ref.Target, pos, err)
			}
			positions = append(positions, p)
		} else {
			needsSnapshot = append(needsSnapshot, ref)
		}
	}
	if len(positions) == 0 {
		return nil, needsSnapshot, nil
	}
	return position.Min(positions), needsSnapshot, nil
}

// introspectAll resolves each spec table through the registry, so the
// pipeline knows the PK (equality key) and the target shape before writing
// anything.
func introspectAll(ctx context.Context, reg *drivers.Registry, qdb *sql.DB, s *spec.Spec) ([]core.TableRef, map[string]*iceberg.Schema, error) {
	refs := make([]core.TableRef, 0, len(s.Tables))
	schemas := make(map[string]*iceberg.Schema, len(s.Tables))
	for _, t := range s.Tables {
		ref, is, err := reg.Introspect(ctx, qdb, t)
		if err != nil {
			return nil, nil, err
		}
		refs = append(refs, ref)
		schemas[t.Source] = is
	}
	return refs, schemas, nil
}

// ── Collapsed pipeline ──────────────────────────────────────────────

func resumeOrNone(p position.Position) string {
	if p == nil {
		return "none"
	}
	return p.String()
}

// Runner wraps the collapsed pipeline and exposes metrics like
// dropped rows by window (proof of caught-up state).
type Runner struct {
	w                                *worker.Worker
	log                              *slog.Logger
	ev                               *eventlog.Run
	rdr                              drivers.StreamSource
	closeQuery                       func()
	streamErr, workerErr, routerDone <-chan error
}

// emit posts one lifecycle event to the audit trail, best-effort by
// contract: a lost event is logged and the pipeline carries on.
func (r *Runner) emit(kind string, fields map[string]any) {
	if r.ev == nil {
		return
	}
	if err := r.ev.Emit(context.Background(), kind, fields); err != nil {
		r.log.Warn("eventlog: emit failed", "kind", kind, "err", err)
	}
}

// NewRunner sets up the collapsed pipeline (catalog, writers, worker,
// reader) and runs the DBLog snapshot phase for tables without a committed
// position. It returns once snapshots are done and the stream is live; Run
// then blocks until cancellation or a terminal error. Resources are owned
// by the Runner and released when Run returns.
func NewRunner(ctx context.Context, s *spec.Spec, cfg Config) (r *Runner, err error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	log := cfg.Logger

	// Audit trail first: job_started marks the boot, and a startup failure
	// still seals the trail with job_stopped.
	var ev *eventlog.Run
	if cfg.Eventlog != nil {
		e, eerr := eventlog.New(ctx, *cfg.Eventlog)
		if eerr != nil {
			return nil, eerr
		}
		ev = e
		_ = ev.Emit(ctx, eventlog.KindJobStarted, map[string]any{
			"pipeline": s.Pipeline, "source": s.Source.Kind, "tables": len(s.Tables),
		})
		defer func() {
			if r == nil {
				_ = ev.Emit(ctx, eventlog.KindJobStopped, map[string]any{"reason": "startup_failed"})
				ev.Close()
			}
		}()
	}

	reg := cfg.Drivers
	if reg == nil {
		reg, err = drivers.New(s, drivers.Runtime{
			ServerID:  cfg.ServerID,
			Heartbeat: cfg.Heartbeat,
			Logger:    log,
		})
		if err != nil {
			return nil, err
		}
	}

	// One query connection for chunk SELECTs, schema introspection, and the
	// position queries (caught-up proof, slot state).
	qdb, err := reg.OpenQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("runner: open query db: %w", err)
	}

	// Resolve source tables and their Iceberg schemas.
	refs, schemas, err := introspectAll(ctx, reg, qdb, s)
	if err != nil {
		return nil, err
	}

	// Catalog + writers, ensuring tables exist.
	cat, err := drivers.NewCatalog(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("runner: catalog: %w", err)
	}
	if err := drivers.EnsureNamespace(ctx, cat, table.Identifier{s.Sink.Namespace}); err != nil {
		return nil, err
	}

	writers := make(map[string]sink.TableWriter, len(refs))
	for _, ref := range refs {
		ident := drivers.TargetIdent(s, ref.Target)
		if err := drivers.EnsureTable(ctx, cat, ident, schemas[ref.Source]); err != nil {
			return nil, fmt.Errorf("runner: ensure %s: %w", ref.Target, err)
		}
		wr, err := drivers.NewTableWriter(ctx, cat, ident, ref.PrimaryKey)
		if err != nil {
			return nil, fmt.Errorf("runner: writer %s: %w", ref.Target, err)
		}
		writers[ref.Target] = wr
	}

	// Worker + ingest channel.
	ingest := make(chan change.Change, 1024)
	w := worker.New(worker.Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval})
	for target, wr := range writers {
		w.Register(target, wr)
	}
	r = &Runner{w: w, log: log, ev: ev, closeQuery: func() { _ = qdb.Close() }}
	w.OnCommit(func(b change.Batch, rows int) {
		log.Info("commit", "table", b.Table, "rows", rows,
			"upserts", len(b.Upserts), "deletes", len(b.Deletes), "position", b.Position)
		r.emit(eventlog.KindCommit, map[string]any{
			"table": b.Table, "rows": rows,
			"upserts": len(b.Upserts), "deletes": len(b.Deletes), "position": b.Position,
		})
	})
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Run(ctx, ingest) }()

	resume, needsSnapshot, err := resumeFrom(ctx, reg, cat, s, refs)
	if err != nil {
		_ = qdb.Close()
		return nil, err
	}
	log.Info("resume", "from", resumeOrNone(resume), "snapshot_tables", len(needsSnapshot))
	r.emit(eventlog.KindResume, map[string]any{
		"from": resumeOrNone(resume), "snapshot_tables": len(needsSnapshot),
	})

	// Reader (one replication connection) → relay → ingest. The reader
	// constructor also performs the source's server-side setup (Postgres
	// slot and publication).
	out := make(chan change.Change, 1024)
	rdr, err := reg.NewReader(ctx, qdb, refs, out)
	if err != nil {
		_ = qdb.Close()
		return nil, err
	}
	r.rdr = rdr

	router := newRelay(ingest, w)
	routerDone := make(chan error, 1)
	go func() { routerDone <- router.run(ctx, out) }()

	start := resume
	if start == nil {
		if m, err := reg.InitialPosition(ctx, qdb); err != nil {
			return nil, fmt.Errorf("runner: initial position: %w", err)
		} else {
			start = m
		}
	}

	streamErr := make(chan error, 1)
	go func() { streamErr <- rdr.Start(ctx, start) }()

	// Snapshot phase: DBLog for tables with no committed position.
	for _, ref := range needsSnapshot {
		log.Info("snapshot", "table", ref.Source)
		r.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source, "target": ref.Target})
		chunker, err := reg.NewChunker(qdb, ref.Source, strings.Join(ref.PrimaryKey, ","), cfg.ChunkSize)
		if err != nil {
			rdr.Close()
			_ = qdb.Close()
			return nil, err
		}
		if err := snapshot.SnapshotTable(ctx, chunker, rdr, router, ref.Target, snapshot.SnapshotConfig{
			WindowTimeout: cfg.WindowTimeout,
			CaughtUpPoll:  cfg.CaughtUpPoll,
		}); err != nil {
			rdr.Close()
			_ = qdb.Close()
			return nil, fmt.Errorf("runner: snapshot %s: %w", ref.Source, err)
		}
		log.Info("snapshot done", "table", ref.Source)
		r.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source, "target": ref.Target})
	}

	r.streamErr = streamErr
	r.workerErr = workerErr
	r.routerDone = routerDone

	return r, nil
}

// Run blocks until ctx is cancelled or a terminal error surfaces, then
// releases the pipeline resources and seals the audit trail.
func (r *Runner) Run(ctx context.Context) error {
	err := r.run(ctx)
	if r.ev != nil {
		reason := "error"
		if errors.Is(err, context.Canceled) {
			reason = "cancelled"
		}
		_ = r.ev.Emit(context.Background(), eventlog.KindJobStopped,
			map[string]any{"reason": reason})
		r.ev.Close()
	}
	r.rdr.Close()
	r.closeQuery()
	return err
}

func (r *Runner) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-r.streamErr:
			return fmt.Errorf("runner: stream: %w", err)
		case err := <-r.workerErr:
			return fmt.Errorf("runner: worker: %w", err)
		case err := <-r.routerDone:
			return fmt.Errorf("runner: router: %w", err)
		}
	}
}

// DroppedByWindow returns the number of rows dropped by the window for a
// target table (proof of caught-up state).
func (r *Runner) DroppedByWindow(target string) int64 {
	return r.w.DroppedByWindow(target)
}
