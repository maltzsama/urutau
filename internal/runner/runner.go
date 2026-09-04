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
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
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
	// MaxParallelChunks caps concurrent chunk SELECTs during snapshot.
	// Must not exceed the ceiling the source driver declares; 0 means the
	// driver default (serial in the collapsed runner).
	MaxParallelChunks int
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
// inside the window is already enqueued ahead of it. The gate mirrors the
// coordinator's pump gate: while a chunk SELECT is in flight the table's
// live events are buffered and released InWindow-tagged only after
// AddWindowRows populates the worker window — the ordering the window proof
// needs (a live event must never deduplicate against an empty window).
type relay struct {
	ingest   chan<- change.Change
	window   *worker.Worker
	flushReq chan chan struct{}

	gateMu       sync.Mutex
	gateOn       bool
	gateTgt      string
	gateChk      uint32
	gateBuf      []change.Change
	flushGate    bool
	gateFlushReq chan chan struct{}
}

func newRelay(ingest chan<- change.Change, window *worker.Worker) *relay {
	return &relay{
		ingest:       ingest,
		window:       window,
		flushReq:     make(chan chan struct{}, 1),
		gateFlushReq: make(chan chan struct{}, 1),
	}
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

// GateOn starts buffering the table's live events for a chunk SELECT in
// flight. Called by the orchestrator before the SELECT.
func (r *relay) GateOn(table string, chunkID uint32) {
	r.gateMu.Lock()
	r.gateOn = true
	r.gateTgt = table
	r.gateChk = chunkID
	r.gateMu.Unlock()
}

// GateFlush releases the buffered events InWindow-tagged for the chunk. It
// is SYNCHRONOUS: it waits for the pump to drain the gate buffer into ingest
// before returning, so a gated event can never be overtaken by the Closes
// marker that Release sends afterwards. This makes the window deduplication
// (and the droppedByWindow evidence) deterministic.
func (r *relay) GateFlush() {
	r.gateMu.Lock()
	r.flushGate = true
	r.gateMu.Unlock()
	req := make(chan struct{})
	r.gateFlushReq <- req
	<-req
}

// gate buffers an event when the gate is on for its table.
func (r *relay) gate(c change.Change) bool {
	r.gateMu.Lock()
	defer r.gateMu.Unlock()
	if !r.gateOn || c.Table != r.gateTgt {
		return false
	}
	r.gateBuf = append(r.gateBuf, c)
	return true
}

// drainGate writes the pending gate buffer to ingest, InWindow-tagged, and
// turns the gate off. Returns true if a flush was performed. The reader's own
// window decision is preserved: an event it explicitly left untagged (at or
// before its source watermark) stays untagged; only events decoded in the
// GateOn↔OpenWindow gap (reader never saw them) are tagged here.
func (r *relay) drainGate(ctx context.Context) (bool, error) {
	r.gateMu.Lock()
	if !r.flushGate {
		r.gateMu.Unlock()
		return false, nil
	}
	buf := r.gateBuf
	chunkID := r.gateChk
	r.gateBuf = nil
	r.gateOn = false
	r.flushGate = false
	r.gateMu.Unlock()

	for _, c := range buf {
		if c.Window == nil {
			c.Window = &change.Window{ChunkID: chunkID, InWindow: true}
		} else {
			c.Window.ChunkID = chunkID
		}
		select {
		case r.ingest <- c:
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
	return true, nil
}

// run routes reader events into the worker's ingest channel.
func (r *relay) run(ctx context.Context, out <-chan change.Change) error {
	for {
		// The gate buffer is drained before any select: the pump is the
		// only writer to ingest, so a gated table's older events can never
		// be overtaken by its later ones.
		if flushed, err := r.drainGate(ctx); err != nil {
			return err
		} else if flushed {
			continue
		}
		select {
		case c, ok := <-out:
			if !ok {
				// A gate flush may be pending (GateFlush raced this select):
				// flush it before exiting so buffered events are never
				// dropped.
				if _, err := r.drainGate(ctx); err != nil {
					return err
				}
				return nil
			}
			if r.gate(c) {
				continue
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
					if r.gate(c) {
						continue
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
		case req := <-r.gateFlushReq:
			// Flush the gate buffer before acking: GateFlush blocks until
			// the gated events are in ingest, so Release's Closes marker
			// (sent after) can never overtake them.
			if _, err := r.drainGate(ctx); err != nil {
				close(req)
				return err
			}
			close(req)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ── Positions and catalog ────────────────────────────────────────────

func resumeFrom(ctx context.Context, reg *drivers.Registry, cat catalog.Catalog, s *spec.Spec, refs []core.TableRef) (position.Position, []core.TableRef, error) {
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

// readSnapshotProgress reads the snapshot state from Iceberg table properties.
func readSnapshotProgress(ctx context.Context, cat catalog.Catalog, ident table.Identifier) (*snapshot.SnapshotProgress, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		// Table does not exist yet — treat as not started.
		return &snapshot.SnapshotProgress{State: snapshot.StateNotStarted}, nil
	}
	return snapshot.ReadSnapshotProgress(tbl.Properties())
}

// introspectAll resolves each spec table through the registry, so the
// pipeline knows the PK (equality key) and the resolved target shape before
// writing anything. The canonical schema carries the declared cast and
// metadata columns; the Iceberg schema is derived from it.
func introspectAll(ctx context.Context, reg *drivers.Registry, qdb *sql.DB, s *spec.Spec, logger *slog.Logger) ([]core.TableRef, map[string]*iceberg.Schema, map[string]core.Schema, error) {
	refs := make([]core.TableRef, 0, len(s.Tables))
	schemas := make(map[string]*iceberg.Schema, len(s.Tables))
	canonical := make(map[string]core.Schema, len(s.Tables))
	for _, t := range s.Tables {
		ref, cs, is, warns, err := reg.Introspect(ctx, qdb, t)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, w := range warns {
			logger.Warn("schema", "table", ref.Source, "warning", w.Message)
		}
		refs = append(refs, ref)
		schemas[t.Source] = is
		canonical[t.Source] = cs
	}
	return refs, schemas, canonical, nil
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

	// committedPositions tracks the latest committed cdc.position per
	// target table. The minimum across all tables is the confirmed
	// position reported to the source (Postgres slot advancement).
	posMu              sync.Mutex
	committedPositions map[string]string
	minCommitted       string // cached minimum, recomputed on each commit
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
	// The parallel-chunk setting may not exceed the ceiling the source
	// driver declares — fail fast at boot, not mid-snapshot.
	if err := drivers.ValidateParallelism(s.Source.Kind, cfg.MaxParallelChunks); err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}

	// One query connection for chunk SELECTs, schema introspection, and the
	// position queries (caught-up proof, slot state).
	qdb, err := reg.OpenQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("runner: open query db: %w", err)
	}

	// Resolve source tables and their Iceberg schemas.
	refs, schemas, _, err := introspectAll(ctx, reg, qdb, s, log)
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

	// Lookup spec tables by source and target for plan parameters.
	specBySource := make(map[string]spec.Table, len(s.Tables))
	specByTarget := make(map[string]spec.Table, len(s.Tables))
	for _, t := range s.Tables {
		specBySource[t.Source] = t
		specByTarget[t.Target] = t
	}

	writers := make(map[string]sink.TableWriter, len(refs))
	for _, ref := range refs {
		ident := drivers.TargetIdent(s, ref.Target)
		t := specBySource[ref.Source]
		cast, _ := core.ParseCastPolicy(t.Cast)
		if err := drivers.EnsureTable(ctx, cat, ident, schemas[ref.Source], t.PartitionBy, cast); err != nil {
			return nil, fmt.Errorf("runner: ensure %s: %w", ref.Target, err)
		}
		wr, err := drivers.NewTableWriter(ctx, cat, ident, ref.PrimaryKey, cast, t.Metadata, t.Source)
		if err != nil {
			return nil, fmt.Errorf("runner: writer %s: %w", ref.Target, err)
		}
		writers[ref.Target] = wr
	}

	// Worker + ingest channel.
	ingest := make(chan change.Change, 1024)
	w := worker.New(worker.Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval})
	for target, wr := range writers {
		mode := change.UpsertMode
		if specByTarget[target].WriteMode == spec.WriteModeAppend {
			mode = change.AppendMode
		}
		w.Register(target, wr, mode)
	}
	r = &Runner{w: w, log: log, ev: ev, closeQuery: func() { _ = qdb.Close() },
		committedPositions: make(map[string]string)}
	w.OnCommit(func(b change.Batch, rows int) {
		log.Info("commit", "table", b.Table, "rows", rows,
			"upserts", len(b.Upserts), "deletes", len(b.Deletes), "position", b.Position)
		r.emit(eventlog.KindCommit, map[string]any{
			"table": b.Table, "rows": rows,
			"upserts": len(b.Upserts), "deletes": len(b.Deletes), "position": b.Position,
		})
		r.updateCommitted(b.Table, b.Position)
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
	rdr.SetConfirmed(r.confirmedPosition)

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

	// Snapshot phase: DBLog for tables with no committed position. Skip
	// when the source does not support snapshot (e.g. Kafka).
	caps := reg.Caps()
	if caps.Snapshot {
		for _, ref := range needsSnapshot {
			// Check bootstrap mode for this table.
			var bootstrapMode spec.BootstrapMode
			var bootstrapPos string
			for _, t := range s.Tables {
				if t.Target == ref.Target && t.Bootstrap != nil {
					bootstrapMode = t.Bootstrap.Mode
					bootstrapPos = t.Bootstrap.Position
					break
				}
			}

			switch bootstrapMode {
			case spec.Adopt, spec.AdoptVerify:
				// Adopt: mark snapshot complete without reading data.
				log.Info("adopt", "table", ref.Source, "mode", bootstrapMode)
				r.emit(eventlog.KindSnapshotStarted, map[string]any{
					"table": ref.Source, "target": ref.Target, "mode": bootstrapMode,
				})
				// Write complete state to Iceberg properties.
				props := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{
					State: snapshot.StateComplete,
				})
				if err := drivers.SetTableProperties(ctx, cat, drivers.TargetIdent(s, ref.Target), props); err != nil {
					rdr.Close()
					_ = qdb.Close()
					return nil, fmt.Errorf("runner: adopt %s: %w", ref.Target, err)
				}
				w.SetSnapshotState(ref.Target, string(snapshot.StateComplete), nil)
				log.Info("adopt done", "table", ref.Source)
				r.emit(eventlog.KindSnapshotDone, map[string]any{
					"table": ref.Source, "target": ref.Target, "mode": bootstrapMode,
				})
			default:
				// Snapshot: load all data from source.
				log.Info("snapshot", "table", ref.Source)
				r.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source, "target": ref.Target})
				chunker, err := reg.NewChunker(qdb, ref.Source, strings.Join(ref.PrimaryKey, ","), cfg.ChunkSize)
				if err != nil {
					rdr.Close()
					_ = qdb.Close()
					return nil, err
				}
				// Read existing snapshot progress for resumable backfill.
				progress, err := readSnapshotProgress(ctx, cat, drivers.TargetIdent(s, ref.Target))
				if err != nil {
					rdr.Close()
					_ = qdb.Close()
					return nil, fmt.Errorf("runner: snapshot progress %s: %w", ref.Target, err)
				}
				if progress.State == snapshot.StateInProgress {
					log.Info("snapshot resuming", "table", ref.Source,
						"pending", snapshot.PendingIDs(progress.Pending))
				}
				// Set initial snapshot state on the worker so batches carry it.
				w.SetSnapshotState(ref.Target, string(snapshot.StateInProgress), progress.Pending)
				if err := snapshot.SnapshotTable(ctx, chunker, rdr, router, ref.Target, snapshot.SnapshotConfig{
					WindowTimeout: cfg.WindowTimeout,
					CaughtUpPoll:  cfg.CaughtUpPoll,
					Progress:      progress,
					Persist: func(sp snapshot.SnapshotProgress) error {
						return drivers.SetTableProperties(ctx, cat, drivers.TargetIdent(s, ref.Target), snapshot.EncodeSnapshotProgress(&sp))
					},
				}, func(table string, completedChunkID uint32, remaining []uint32) {
					w.SetSnapshotState(ref.Target, string(snapshot.StateInProgress), remaining)
				}); err != nil {
					rdr.Close()
					_ = qdb.Close()
					return nil, fmt.Errorf("runner: snapshot %s: %w", ref.Source, err)
				}
				// Snapshot complete: mark on the worker.
				w.SetSnapshotState(ref.Target, string(snapshot.StateComplete), nil)
				log.Info("snapshot done", "table", ref.Source)
				r.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source, "target": ref.Target})
			}
			// Handle explicit start position for adopted tables.
			if bootstrapMode == spec.Adopt && bootstrapPos != "" {
				// Position will be used by the stream start.
				_ = bootstrapPos
			}
		}
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

// updateCommitted is called from the OnCommit callback. It stores the
// latest committed position for a target table and recomputes the minimum
// across all tables.
func (r *Runner) updateCommitted(table, pos string) {
	if pos == "" {
		return
	}
	r.posMu.Lock()
	defer r.posMu.Unlock()
	r.committedPositions[table] = pos
	best := ""
	for _, p := range r.committedPositions {
		if best == "" || p < best {
			best = p
		}
	}
	r.minCommitted = best
}

// confirmedPosition returns the minimum committed position across all
// target tables. The Postgres reader uses this to advance the slot's
// confirmed_flush_lsn.
func (r *Runner) confirmedPosition() position.Position {
	r.posMu.Lock()
	defer r.posMu.Unlock()
	if r.minCommitted == "" {
		return nil
	}
	p, err := position.ParseLSN(r.minCommitted)
	if err != nil {
		return nil
	}
	return p
}
