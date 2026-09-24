// Package runner wires the collapsed process: one binary runs the source
// reader, the DBLog snapshot, and the worker in a single process (local
// mode). The gRPC/Flight split arrives with multi-worker support.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/maintenance"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
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

// relay pumps reader events into the worker's ingest channel and releases
// the DBLog chunk markers. Window tagging happens in the reader at decode
// time; the marker's Release first drains the pump, so every event decoded
// inside the window is already enqueued ahead of it. The gate mirrors the
// coordinator's pump gate: while a chunk SELECT is in flight the table's
// live events are buffered and released InWindow-tagged only after
// AddWindowRows populates the worker window — the ordering the window proof
// needs (a live event must never deduplicate against an empty window).
type relay struct {
	ingest   chan<- worker.Ingest
	window   *worker.Worker
	flushReq chan chan struct{}

	gateMu       sync.Mutex
	gateOn       bool
	gateTgt      string
	gateChk      uint32
	gateBuf      []*dataplane.Batch
	flushGate    bool
	gateFlushReq chan chan struct{}
}

func newRelay(ingest chan<- worker.Ingest, window *worker.Worker) *relay {
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
	r.ingest <- worker.Ingest{
		Table:    table,
		Position: at.String(),
		Win:      &rowchange.Window{ChunkID: chunkID, Closes: true},
	}
}

func (r *relay) AddWindowRows(target string, chunkID uint32, rows []rowchange.Change) error {
	// Build the wire record against the introspected schema (the worker's
	// known schema), never a per-batch inference. Snapshot chunk rows are
	// inserts only, so the C-8 delete guard does not apply. MergeSchema
	// keeps the known shape and only appends columns a row carries that it
	// lacks.
	rec, err := transport.RecordFromChanges(rows, transport.MergeSchema(rows, r.window.KnownSchema(target)), nil)
	if err != nil {
		return err
	}
	dpb := &dataplane.Batch{Table: target, Record: rec, Mode: dataplane.AppendMode}
	// The worker window takes ownership of the batch.
	return r.window.AddWindowRows(target, chunkID, dpb)
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
func (r *relay) gate(b *dataplane.Batch) bool {
	r.gateMu.Lock()
	defer r.gateMu.Unlock()
	if !r.gateOn || b.Table != r.gateTgt {
		return false
	}
	r.gateBuf = append(r.gateBuf, b)
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

	for _, b := range buf {
		select {
		case r.ingest <- worker.Ingest{Table: b.Table, Batch: b, Win: &rowchange.Window{ChunkID: chunkID, InWindow: true}}:
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
	return true, nil
}

// run pulls columnar batches from the reader and routes them into the
// worker's ingest channel, gating them during a chunk SELECT (ordering the
// window proof needs: a live event must never deduplicate against an empty
// window). Release drains decoded events ahead of the Closes marker.
func (r *relay) run(ctx context.Context, rdr source.Reader) error {
	batchCh := make(chan *dataplane.Batch, 16)
	// readErr carries the puller's failure to the consumer below. A closed
	// batchCh alone cannot distinguish "the source ended" from "the source
	// broke": discarding the error here would let a mid-stream failure end
	// the pipeline as a clean, successful run with silently truncated data —
	// the same class of bug fixed in the plugin reader, one layer up. Written
	// once before close(batchCh), read only after the channel is drained, so
	// the close/read pair carries the happens-before edge.
	var readErr error
	go func() {
		defer close(batchCh)
		for {
			b, err := rdr.Next(ctx)
			if err != nil {
				readErr = err

				return
			}
			if b == nil {
				return
			}
			select {
			case batchCh <- b:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		if flushed, err := r.drainGate(ctx); err != nil {
			return err
		} else if flushed {
			continue
		}
		select {
		case b, ok := <-batchCh:
			if !ok {
				if _, err := r.drainGate(ctx); err != nil {
					return err
				}

				return readErr // nil on a clean end of stream
			}
			if r.gate(b) {
				continue
			}
			select {
			case r.ingest <- worker.Ingest{Table: b.Table, Batch: b}:
			case <-ctx.Done():
				return ctx.Err()
			}
		case req := <-r.flushReq:
			// Drain everything already decoded into ingest, then ack.
		drain:
			for {
				select {
				case b, ok := <-batchCh:
					if !ok {
						break drain
					}
					if r.gate(b) {
						continue
					}
					select {
					case r.ingest <- worker.Ingest{Table: b.Table, Batch: b}:
					case <-ctx.Done():
						close(req)
						return ctx.Err()
					}
				default:
					// give the puller a beat to flush its buffer
					select {
					case b, ok := <-batchCh:
						if !ok {
							break drain
						}
						if r.gate(b) {
							continue
						}
						select {
						case r.ingest <- worker.Ingest{Table: b.Table, Batch: b}:
						case <-ctx.Done():
							close(req)
							return ctx.Err()
						}
					default:
						break drain
					}
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

func resumeFrom(ctx context.Context, src source.Source, snk sink.Sink, refs []core.TableRef) (position.Position, []core.TableRef, []string, error) {
	var positions []position.Position
	var needsSnapshot []core.TableRef
	byTarget := make(map[string]position.Position, len(refs))
	for _, ref := range refs {
		pos, err := snk.Position(ctx, ref)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("runner: %s: %w", ref.Target, err)
		}
		if pos != "" {
			p, err := src.ParsePosition(pos)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("runner: %s cdc.position %q: %w", ref.Target, pos, err)
			}
			positions = append(positions, p)
			byTarget[ref.Target] = p
		} else {
			needsSnapshot = append(needsSnapshot, ref)
		}
	}
	if len(positions) == 0 {
		return nil, needsSnapshot, nil, nil
	}
	best, err := position.MinSafe(positions)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("runner: %w", err)
	}
	// Streams ahead of the resume point hold already-committed data past it;
	// they replay from best (idempotent under upsert). Naming them makes a
	// crash-recovery replay observable (#155).
	recovery := position.Ahead(best, byTarget)
	return best, needsSnapshot, recovery, nil
}

// runIncremental reads each incremental table once (#157) and pushes the rows
// through the worker's ingest path — the same path a snapshot uses, minus the
// window. The cursor is read from cdc.cursor and persisted post-commit by the
// OnCommit callback, so the data and the cursor advance together.
func (r *Runner) runIncremental(ctx context.Context, src source.Source, snk sink.Sink, refs []core.TableRef, specBySource map[string]spec.Table, w *worker.Worker, ingest chan worker.Ingest) error {
	inc, ok := src.(source.IncrementalSource)
	if !ok {
		return fmt.Errorf("runner: source does not support incremental mode")
	}
	for _, ref := range refs {
		t := specBySource[ref.Source]
		// The committed cdc.position IS the cursor for an incremental table:
		// the batch watermark was written atomically with the rows.
		after, err := snk.Position(ctx, ref)
		if err != nil {
			return fmt.Errorf("runner: incremental %s: %w", ref.Target, err)
		}
		next, rows, err := inc.Incremental(ctx, ref, t.Cursor, after)
		if err != nil {
			return fmt.Errorf("runner: incremental %s: %w", ref.Target, err)
		}
		if len(rows) == 0 {
			continue
		}
		changes := make([]rowchange.Change, len(rows))
		for i, row := range rows {
			key := make([]any, 0, len(ref.PrimaryKey))
			for _, pk := range ref.PrimaryKey {
				key = append(key, row[pk])
			}
			changes[i] = rowchange.Change{
				Op:       rowchange.OpInsert,
				Table:    ref.Target,
				Key:      key,
				After:    row,
				Position: next,
				Snapshot: false,
				Phase:    core.PhaseIncremental,
				IngestTS: time.Now(),
			}
		}
		rec, err := transport.RecordFromChanges(changes, transport.MergeSchema(changes, w.KnownSchema(ref.Target)), nil)
		if err != nil {
			return fmt.Errorf("runner: incremental %s: %w", ref.Target, err)
		}
		dpb := &dataplane.Batch{Table: ref.Target, Record: rec, Mode: dataplane.UpsertMode, Watermark: []byte(next)}
		select {
		case ingest <- worker.Ingest{Table: ref.Target, Batch: dpb}:
		case <-ctx.Done():
			return ctx.Err()
		}
		r.log.Info("incremental read", "table", ref.Source, "rows", len(rows), "cursor", next)
	}
	return nil
}

// readSnapshotProgress reads the snapshot state from the sink's table
// properties.
func readSnapshotProgress(ctx context.Context, snk sink.Sink, ref core.TableRef) (*snapshot.SnapshotProgress, error) {
	props, err := snk.Properties(ctx, ref)
	if err != nil {
		return nil, err
	}
	return snapshot.ReadSnapshotProgress(props)
}

// canonicalForTarget returns the canonical schema for one target table (the
// schema drift reference). Zero schema when the target has no source.
func canonicalForTarget(canonical map[string]core.Schema, refs []core.TableRef, target string) core.Schema {
	for _, ref := range refs {
		if ref.Target == target {
			return canonical[ref.Source]
		}
	}
	return core.Schema{}
}

// introspectAll resolves each spec table through the source, producing both
// the RESOLVED shape (cast types + metadata columns, the sink's target) and
// the WIRE shape (the source types the worker encodes). Cast warnings surface
// here, once, from the resolver.
func introspectAll(ctx context.Context, src source.Source, s *spec.Spec, logger *slog.Logger) (refs []core.TableRef, resolved, wire, sourceSchemas map[string]core.Schema, casts map[string]core.CastPolicy, err error) {
	refs = make([]core.TableRef, 0, len(s.Tables))
	resolved = make(map[string]core.Schema, len(s.Tables))
	wire = make(map[string]core.Schema, len(s.Tables))
	casts = make(map[string]core.CastPolicy, len(s.Tables))
	sourceSchemas = make(map[string]core.Schema, len(s.Tables))
	for _, t := range s.Tables {
		ref, srcSchema, warns, ierr := src.Introspect(ctx, t)
		if ierr != nil {
			return nil, nil, nil, nil, nil, ierr
		}
		cast, cerr := core.ParseCastPolicy(t.Cast)
		if cerr != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("runner: %s: %w", t.Source, cerr)
		}
		res, rwarns, rerr := core.ResolveSchema(srcSchema, cast, t.Metadata)
		if rerr != nil {
			return nil, nil, nil, nil, nil, rerr
		}
		for _, w := range warns {
			logger.Warn("schema", "table", ref.Source, "warning", w.Message)
		}
		for _, w := range rwarns {
			logger.Warn("schema", "table", ref.Source, "warning", w.Message)
		}
		refs = append(refs, ref)
		resolved[t.Source] = res
		// Event columns are captured BEFORE the enrich extension: the
		// reference destinations ride the wire, but they are not event
		// columns — New validates the event side against the source view.
		sourceSchemas[t.Source] = srcSchema
		wire[t.Source] = enrich.AddRefColumns(core.WireSchema(srcSchema, res), t.Enrich)
		resolved[t.Source] = enrich.AddRefColumns(res, t.Enrich)
		casts[t.Source] = cast
	}
	return refs, resolved, wire, sourceSchemas, casts, nil
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
	w                     *worker.Worker
	enrichStages          []*enrich.Stage
	log                   *slog.Logger
	ev                    *eventlog.Run
	rdr                   source.Reader
	closeQuery            func()
	workerErr, routerDone <-chan error

	// committedPositions tracks the latest durably-committed position per
	// target table. The minimum across tables is the confirmed position
	// reported to the source (Postgres slot advancement).
	posMu              sync.Mutex
	committedPositions map[string]position.Position
	minConfirmed       position.Position // recomputed on each commit
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
// NewRunner opens the source and sink through the driver registry — the
// runner consumes only the contracts, never a concrete implementation.
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
	src, err := driver.OpenSource(s, source.Runtime{
		ServerID:  cfg.ServerID,
		Heartbeat: cfg.Heartbeat,
		Logger:    cfg.Logger,
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
	return newRunner(ctx, s, cfg, src, snk)
}

// NewRunnerWithAdapters runs the pipeline over already-open source/sink
// adapters — the external plugin path. The adapters implement the public
// source.Source / sink.Sink contracts; the caller owns parallelism
// validation (a plugin source declares no registry ceiling).
func NewRunnerWithAdapters(ctx context.Context, s *spec.Spec, cfg Config, src source.Source, snk sink.Sink) (*Runner, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return newRunner(ctx, s, cfg, src, snk)
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

func newRunner(ctx context.Context, s *spec.Spec, cfg Config, src source.Source, snk sink.Sink) (r *Runner, err error) {
	log := cfg.Logger

	// A discovery pipeline lists no tables: the source enumerates them now.
	// Writing the expanded list back into s.Tables makes every later loop
	// (rejectCollapsedPartitioning, introspection, enrich, plan lookups) see
	// the discovered set without threading a second list through each.
	tables, err := source.ExpandTables(ctx, src, s)
	if err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}
	s.Tables = tables

	// workers>1 is a distributed-mode contract: the collapsed runner has a
	// single in-process worker per table and would silently ignore the
	// partition count, giving the user one worker when they declared N
	// (WK-001 C0). The distributed coordinator accepts it (and validates the
	// sink capability); here it is a boot error. NewRunnerWithAdapters reaches
	// this path with caller-owned adapters, so the guard lives here too.
	if err := rejectCollapsedPartitioning(s); err != nil {
		return nil, err
	}

	// Audit trail first: job_started marks the boot, and a startup failure
	// still seals the trail with job_stopped.
	var ev *eventlog.Run
	if cfg.Eventlog != nil {
		ec := *cfg.Eventlog
		// Apply the shared key convention: the trail lives under the
		// pipeline name so a reader can discover it by listing.
		if ec.Pipeline == "" {
			ec.Pipeline = s.Pipeline
		}
		e, eerr := eventlog.New(ctx, ec)
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
				_ = ev.Close()
			}
		}()
	}

	// A startup failure must release the sink (the adapters path owns it).
	defer func() {
		if r == nil {
			_ = snk.Close()
		}
	}()

	// The SQL surface (chunked snapshot SELECTs) is optional: a stream source
	// (kafka) has no query connection. The source owns that connection; the
	// runner only releases it on shutdown.
	qsrc, _ := src.(source.QuerySource)
	closeQuery := func() {
		if qsrc != nil {
			_ = qsrc.CloseQuery()
		}
	}

	// Resolve source tables into the SOURCE schema (what the worker encodes
	// and the wire carries) and the RESOLVED schema (the sink's target shape
	// with cast types and metadata columns). The source schemas also feed the
	// schema-drift check: the batcher compares every change against the
	// column set known at introspection time.
	refs, resolved, wire, sourceSchemas, casts, err := introspectAll(ctx, src, s, log)
	if err != nil {
		return nil, err
	}

	// Lookup spec tables by source and target for plan parameters.
	specBySource := make(map[string]spec.Table, len(s.Tables))
	specByTarget := make(map[string]spec.Table, len(s.Tables))
	for _, t := range s.Tables {
		specBySource[t.Source] = t
		specByTarget[t.Target] = t
	}

	// Split by sync mode (#157): incremental tables do not use the slot,
	// publication or CDC resume — they run a cursor pass below. Everything
	// else stays on the CDC path.
	var cdcRefs, incrRefs []core.TableRef
	incrRefByTarget := map[string]core.TableRef{}
	for _, ref := range refs {
		if specBySource[ref.Source].Mode == spec.ModeIncremental {
			incrRefs = append(incrRefs, ref)
			incrRefByTarget[ref.Target] = ref
		} else {
			cdcRefs = append(cdcRefs, ref)
		}
	}

	// Enrich stages are built and, for any wildcard reference, loaded
	// SYNCHRONOUSLY here — BEFORE EnsureTable — so a wildcard select's real
	// destination columns are known in time to correct resolved/wire before
	// the sink table is created (#56). An explicit-select reference is
	// unaffected: LoadWildcards skips it, and Start (below, after the
	// writer/EnsureTable loop) still loads it asynchronously exactly as
	// before. Building the Stage here (rather than in the writer loop,
	// where it lived before this fix) is what makes RefColumns() available
	// before EnsureTable runs.
	enrichStageByTarget := make(map[string]*enrich.Stage, len(s.Tables))
	var enrichStages []*enrich.Stage
	closeStages := func() {
		for _, st := range enrichStages {
			st.Stop()
		}
	}
	for _, t := range s.Tables {
		if len(t.Enrich) == 0 {
			continue
		}
		st, serr := enrich.New(t.Enrich, sourceSchemas[t.Source], log)
		if serr != nil {
			return nil, fmt.Errorf("runner: %s: %w", t.Target, serr)
		}
		if serr := st.LoadWildcards(ctx); serr != nil {
			closeStages()
			return nil, fmt.Errorf("runner: %s: enrich: %w", t.Target, serr)
		}
		dests := st.RefColumns()
		wire[t.Source] = enrich.AddColumns(wire[t.Source], dests)
		resolved[t.Source] = enrich.AddColumns(resolved[t.Source], dests)
		enrichStageByTarget[t.Target] = st
		enrichStages = append(enrichStages, st)
	}

	// Writers, ensuring tables exist through the sink. The table's write
	// shape is resolved once: the sink's DDL (where the engine is chosen)
	// and the worker's collapse must agree on it. resolved[ref.Source] is
	// now correct even for a wildcard reference, per the loop above.
	writers := make(map[string]sink.TableWriter, len(refs))
	modes := make(map[string]dataplane.WriteMode, len(refs))
	for _, ref := range refs {
		t := specBySource[ref.Source]
		cast := casts[ref.Source]
		mode := t.WriteMode.ChangeMode()
		if err := dataplane.RequireUpsertKey(ref.Target, ref.PrimaryKey, mode); err != nil {
			closeStages()
			return nil, fmt.Errorf("runner: %w", err)
		}
		if err := snk.EnsureTable(ctx, ref, resolved[ref.Source], t.PartitionBy, cast, mode); err != nil {
			closeStages()
			return nil, fmt.Errorf("runner: ensure %s: %w", ref.Target, err)
		}
		wr, err := snk.Writer(ctx, ref, cast, t.Metadata)
		if err != nil {
			closeStages()
			return nil, fmt.Errorf("runner: writer %s: %w", ref.Target, err)
		}
		writers[ref.Target] = wr
		modes[ref.Target] = mode
	}

	// Worker + ingest channel: the relay feeds columnar Ingest batches
	// pulled from the source's pull-based reader (M4).
	ingest := make(chan worker.Ingest, 1024)
	w := worker.New(worker.Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval})
	for target, wr := range writers {
		mode := modes[target]
		w.Register(target, wr, mode)
		if mode == dataplane.AppendMode && specByTarget[target].OnDelete == spec.OnDeleteSkip {
			w.SetDropDeletes(target, true)
		}
		// The drift check knows the source (wire) schema (with its types, so
		// nested struct drift is caught) per table.
		if cs := canonicalForTarget(wire, refs, target); len(cs.Columns) > 0 {
			w.SetKnownSchema(target, cs)
		}
		// Enrichment: broadcast reference joins declared for this table,
		// built and (for any wildcard reference) synchronously warmed
		// above, before EnsureTable. Start's remaining first loads
		// (explicit-select references, plus the refresh ticker for
		// everything) stay asynchronous — the cold-start policy governs
		// whatever hasn't loaded yet.
		if st, ok := enrichStageByTarget[target]; ok {
			st.Start(ctx)
			w.SetEnricher(target, st)
		}
	}
	r = &Runner{w: w, log: log, ev: ev, enrichStages: enrichStages, closeQuery: func() {
		if qsrc != nil {
			_ = qsrc.CloseQuery()
		}
	},
		committedPositions: make(map[string]position.Position)}

	// Iceberg table maintenance (issue #96): the collapsed runner schedules
	// maintenance in-process — one loop per target table, for the life of
	// the pipeline (ctx, not a narrower setup-only context) — because there
	// is no separate worker process to hand the pass to. The distributed
	// coordinator instead provisions an ephemeral maintenance worker per
	// table (see its scheduler). Checked against the NEUTRAL sink.Maintainable
	// interface — never a concrete sink package, which
	// internal/architecture's TestOrchestrationConsumesContracts forbids
	// the runner from importing. Maintenance is Iceberg-only (spec.Validate
	// rejects it on any other sink type), so a sink that does not implement
	// the capability here is a bug, not a configuration to tolerate: fail
	// loudly, matching requireConcurrentSink's hard capability check in the
	// coordinator, rather than silently dropping the operator's maintenance
	// block. Skipped entirely (no goroutine at all) when Maintenance is nil
	// or disabled, so a pipeline that never configured it pays nothing.
	if s.Sink.MaintenanceEnabled() {
		msnk, ok := snk.(sink.Maintainable)
		if !ok {
			return nil, fmt.Errorf("runner: sink %q does not support table maintenance (sink.maintenance is Iceberg-only)", s.Sink.Type)
		}
		sched := maintenance.NewSchedule()
		for _, ref := range refs {
			ref := ref
			// snk.Position reads the table's OWN committed cdc.position
			// (CommittedPosition, walk-back included), rather than an
			// in-memory approximation — compaction only runs every few
			// minutes at minimum, so the extra catalog round-trip is not a
			// hot-path cost, and reading the authoritative value avoids
			// coupling the Maintainer to the runner's/coordinator's
			// internal bookkeeping (which differ in shape between the two).
			// A read error just means no position is attached to this
			// pass's compaction commit — not fatal, logged by compact
			// itself via its normal error path if it ever surfaces there.
			currentPosition := func() string {
				pos, err := snk.Position(ctx, ref)
				if err != nil {
					return ""
				}
				return pos
			}
			m := msnk.Maintain(ref, *s.Sink.Maintenance, log, currentPosition, nil)
			go maintenance.RunLoop(ctx, m, ref.Target, s.Sink.Maintenance, sched, log)
		}
	}

	w.OnSchemaDrift(func(d worker.SchemaDrift) {
		log.Error("schema drift: pipeline paused", "table", d.Table, "column", d.Column,
			"action", "declare the column in the spec and resume")
		r.emit(eventlog.KindSchemaDrift, map[string]any{
			"table": d.Table, "column": d.Column, "kind": d.Kind,
		})
	})
	w.OnDroppedDelete(func(table, pos string) {
		log.Warn("append-only delete dropped", "table", table, "position", pos,
			"action", "the source carried no before image or the table declares onDelete: skip")
		r.emit(eventlog.KindDeleteDropped, map[string]any{
			"table": table, "position": pos,
		})
	})
	w.OnCommit(func(b *dataplane.Batch, rows int) {
		up, del := worker.CountOps(b)
		log.Info("commit", "table", b.Table, "rows", rows,
			"upserts", up, "deletes", del, "position", string(b.Watermark))
		r.emit(eventlog.KindCommit, map[string]any{
			"table": b.Table, "rows": rows,
			"upserts": up, "deletes": del, "position": string(b.Watermark),
		})
		// An incremental table's cursor is the batch watermark, written
		// atomically with the rows by the sink's commit — never a separate
		// property, which would race the data commit. It never feeds the
		// LSN/confirmed point, so the position parse does not apply.
		if _, ok := incrRefByTarget[b.Table]; ok {
			return
		}
		// A garbage position string never advances the confirmed point.
		p, err := src.ParsePosition(string(b.Watermark))
		if err != nil {
			log.Warn("runner: commit position parse", "table", b.Table, "position", string(b.Watermark), "err", err)
			return
		}
		r.updateCommitted(b.Table, p)
	})
	workerErr := make(chan error, 1)
	go func() { workerErr <- w.Run(ctx, ingest) }()

	resume, needsSnapshot, recovery, err := resumeFrom(ctx, src, snk, cdcRefs)
	if err != nil {
		closeQuery()
		closeStages()
		closeStages()
		return nil, err
	}
	log.Info("resume", "from", resumeOrNone(resume), "snapshot_tables", len(needsSnapshot))
	if len(recovery) > 0 {
		log.Info("crash recovery: streams ahead of the resume point replay from it",
			"from", resumeOrNone(resume), "streams", recovery)
	}
	r.emit(eventlog.KindResume, map[string]any{
		"from": resumeOrNone(resume), "snapshot_tables": len(needsSnapshot),
	})

	// Per-table bootstrap config, resolved once: the stream start below and
	// the snapshot loop both consult it.
	bootstrapByTarget := make(map[string]spec.Bootstrap, len(s.Tables))
	var explicitPositions []position.Position
	for _, t := range s.Tables {
		if t.Bootstrap == nil {
			continue
		}
		bootstrapByTarget[t.Target] = *t.Bootstrap
		if t.Bootstrap.StartAt == spec.StartAtExplicit && t.Bootstrap.Position != "" {
			p, err := src.ParsePosition(t.Bootstrap.Position)
			if err != nil {
				closeQuery()
				closeStages()
				closeStages()
				closeStages()
				return nil, fmt.Errorf("runner: %s bootstrap.position %q: %w", t.Target, t.Bootstrap.Position, err)
			}
			explicitPositions = append(explicitPositions, p)
		}
	}

	// Reader (one replication connection) → relay → ingest, only when there
	// are CDC tables. The reader constructor also performs the source's
	// server-side setup (Postgres slot and publication). An all-incremental
	// pipeline opens no replication connection at all.
	var rdr source.Reader
	var router *relay
	var routerDone chan error
	if len(cdcRefs) > 0 {
		rdr, err = src.Open(ctx, cdcRefs)
		if err != nil {
			closeQuery()
			closeStages()
			closeStages()
			return nil, err
		}
		// A source that can take the source schema (optional interface) gets
		// it now so the source boundary gates on drift and encodes stable
		// batches with the source types the sink casts.
		if si, ok := rdr.(source.SchemaSetter); ok {
			byTarget := make(map[string]core.Schema, len(refs))
			for _, t := range s.Tables {
				if cs, ok := wire[t.Source]; ok {
					byTarget[t.Target] = cs
				}
			}
			si.SetSourceSchemas(byTarget)
		}
		r.rdr = rdr
		rdr.SetConfirmed(r.confirmedPosition)

		start := resume
		if start == nil && len(explicitPositions) > 0 {
			// An adopted table with an explicit start position overrides the
			// source default. The minimum across tables is the safe choice:
			// the stream is one per source, and starting too late would skip
			// data.
			start = position.Min(explicitPositions)
		}
		if start == nil {
			if m, err := src.InitialPosition(ctx); err != nil {
				return nil, fmt.Errorf("runner: initial position: %w", err)
			} else {
				start = m
			}
		}

		// Pull: the reader is pull-based; the relay starts it and routes
		// batches.
		if err := rdr.Start(ctx, start); err != nil {
			return nil, fmt.Errorf("runner: start stream: %w", err)
		}
		router = newRelay(ingest, w)
		routerDone = make(chan error, 1)
		go func() { routerDone <- router.run(ctx, rdr) }()
	}

	// Snapshot phase: DBLog for tables with no committed position. Skip
	// when the source does not support snapshot (e.g. Kafka).
	caps, _ := driver.CapsForKind(s.Source.Kind)
	if caps.Snapshot {
		for _, ref := range needsSnapshot {
			bootstrapMode := spec.BootstrapSnapshot
			if b, ok := bootstrapByTarget[ref.Target]; ok {
				bootstrapMode = b.Mode
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
				if err := snk.SetProperties(ctx, ref, props); err != nil {
					rdr.Close()
					closeQuery()
					closeStages()
					closeStages()
					closeStages()
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
				chunker, err := qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), cfg.ChunkSize)
				if err != nil {
					rdr.Close()
					closeQuery()
					closeStages()
					closeStages()
					closeStages()
					return nil, err
				}
				// Read existing snapshot progress for resumable backfill.
				progress, err := readSnapshotProgress(ctx, snk, ref)
				if err != nil {
					rdr.Close()
					closeQuery()
					closeStages()
					closeStages()
					closeStages()
					return nil, fmt.Errorf("runner: snapshot progress %s: %w", ref.Target, err)
				}
				if progress.State == snapshot.StateInProgress {
					log.Info("snapshot resuming", "table", ref.Source,
						"pending", snapshot.PendingIDs(progress.Pending))
				}
				// Set initial snapshot state on the worker so batches carry it.
				w.SetSnapshotState(ref.Target, string(snapshot.StateInProgress), progress.Pending)
				if progress.State == snapshot.StateInProgress {
					// The bloom guard was recreated empty: keys live events
					// touched before the crash are unknown, so pure appends
					// could duplicate committed rows. Every snapshot row goes
					// through the upsert path on a resumed snapshot.
					w.MarkSnapshotResumed(ref.Target)
				}
				if err := snapshot.SnapshotTable(ctx, chunker, rdr, router, ref.Target, snapshot.SnapshotConfig{
					WindowTimeout: cfg.WindowTimeout,
					CaughtUpPoll:  cfg.CaughtUpPoll,
					Progress:      progress,
					Persist: func(sp snapshot.SnapshotProgress) error {
						return snk.SetProperties(ctx, ref, snapshot.EncodeSnapshotProgress(&sp))
					},
				}, func(table string, completedChunkID uint32, remaining []uint32) {
					w.SetSnapshotState(ref.Target, string(snapshot.StateInProgress), remaining)
				}); err != nil {
					rdr.Close()
					closeQuery()
					closeStages()
					closeStages()
					closeStages()
					return nil, fmt.Errorf("runner: snapshot %s: %w", ref.Source, err)
				}
				// Snapshot complete: mark on the worker.
				w.SetSnapshotState(ref.Target, string(snapshot.StateComplete), nil)
				log.Info("snapshot done", "table", ref.Source)
				r.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source, "target": ref.Target})
			}
		}
	}

	// Incremental pass (#157): one cursor read per incremental table, through
	// the same worker/ingest path as a snapshot. No slot, no window.
	if len(incrRefs) > 0 {
		if err := r.runIncremental(ctx, src, snk, incrRefs, specBySource, w, ingest); err != nil {
			if r.rdr != nil {
				r.rdr.Close()
			}
			closeQuery()
			closeStages()
			closeStages()
			closeStages()
			return nil, err
		}
	}

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
		fields := map[string]any{"reason": reason}
		if err != nil {
			fields["error"] = err.Error() // the underlying error text (issue #351)
		}
		_ = r.ev.Emit(context.Background(), eventlog.KindJobStopped, fields)
		_ = r.ev.Close()
	}
	if r.rdr != nil {
		r.rdr.Close()
	}
	r.closeQuery()
	for _, st := range r.enrichStages {
		st.Stop()
	}
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
// target table (proof of caught-up state).
func (r *Runner) DroppedByWindow(target string) int64 {
	return r.w.DroppedByWindow(target)
}

// updateCommitted is called from the OnCommit callback. It stores the
// latest committed position for a target table and recomputes the minimum
// across all tables.
// updateCommitted records the position a target table durably committed and
// recomputes the pipeline-wide minimum — the value reported to the source so
// its retention never advances past uncommitted data. The minimum uses the
// position's own ordering: LSNs and GTID sets are not lexicographically
// ordered ("0/10" sorts before "0/2" as strings, 16 after 2 as positions),
// and a wrong minimum would advance the slot past data still in flight.
func (r *Runner) updateCommitted(table string, pos position.Position) {
	if pos == nil {
		return
	}
	r.posMu.Lock()
	defer r.posMu.Unlock()
	r.committedPositions[table] = pos
	vals := make([]position.Position, 0, len(r.committedPositions))
	for _, p := range r.committedPositions {
		vals = append(vals, p)
	}
	// MinSafe: an incomparable pair has no safe minimum — nil holds the
	// confirmed point back rather than advancing the slot past uncommitted
	// data (same direction as StringPosition's incomparable).
	best, err := position.MinSafe(vals)
	if err != nil {
		r.log.Warn("runner: incomparable committed positions; not advancing confirmed point", "err", err)
		r.minConfirmed = nil
		return
	}
	r.minConfirmed = best
}

// confirmedPosition returns the minimum committed position across all
// target tables. The Postgres reader uses this to advance the slot's
// confirmed_flush_lsn; nil means nothing is durably committed yet.
func (r *Runner) confirmedPosition() position.Position {
	r.posMu.Lock()
	defer r.posMu.Unlock()
	return r.minConfirmed
}
