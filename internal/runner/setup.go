package runner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/maintenance"
	"github.com/maltzsama/urutau/internal/plan"
	"github.com/maltzsama/urutau/internal/resume"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// startup collects the release steps newRunner acquires while it builds a
// Runner. One defer runs it when setup fails (r == nil), so a single cleanup
// path releases every resource exactly once instead of the drifted
// per-return lists the function used to hand-roll.
type startup struct {
	// cancel is registered first: it stops the worker, relay, maintenance
	// loops and snapshot watch before any resource they own is released.
	cancel context.CancelFunc
	// relayDone closes when the relay goroutine has fully returned, so the
	// socket it may still be sending on (ingest) can be closed without a
	// send-on-closed-channel race.
	relayDone <-chan struct{}
	// ingest is the worker's input channel; closing it lets the worker's
	// router drain and exit (it ranges over the channel).
	ingest  chan worker.Ingest
	closers []func()
}

// add registers one release step; steps run in reverse acquisition order.
func (s *startup) add(f func()) { s.closers = append(s.closers, f) }

// run stops the setup goroutines and releases every resource. The resources
// are released in reverse acquisition order, so a dependency is never torn
// down before a later-acquired owner.
func (s *startup) run() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.relayDone != nil {
		<-s.relayDone
	}
	if s.ingest != nil {
		close(s.ingest)
	}
	for i := len(s.closers) - 1; i >= 0; i-- {
		s.closers[i]()
	}
}

// setup is the state newRunner accumulates while it builds a Runner: each
// phase reads and writes the same fields, so the pipeline reads as a short
// sequence of named methods rather than one long body with a hand-rolled
// cleanup on every error return.
type setup struct {
	ctx    context.Context
	s      *spec.Spec
	cfg    Config
	src    source.Source
	snk    sink.Sink
	ddlRep *ddlReporter
	log    *slog.Logger

	st     startup
	runCtx context.Context
	cancel context.CancelFunc

	ev         *eventlog.Run
	qsrc       source.QuerySource
	closeQuery func()

	refs            []core.TableRef
	resolved        map[string]core.Schema
	wire            map[string]core.Schema
	casts           map[string]core.CastPolicy
	specBySource    map[string]spec.Table
	specByTarget    map[string]spec.Table
	cdcRefs         []core.TableRef
	incrRefs        []core.TableRef
	incrRefByTarget map[string]core.TableRef

	enrichStages        []*enrich.Stage
	enrichStageByTarget map[string]*enrich.Stage

	writers map[string]sink.TableWriter
	modes   map[string]dataplane.WriteMode

	ingest chan worker.Ingest
	w      *worker.Worker
	r      *Runner

	start             position.Position
	needsSnapshot     []core.TableRef
	snapshotRefs      []core.TableRef
	adoptRefs         []core.TableRef
	caps              source.Capabilities
	heldRows          map[string]bool
	bootstrapByTarget map[string]spec.Bootstrap

	rdr        source.Reader
	router     *relay
	relayDone  chan struct{}
	workerErr  chan error
	routerDone chan error
	workerDone chan struct{}
}

// newRunner builds and starts the collapsed pipeline. It runs as a short
// sequence of named phases over a setup: the setup context is created and
// registered first, so a failure cancels the goroutines started along the way
// and one deferred startup.run releases every acquired resource. r is set
// only once every phase has succeeded, so any failure takes the single
// cleanup path.
func newRunner(ctx context.Context, s *spec.Spec, cfg Config, src source.Source, snk sink.Sink, ddlRep *ddlReporter) (r *Runner, err error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	b := &setup{ctx: ctx, s: s, cfg: cfg, src: src, snk: snk, ddlRep: ddlRep, log: cfg.Logger}
	defer func() {
		if r == nil {
			b.st.run()
		}
	}()

	// Setup-owned context FIRST: the worker, relay, maintenance loops and
	// snapshot watch all run under it, and a failure must stop them.
	b.runCtx, b.cancel = context.WithCancel(ctx)
	b.st.cancel = b.cancel

	if err := b.expandTables(); err != nil {
		return nil, err
	}
	if err := b.rejectCollapsedPartitioning(); err != nil {
		return nil, err
	}
	if err := b.openEventlog(); err != nil {
		return nil, err
	}
	if err := b.introspectPlan(); err != nil {
		return nil, err
	}
	if err := b.openWriters(); err != nil {
		return nil, err
	}
	b.buildWorker()
	if err := b.startMaintenance(); err != nil {
		return nil, err
	}
	b.registerCallbacks()
	b.startWorker()
	if err := b.resolveResume(); err != nil {
		return nil, err
	}
	if err := b.openReaderAndRelay(); err != nil {
		return nil, err
	}
	if err := b.runSnapshotPhase(); err != nil {
		return nil, err
	}
	if err := b.runIncrementalPhase(); err != nil {
		return nil, err
	}

	b.r.workerErr = b.workerErr
	b.r.routerDone = b.routerDone
	b.r.workerDone = b.workerDone
	r = b.r
	return r, nil
}

// expandTables resolves the effective table list: a discovery pipeline lists
// no tables and the source enumerates them now. Writing the expanded list back
// into s.Tables makes every later loop (rejectCollapsedPartitioning,
// introspection, enrich, plan lookups) see the discovered set without
// threading a second list through each. The expansion itself is the shared
// plan.ExpandTables, so both modes agree on discovery (#718 step 2).
func (b *setup) expandTables() error {
	tables, err := plan.ExpandTables(b.ctx, b.src, b.s)
	if err != nil {
		return fmt.Errorf("runner: %w", err)
	}
	b.s.Tables = tables
	return nil
}

// rejectCollapsedPartitioning refuses workers>1 in the collapsed runner: it
// has a single in-process worker per table and would silently ignore the
// partition count (WK-001 C0). NewRunnerWithAdapters reaches this path with
// caller-owned adapters, so the guard lives here too.
func (b *setup) rejectCollapsedPartitioning() error {
	return rejectCollapsedPartitioning(b.s)
}

// openEventlog starts the audit trail, wires destructive-DDL reports into it,
// and registers the sink/query release steps. The sink is owned by the
// adapters path; a startup failure must release it.
func (b *setup) openEventlog() error {
	ev, onFail, err := openEventlog(b.ctx, b.s, b.cfg)
	if err != nil {
		return err
	}
	b.ev = ev
	if onFail != nil {
		b.st.add(onFail)
	}
	// The source reader reports destructive DDL through the reporter captured
	// at OpenSource time; from here it lands in the audit trail (issue #671).
	if b.ddlRep != nil {
		b.ddlRep.set(func(d source.DestructiveDDL) {
			if b.ev != nil {
				_ = b.ev.Emit(context.Background(), eventlog.KindDestructiveDDL, map[string]any{
					"source": d.Source,
					"kind":   d.Kind,
					"table":  d.Table,
					"detail": d.Detail,
				})
			}
			b.log.Warn("runner: destructive DDL on the source; not propagated to the sink",
				"source", d.Source, "kind", d.Kind, "table", d.Table)
		})
	}

	// A startup failure must release the sink (the adapters path owns it).
	b.st.add(func() { _ = b.snk.Close() })

	// The SQL surface (chunked snapshot SELECTs) is optional: a stream source
	// (kafka) has no query connection. The source owns that connection; the
	// runner only releases it on shutdown.
	qsrc, _ := b.src.(source.QuerySource)
	if qsrc != nil {
		b.qsrc = qsrc
		b.closeQuery = func() { _ = qsrc.CloseQuery() }
		b.st.add(b.closeQuery)
	}
	return nil
}

// introspectPlan resolves source tables into the SOURCE schema (what the
// worker encodes and the wire carries) and the RESOLVED schema (the sink's
// target shape), splits the tables by sync mode, and indexes the spec tables
// for plan parameters. The resolution itself is the shared plan.Introspect —
// the same introspection, cast policy, schema resolution and enrich wildcard
// expansion the coordinator runs (#718 step 2).
func (b *setup) introspectPlan() error {
	// The enrich stages this runner owns are built as plan resolves each table
	// (resolveEnrichColumns), so a wildcard reference's real columns are known
	// before sink DDL and the stage is warmed with the ONE reference query it
	// has always run — no separate wildcard pass.
	b.enrichStageByTarget = make(map[string]*enrich.Stage, len(b.s.Tables))
	// Cast warnings surface here, once, from the resolver. The source schemas
	// also feed the schema-drift check.
	p, err := plan.Introspect(b.ctx, b.src, b.s.Tables, plan.Options{
		Logger:  b.log,
		Resolve: b.resolveEnrichColumns,
	})
	if err != nil {
		return err
	}
	b.refs, b.resolved, b.wire = p.Refs, p.Resolved, p.Wire
	b.casts = p.Casts

	// Lookup spec tables by source and target for plan parameters.
	b.specBySource = make(map[string]spec.Table, len(b.s.Tables))
	b.specByTarget = make(map[string]spec.Table, len(b.s.Tables))
	for _, t := range b.s.Tables {
		b.specBySource[t.Source] = t
		b.specByTarget[t.Target] = t
	}

	// Split by sync mode (#157): incremental tables do not use the slot,
	// publication or CDC resume — they run a cursor pass below. Everything
	// else stays on the CDC path.
	b.incrRefByTarget = map[string]core.TableRef{}
	for _, ref := range p.Refs {
		if b.specBySource[ref.Source].Mode == spec.ModeIncremental {
			b.incrRefs = append(b.incrRefs, ref)
			b.incrRefByTarget[ref.Target] = ref
		} else {
			b.cdcRefs = append(b.cdcRefs, ref)
		}
	}
	return nil
}

// resolveEnrichColumns is the runner's plan.Resolver: it builds each table's
// long-lived join stage and resolves the reference destination columns through
// it — the explicit selects plus, for a wildcard select, the real names
// LoadWildcards runs the query to discover (#56). Doing both here keeps ONE
// reference query and warms the stage synchronously, exactly as before; plan
// owns the schema extension and applies the returned destination names to the
// wire and resolved shapes before EnsureTable. Every stage is registered for
// release the moment it exists, so a later failure stops it.
func (b *setup) resolveEnrichColumns(_ context.Context, t spec.Table, sourceSchema core.Schema) ([]string, error) {
	if len(t.Enrich) == 0 {
		return nil, nil
	}
	st, serr := enrich.New(t.Enrich, sourceSchema, b.log)
	if serr != nil {
		return nil, fmt.Errorf("runner: %s: %w", t.Target, serr)
	}
	b.st.add(st.Stop)
	if serr := st.LoadWildcards(b.ctx); serr != nil {
		return nil, fmt.Errorf("runner: %s: enrich: %w", t.Target, serr)
	}
	b.enrichStageByTarget[t.Target] = st
	b.enrichStages = append(b.enrichStages, st)
	return st.RefColumns(), nil
}

// openWriters ensures every target table exists through the sink and opens its
// writer. The table's write shape is resolved once: the sink's DDL (where the
// engine is chosen) and the worker's collapse must agree on it.
func (b *setup) openWriters() error {
	writers := make(map[string]sink.TableWriter, len(b.refs))
	modes := make(map[string]dataplane.WriteMode, len(b.refs))
	for _, ref := range b.refs {
		t := b.specBySource[ref.Source]
		cast := b.casts[ref.Source]
		mode := t.WriteMode.ChangeMode()
		if err := dataplane.RequireUpsertKey(ref.Target, ref.PrimaryKey, mode); err != nil {
			return fmt.Errorf("runner: %w", err)
		}
		if err := b.snk.EnsureTable(b.ctx, ref, b.resolved[ref.Source], t.PartitionBy, cast, mode); err != nil {
			return fmt.Errorf("runner: ensure %s: %w", ref.Target, err)
		}
		wr, err := b.snk.Writer(b.ctx, ref, cast, t.Metadata)
		if err != nil {
			return fmt.Errorf("runner: writer %s: %w", ref.Target, err)
		}
		writers[ref.Target] = wr
		modes[ref.Target] = mode
	}
	b.writers, b.modes = writers, modes
	return nil
}

// buildWorker creates the worker and ingest channel, registers each writer and
// starts its enrich stage, then builds the Runner handle. The handle is kept
// on the setup so the outer newRunner's named return stays nil until every
// phase has succeeded — a late failure must still take the cleanup path.
func (b *setup) buildWorker() {
	b.ingest = make(chan worker.Ingest, 1024)
	b.st.ingest = b.ingest
	b.w = worker.New(worker.Config{MaxRows: b.cfg.MaxRows, MaxInterval: b.cfg.MaxInterval})
	for target, wr := range b.writers {
		mode := b.modes[target]
		b.w.Register(target, wr, mode)
		if mode == dataplane.AppendMode && b.specByTarget[target].OnDelete == spec.OnDeleteSkip {
			b.w.SetDropDeletes(target, true)
		}
		// The drift check knows the source (wire) schema (with its types, so
		// nested struct drift is caught) per table.
		if cs := canonicalForTarget(b.wire, b.refs, target); len(cs.Columns) > 0 {
			b.w.SetKnownSchema(target, cs)
		}
		// A wildcard reference is already warm; Start's remaining first loads
		// (explicit-select references, plus the refresh ticker) stay
		// asynchronous.
		if st, ok := b.enrichStageByTarget[target]; ok {
			st.Start(b.ctx)
			b.w.SetEnricher(target, st)
		}
	}
	b.r = &Runner{
		w:                  b.w,
		log:                b.log,
		ev:                 b.ev,
		snk:                b.snk,
		enrichStages:       b.enrichStages,
		closeQuery:         b.closeQuery,
		cancel:             b.cancel,
		committedPositions: make(map[string]position.Position),
		delivered:          make(map[string]position.Position),
	}
}

// startMaintenance starts one in-process maintenance loop per target table
// (issue #96): there is no separate worker process to hand the pass to. The
// sink is checked through the neutral sink.Maintainable interface; maintenance
// is Iceberg-only and spec.Validate rejects it elsewhere, so a sink lacking
// the capability is a bug, not a config to tolerate.
func (b *setup) startMaintenance() error {
	if !b.s.Sink.MaintenanceEnabled() {
		return nil
	}
	msnk, ok := b.snk.(sink.Maintainable)
	if !ok {
		return fmt.Errorf("runner: sink %q does not support table maintenance (sink.maintenance is Iceberg-only)", b.s.Sink.Type)
	}
	sched := maintenance.NewSchedule()
	for _, ref := range b.refs {
		ref := ref
		// snk.Position reads the table's OWN committed cdc.position rather
		// than an in-memory approximation (compaction runs every few minutes
		// at minimum, so the extra catalog round-trip is not hot-path). A
		// read error just means no position is attached to this pass's
		// compaction commit.
		currentPosition := func() string {
			pos, err := b.snk.Position(b.ctx, ref)
			if err != nil {
				return ""
			}
			return pos
		}
		m := msnk.Maintain(ref, *b.s.Sink.Maintenance, b.log, currentPosition, nil)
		go maintenance.RunLoop(b.runCtx, m, ref.Target, b.s.Sink.Maintenance, sched, b.log)
	}
	return nil
}

// registerCallbacks wires the worker's schema-drift, dropped-delete and commit
// notifications into the audit trail and the confirmed-point bookkeeping.
func (b *setup) registerCallbacks() {
	b.w.OnSchemaDrift(func(d worker.SchemaDrift) {
		b.log.Error("schema drift: pipeline paused", "table", d.Table, "column", d.Column,
			"action", "declare the column in the spec and resume")
		b.r.emit(eventlog.KindSchemaDrift, map[string]any{
			"table": d.Table, "column": d.Column, "kind": d.Kind,
		})
	})
	b.w.OnDroppedDelete(func(table, pos string) {
		b.log.Warn("append-only delete dropped", "table", table, "position", pos,
			"action", "the source carried no before image or the table declares onDelete: skip")
		b.r.emit(eventlog.KindDeleteDropped, map[string]any{
			"table": table, "position": pos,
		})
	})
	b.w.OnCommit(func(bt *dataplane.Batch, rows int) {
		up, del := dpint.CountOps(bt)
		b.log.Info("commit", "table", bt.Table, "rows", rows,
			"upserts", up, "deletes", del, "position", string(bt.Watermark))
		b.r.emit(eventlog.KindCommit, map[string]any{
			"table": bt.Table, "rows": rows,
			"upserts": up, "deletes": del, "position": string(bt.Watermark),
		})
		// An incremental table's cursor is the batch watermark, and never
		// feeds the LSN/confirmed point, so the position parse does not apply.
		if _, ok := b.incrRefByTarget[bt.Table]; ok {
			return
		}
		// A garbage position string never advances the confirmed point.
		p, err := b.src.ParsePosition(string(bt.Watermark))
		if err != nil {
			b.log.Warn("runner: commit position parse", "table", bt.Table, "position", string(bt.Watermark), "err", err)
			return
		}
		b.r.updateCommitted(bt.Table, p)
	})
}

// startWorker launches the worker under the setup context and records its
// completion channels; Run waits for workerDone before releasing the sink.
func (b *setup) startWorker() {
	b.workerErr = make(chan error, 1)
	b.workerDone = make(chan struct{})
	go func() { defer close(b.workerDone); b.workerErr <- b.w.Run(b.runCtx, b.ingest) }()
}

// resolveResume resolves the stream resume point and the snapshot/adopt set
// through the shared resume resolver (internal/resume, the same one the
// coordinator runs — #718 step 3), marks the pending snapshots before the
// stream can commit, and resolves the per-table bootstrap configuration the
// stream start and snapshot loop both consult.
func (b *setup) resolveResume() error {
	// Per-table bootstrap config, resolved once: the shared resolver splits
	// an adopt table out of the snapshot set (its snapshot is marked complete
	// without reading), and the snapshot loop reads the mode back for logging.
	b.bootstrapByTarget = make(map[string]spec.Bootstrap, len(b.s.Tables))
	for _, t := range b.s.Tables {
		if t.Bootstrap != nil {
			b.bootstrapByTarget[t.Target] = *t.Bootstrap
		}
	}

	// The source's snapshot capability gates the snapshot routing: a
	// non-snapshotting source (Kafka) has no query connection and must never
	// be routed into the snapshot phase (issue #394). An unregistered kind
	// has no capabilities.
	caps, _ := driver.CapsForKind(b.s.Source.Kind)
	b.caps = caps

	res, err := resume.Resolve(b.ctx, b.src, b.snk, b.cdcRefs, resume.Options{
		Caps:      caps,
		Bootstrap: b.bootstrapByTarget,
	})
	if err != nil {
		return fmt.Errorf("runner: %w", err)
	}
	b.start = res.Start
	b.snapshotRefs, b.adoptRefs = res.Snapshot, res.Adopt
	// The union marks every table the snapshot phase will touch (snapshot or
	// adopt) not_started before the stream can commit to it.
	b.needsSnapshot = append(append([]core.TableRef{}, res.Snapshot...), res.Adopt...)
	b.log.Info("resume", "from", position.StringOrNone(res.Resume), "snapshot_tables", len(b.needsSnapshot))
	if len(res.Recovery) > 0 {
		b.log.Info("crash recovery: streams ahead of the resume point replay from it",
			"from", position.StringOrNone(res.Resume), "streams", res.Recovery)
	}
	b.r.emit(eventlog.KindResume, map[string]any{
		"from": position.StringOrNone(res.Resume), "snapshot_tables": len(b.needsSnapshot),
	})

	// Before the stream can commit anything: a crash from here on must find
	// these tables unfinished, whatever positions the stream commits to them
	// (#428). Skipped when the source does not snapshot (e.g. Kafka).
	if caps.Snapshot {
		heldRows, err := markSnapshotsPending(b.ctx, b.snk, b.needsSnapshot)
		if err != nil {
			return err
		}
		b.heldRows = heldRows
	}
	return nil
}

// openReaderAndRelay opens the one replication connection (only when there are
// CDC tables), installs the source schemas and confirmed-point callback,
// computes the start position, starts the stream, and launches the relay. An
// all-incremental pipeline opens no replication connection at all.
func (b *setup) openReaderAndRelay() error {
	if len(b.cdcRefs) == 0 {
		return nil
	}
	rdr, err := b.src.Open(b.ctx, b.cdcRefs)
	if err != nil {
		return err
	}
	b.st.add(rdr.Close)
	b.rdr = rdr

	// A source that can take the source schema (optional interface) gets it
	// now so the source boundary gates on drift and encodes stable batches
	// with the source types the sink casts.
	if si, ok := rdr.(source.SchemaSetter); ok {
		byTarget := make(map[string]core.Schema, len(b.refs))
		for _, t := range b.s.Tables {
			if cs, ok := b.wire[t.Source]; ok {
				byTarget[t.Target] = cs
			}
		}
		si.SetSourceSchemas(byTarget)
	}
	b.r.rdr = rdr
	rdr.SetConfirmed(b.r.confirmedPosition)

	// start is the resume point, or an adopted table's explicit bootstrap
	// position when there is none (resolved by the shared resolver); a nil
	// start falls back to the source's initial position.
	start := b.start
	if start == nil {
		if m, err := b.src.InitialPosition(b.ctx); err != nil {
			return fmt.Errorf("runner: initial position: %w", err)
		} else {
			start = m
		}
	}
	if err := rdr.Start(b.ctx, start); err != nil {
		return fmt.Errorf("runner: start stream: %w", err)
	}

	// Pull: the reader is pull-based; the relay starts it and routes batches.
	b.router = newRelay(b.ingest, b.w, b.r.deliverFunc(b.src))
	b.routerDone = make(chan error, 1)
	b.relayDone = make(chan struct{})
	b.st.relayDone = b.relayDone
	go func() {
		defer close(b.relayDone)
		b.routerDone <- b.router.run(b.runCtx, rdr)
	}()
	return nil
}

// runSnapshotPhase runs the DBLog snapshot for tables with no committed
// position (or one an earlier run left unfinished), and the adopt path for a
// table whose bootstrap mode adopts existing data. A worker or relay failure
// must abort the snapshot: its blocking handshakes only observe a context, and
// newRunner has not yet handed workerErr/routerDone to Runner.run (#551).
func (b *setup) runSnapshotPhase() error {
	if !b.caps.Snapshot || (len(b.snapshotRefs) == 0 && len(b.adoptRefs) == 0) {
		return nil
	}
	sw := newSnapshotWatch(b.runCtx, b.workerErr, b.routerDone)
	b.st.add(sw.cancel)
	for _, ref := range b.snapshotRefs {
		if err := b.runSnapshotRef(sw.ctx, ref); err != nil {
			return err
		}
	}
	for _, ref := range b.adoptRefs {
		if err := b.runSnapshotRef(sw.ctx, ref); err != nil {
			return err
		}
	}
	// The snapshot phase finished: stop the failure watch and surface a
	// worker/relay death it caught (Runner.run would otherwise block on a
	// channel the watch already drained).
	return sw.stop()
}

// runSnapshotRef runs the boot snapshot or adopt for one table, taking the
// mode from the table's bootstrap block (snapshot when absent).
func (b *setup) runSnapshotRef(ctx context.Context, ref core.TableRef) error {
	bootstrapMode := spec.BootstrapSnapshot
	if bo, ok := b.bootstrapByTarget[ref.Target]; ok {
		bootstrapMode = bo.Mode
	}
	return b.r.runSnapshot(ctx, ref, bootstrapMode, b.qsrc, b.snk, b.rdr, b.router, b.w, b.cfg, b.heldRows, b.log)
}

// runIncrementalPhase pages each incremental table to exhaustion (#157/#572),
// through the same worker/ingest path as a snapshot. No slot, no window.
func (b *setup) runIncrementalPhase() error {
	if len(b.incrRefs) == 0 {
		return nil
	}
	return b.r.runIncremental(b.ctx, b.src, b.snk, b.incrRefs, b.specBySource, b.w, b.ingest)
}
