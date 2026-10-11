package runner

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/plan"
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
