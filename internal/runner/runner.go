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

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
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
	// onDeliver notes a batch's position as it is read from the source, so
	// the runner can tell delivered (dispatched) from committed per table.
	onDeliver func(table, pos string)

	gateMu       sync.Mutex
	gateOn       bool
	gateTgt      string
	gateChk      uint32
	gateBuf      []*dataplane.Batch
	flushGate    bool
	gateFlushReq chan chan struct{}
}

func newRelay(ingest chan<- worker.Ingest, window *worker.Worker, onDeliver func(table, pos string)) *relay {
	return &relay{
		ingest:       ingest,
		window:       window,
		onDeliver:    onDeliver,
		flushReq:     make(chan chan struct{}, 1),
		gateFlushReq: make(chan chan struct{}, 1),
	}
}

func (r *relay) Release(ctx context.Context, table string, chunkID uint32, at position.Position) error {
	req := make(chan struct{})
	// Every handshake observes ctx: if the relay goroutine has already
	// exited, nobody closes req and the send below would block forever,
	// wedging the boot snapshot with no error (issue #551).
	select {
	case r.flushReq <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case r.ingest <- worker.Ingest{
		Table:    table,
		Position: at.String(),
		Win:      &rowchange.Window{WindowID: uint64(chunkID), Closes: true},
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *relay) AddWindowRows(target string, chunkID uint32, batch *dataplane.Batch) error {
	// The batch is already Arrow (the chunk SELECT was encoded straight into
	// builders, #584); the worker window takes ownership.
	return r.window.AddWindowRows(target, uint64(chunkID), batch)
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
func (r *relay) GateFlush(ctx context.Context) error {
	r.gateMu.Lock()
	r.flushGate = true
	r.gateMu.Unlock()
	req := make(chan struct{})
	select {
	case r.gateFlushReq <- req:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// deliverNote reports a batch's last position to the runner as it is read
// from the source, before it is routed or gated. It is the "delivered" half
// of the confirmed-point rule: a table with delivered past its committed has
// data still in flight, so the confirmed position must not advance past its
// committed.
func (r *relay) deliverNote(b *dataplane.Batch) {
	if r.onDeliver == nil || b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		return
	}
	reader, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		return
	}
	r.onDeliver(b.Table, reader.Position(reader.NumRows()-1))
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
		case r.ingest <- worker.Ingest{Table: b.Table, Batch: b, Win: &rowchange.Window{WindowID: uint64(chunkID), InWindow: true}}:
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
	drainer, _ := rdr.(source.Drainer)

	// readerMu serializes rdr.Next and rdr.Drain: both read the reader's own
	// buffer, so a Release flush must never interleave a pull and reorder the
	// InWindow events ahead of the Closes marker. drainReq asks the puller
	// goroutine — the sole reader — to flush the reader's buffer into batchCh.
	var readerMu sync.Mutex
	drainReq := make(chan *drainRequest, 1)

	// pushBatch routes one batch into batchCh, applying the delivered-note
	// bookkeeping the normal pull does.
	pushBatch := func(b *dataplane.Batch) error {
		r.deliverNote(b)
		select {
		case batchCh <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	go func() {
		defer close(batchCh)
		for {
			// Service a pending drain before pulling: flush the reader's
			// buffered events into batchCh, in order.
			select {
			case req := <-drainReq:
				if err := r.serviceDrain(ctx, req, drainer, &readerMu, pushBatch); err != nil {
					readErr = err
					return
				}
				continue
			default:
			}

			readerMu.Lock()
			b, err := rdr.Next(ctx)
			if err != nil {
				readerMu.Unlock()
				readErr = err
				return
			}
			if b == nil {
				readerMu.Unlock()
				return
			}
			err = pushBatch(b)
			readerMu.Unlock()
			if err != nil {
				readErr = err
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
			// Drain the reader's own buffer ahead of the Closes marker, so no
			// InWindow event is overtaken (issue #488).
			if err := r.flushDecoded(ctx, drainer, drainReq, batchCh); err != nil {
				close(req)
				return err
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
			// A position does not prove the snapshot finished: the stream
			// commits to a table before and during its snapshot (#428).
			props, err := snk.Properties(ctx, ref)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("runner: %s: snapshot state: %w", ref.Target, err)
			}
			if snapshot.Unfinished(props) {
				needsSnapshot = append(needsSnapshot, ref)
			}
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

// runIncremental drains each incremental table in bounded pages (#572),
// pushing the rows through the worker's ingest path — the same path a snapshot
// uses, minus the window. The cursor is read from cdc.cursor and persisted
// post-commit by the OnCommit callback, so the data and the cursor advance
// together.
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
		// Drain the table a page at a time: the source returns one bounded page
		// and says whether more is already available, so a large table never
		// loads entirely into memory (issue #572).
		for {
			next, rows, more, err := inc.Incremental(ctx, ref, t.Cursor, after)
			if err != nil {
				return fmt.Errorf("runner: incremental %s: %w", ref.Target, err)
			}
			if len(rows) == 0 {
				break
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
			after = next
			if !more {
				break
			}
		}
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
			return nil, nil, nil, nil, nil, fmt.Errorf("runner: table %s: %w", t.Target, rerr)
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

// markSnapshotsPending marks each table about to be snapshotted not_started,
// unless an earlier run already left it unfinished (its in_progress carries
// resumable bounds). It returns the tables that already hold committed rows:
// the bloom guard of this run never saw the keys an earlier run wrote, so
// their snapshot rows must take the upsert path.
func markSnapshotsPending(ctx context.Context, snk sink.Sink, refs []core.TableRef) (map[string]bool, error) {
	held := map[string]bool{}
	for _, ref := range refs {
		pos, err := snk.Position(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %w", ref.Target, err)
		}
		held[ref.Target] = pos != ""
		props, err := snk.Properties(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: snapshot state: %w", ref.Target, err)
		}
		if snapshot.Unfinished(props) {
			continue
		}
		mark := map[string]string{snapshot.PropSnapshotState: string(snapshot.StateNotStarted)}
		if err := snk.SetProperties(ctx, ref, mark); err != nil {
			return nil, fmt.Errorf("runner: %s: mark snapshot pending: %w", ref.Target, err)
		}
	}
	return held, nil
}

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
