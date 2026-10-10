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
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/gate"
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

// relayGateMaxEvents and relayGateMaxBytes bound the relay's DBLog window,
// the same structural backpressure the coordinator's gate has (#438, audit
// #5): beyond it the relay blocks until the window's chunk is ready and
// drains. The runner's gate is a single window, keyed by (target, 0).
const (
	relayGateMaxEvents = 1024
	relayGateMaxBytes  = 64 << 20
)

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

	// gate is the shared DBLog window gate (design §3.1): while a chunk
	// SELECT is in flight the table's live events are buffered and released
	// InWindow-tagged only after AddWindowRows populates the worker window.
	gate *gate.Gate[*dataplane.Batch]

	gateMu       sync.Mutex
	gateTgt      string
	gateChk      uint32
	flushGate    bool
	gateFlushReq chan chan struct{}
}

// relaySink delivers the relay's gate drains to the worker's ingest channel.
type relaySink struct{ r *relay }

func (s relaySink) Flush(ctx context.Context, _ gate.Window, windowID uint64, items []*dataplane.Batch) error {
	return s.r.sendWindowed(ctx, uint32(windowID), items)
}

// Close releases the relay's window at seal. drainGate Flushes before it, so
// the buffer is empty here; anything left is released defensively.
func (s relaySink) Close(_ context.Context, _ gate.Window, items []*dataplane.Batch) error {
	for _, b := range items {
		b.Release()
	}
	return nil
}

func (s relaySink) Release(items []*dataplane.Batch) {
	for _, b := range items {
		b.Release()
	}
}

// sendWindowed enqueues held batches InWindow-tagged for chunkID, in order.
func (r *relay) sendWindowed(ctx context.Context, chunkID uint32, items []*dataplane.Batch) error {
	for _, b := range items {
		select {
		case r.ingest <- worker.Ingest{Table: b.Table, Batch: b, Win: &rowchange.Window{WindowID: uint64(chunkID), InWindow: true}}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func newRelay(ingest chan<- worker.Ingest, window *worker.Worker, onDeliver func(table, pos string)) *relay {
	r := &relay{
		ingest:       ingest,
		window:       window,
		onDeliver:    onDeliver,
		flushReq:     make(chan chan struct{}, 1),
		gateFlushReq: make(chan chan struct{}, 1),
	}
	r.gate = gate.New[*dataplane.Batch](relayGateMaxEvents, relayGateMaxBytes, dpint.BatchBytes, relaySink{r})
	return r
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
	if err := r.window.AddWindowRows(target, uint64(chunkID), batch); err != nil {
		return err
	}
	// The chunk's rows are in the window: a full gate may now drain early
	// rather than block the relay, the coordinator's ChunkReady early drain.
	r.gate.MarkReady(target, 0, uint64(chunkID))
	return nil
}

// GateOn starts buffering the table's live events for a chunk SELECT in
// flight. Called by the orchestrator before the SELECT.
func (r *relay) GateOn(table string, chunkID uint32) {
	// Open the window before publishing the chunk id: a batch the relay
	// pulls concurrently is then held rather than routed untagged.
	r.gate.Open(table, 0)
	r.gateMu.Lock()
	r.gateTgt = table
	r.gateChk = chunkID
	r.gateMu.Unlock()
}

// holdGate buffers a live batch in the relay's open window, if any, blocking
// while that bounded window is full. A drain failure surfaces to the relay.
func (r *relay) holdGate(ctx context.Context, b *dataplane.Batch) (bool, error) {
	return r.gate.Hold(ctx, b.Table, b)
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
	table := r.gateTgt
	chunkID := r.gateChk
	r.flushGate = false
	r.gateMu.Unlock()

	// Flush tags the held events for the chunk; Close then seals the window
	// (its buffer is empty here — the relay goroutine is the only producer).
	if err := r.gate.Flush(ctx, table, 0, uint64(chunkID)); err != nil {
		return true, err
	}
	if err := r.gate.Close(ctx, table, 0); err != nil {
		return true, err
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
			held, err := r.holdGate(ctx, b)
			if err != nil {
				return err
			}
			if held {
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

// runIncremental drains each incremental table in bounded pages (#572),
// pushing the rows through the worker's ingest path — the same path a snapshot
// uses, minus the window. The cursor is read from cdc.cursor and persisted
// post-commit by the OnCommit callback, so the data and the cursor advance
// together.
//
// A source that implements source.BatchIncrementalSource (#733) encodes the
// page straight into Arrow here; the runner passes it the same canonical wire
// schema it installs on the worker (KnownSchema), so the direct path is
// byte-for-byte the map path's RecordFromChanges. A source that does not, or a
// table whose schema is not yet known, falls back to source.IncrementalSource.
func (r *Runner) runIncremental(ctx context.Context, src source.Source, snk sink.Sink, refs []core.TableRef, specBySource map[string]spec.Table, w *worker.Worker, ingest chan worker.Ingest) error {
	inc, incOK := src.(source.IncrementalSource)
	batchInc, batchOK := src.(source.BatchIncrementalSource)
	if !incOK && !batchOK {
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
		// The batch path needs the canonical schema to build its encoder; with
		// none (an unregistered table) it cannot match the map path, so it
		// falls back rather than infer a drifting shape.
		known := w.KnownSchema(ref.Target)
		direct := batchOK && len(known.Columns) > 0
		if !direct && !incOK {
			return fmt.Errorf("runner: incremental %s: source implements only the columnar path and the target schema is unknown", ref.Target)
		}
		// Drain the table a page at a time: the source returns one bounded page
		// and says whether more is already available, so a large table never
		// loads entirely into memory (issue #572).
		for {
			next, dpb, more, err := incrementalPage(ctx, inc, batchInc, ref, t, after, known, direct)
			if err != nil {
				return fmt.Errorf("runner: incremental %s: %w", ref.Target, err)
			}
			if dpb == nil {
				break
			}
			select {
			case ingest <- worker.Ingest{Table: ref.Target, Batch: dpb}:
			case <-ctx.Done():
				return ctx.Err()
			}
			r.log.Info("incremental read", "table", ref.Source, "rows", int(dpb.Record.NumRows()), "cursor", next)
			after = next
			if !more {
				break
			}
		}
	}
	return nil
}

// incrementalPage reads one incremental page and returns it as a ready wire
// batch, or (_, nil, false, nil) at the end of the drain. direct selects the
// columnar BatchIncrementalSource path (encoding straight into Arrow); the
// fallback encodes source.IncrementalSource's row maps with RecordFromChanges,
// exactly as before. Both paths share the cursor/tie-break semantics — the
// source computes next the same way in either case.
func incrementalPage(ctx context.Context, inc source.IncrementalSource, batchInc source.BatchIncrementalSource, ref core.TableRef, t spec.Table, after string, known core.Schema, direct bool) (string, *dataplane.Batch, bool, error) {
	if direct {
		return batchInc.IncrementalBatch(ctx, ref, t.Cursor, after, known)
	}
	next, rows, more, err := inc.Incremental(ctx, ref, t.Cursor, after)
	if err != nil {
		return "", nil, false, err
	}
	if len(rows) == 0 {
		return "", nil, false, nil
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
	rec, err := transport.RecordFromChanges(changes, transport.MergeSchema(changes, known), nil)
	if err != nil {
		return "", nil, false, err
	}
	dpb := &dataplane.Batch{Table: ref.Target, Record: rec, Mode: dataplane.UpsertMode, Watermark: []byte(next)}
	return next, dpb, more, nil
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
