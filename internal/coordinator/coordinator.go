// Package coordinator drives the source side of the split pipeline: it owns
// the replication reader and the DBLog snapshot, serves the control plane
// (Session/Assignment) and the Arrow Flight data plane, and streams change
// batches to the connected workers. Tables map to worker groups
// (spec.Tables[].Worker; a table without a group owns its own worker), each
// group gets its own Flight queue, and one batch always routes to exactly
// one worker.
package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
	"google.golang.org/grpc/keepalive"
)

// Config tunes the coordinator for one pipeline.
type Config struct {
	Spec       *spec.Spec
	ListenAddr string // gRPC + Flight listen address ("host:port" or ":0")

	ChunkSize     int
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
	ServerID      uint32
	Heartbeat     time.Duration
	// MaxParallelChunks caps concurrent chunk SELECTs during snapshot.
	// Must not exceed the ceiling the source driver declares.
	MaxParallelChunks int

	// FlowTotalBytes is the process-wide ceiling on serialized batch bytes
	// in flight (queued or sent, unacked). FlowPerWorkerMin is the floor
	// that keeps a slow worker from starving. Defaults 512Mi / 16Mi.
	FlowTotalBytes   int64
	FlowPerWorkerMin int64

	// Eventlog is optional: when set, the coordinator writes its per-run
	// audit trail (job_started, snapshots, commits, terminal) to S3.
	Eventlog *eventlog.Config

	// Checkpoint is optional: async position manifests to S3 (design §6).
	// Convenience only — the Iceberg table property is the source of truth.
	Checkpoint *CheckpointConfig

	// WaitWorker bounds how long the coordinator waits for every expected
	// worker session before failing the boot.
	WaitWorker time.Duration

	// Supervision: a worker that stops acking past AckTimeout is reset
	// (epoch++ and session cancel). Resets within ResetWindow beyond
	// MaxResets terminate the job. Defaults 30s / 5 / 15m.
	AckTimeout  time.Duration
	MaxResets   int
	ResetWindow time.Duration

	// MetricsAddr serves /metrics (Prometheus) and /statusz (live state).
	// Empty disables the endpoint.
	MetricsAddr string

	Logger *slog.Logger
}

// workerQueueCap bounds one worker's in-flight batches — the structural
// backpressure hop of design §1.1 (workerCh cap 64).
const workerQueueCap = 64

// queuedBatch is one serialized batch waiting for the Flight stream.
type queuedBatch struct {
	body []byte // complete Arrow IPC stream
	meta []byte // BatchMeta proto
}

// Coordinator runs the source pipeline and serves workers.
type Coordinator struct {
	cfg Config
	log *slog.Logger

	src       source.Source
	qsrc      source.QuerySource
	refs      []source.TableRef
	snk       sink.Sink
	canonical map[string]core.Schema // per-source canonical schema for typed wire format

	// Worker registry: groups resolved at boot from the spec, one queue and
	// one ticket each; route maps every target table to its owning worker.
	route    map[string]*workerState
	workers  map[string]*workerState
	byTicket map[string]*workerState
	budget   *flowBudget
	index    map[string]*positionIndex

	// runCtx outlives the helper goroutines that need cancellation (the
	// wireRelay) but are called outside run's select.
	runCtx context.Context

	runID string // run-id of this boot (assignment + eventlog, §5.6.1)

	ev *eventlog.Run

	ready       chan struct{} // one send per attached session
	sessionErrs chan error    // first exit wins
	batchSeq    atomic.Uint64 // monotonic BatchMeta.batch_id
	mu          sync.Mutex    // guards session attach/detach

	// DBLog window gate (design §3.1): while a chunk's SELECT is in flight
	// on the worker, live events of that table are held here instead of
	// being shipped — a live event racing ahead of the chunk's rows would
	// miss the window delete and duplicate the row. On ChunkReady the held
	// events are released InWindow-tagged, then the Closes marker.
	gateMu  sync.Mutex
	gateOn  bool
	gateTgt string
	gateBuf []change.Change

	// chunkReady routes worker ChunkReady replies to the snapshot loop.
	chunkReady chan *pb.ChunkReady

	// confirmed tracks the latest position each target table durably
	// committed (from worker Acks). The minimum across tables is reported
	// to the source so its retention never advances past uncommitted data.
	confirmedMu sync.Mutex
	confirmed   map[string]position.Position

	cp         *checkpoint
	supervisor *supervisor
	terminate  chan error
	metrics    *observability.Metrics
}

// workerState is one worker group's slice of the pipeline: its tables, its
// Flight queue, and — once it connects — its control surface.
type workerState struct {
	name   string
	refs   []source.TableRef
	queue  chan queuedBatch
	ticket []byte

	out      chan *pb.CoordinatorMessage // attached by Session
	control  pb.UrutauControl_ControlServer
	attached bool
	epoch    uint64 // last accepted epoch (guards stale Hellos)
	cancel   context.CancelFunc

	// committed: target table → position the worker reported after its last
	// commit. Refreshed on every ready Hello (design §5.6.1).
	committed map[string]string
}

// workerName resolves the worker group of one spec table: the explicit
// worker= grouping, or the table's own pod (1:1 default).
func workerName(t spec.Table) string {
	if t.Worker != "" {
		return t.Worker
	}
	return t.Target
}

// Run boots the pipeline and blocks until ctx is cancelled or a terminal
// error occurs.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Coordinator{
		cfg:         cfg,
		log:         cfg.Logger,
		route:       map[string]*workerState{},
		workers:     map[string]*workerState{},
		byTicket:    map[string]*workerState{},
		index:       map[string]*positionIndex{},
		ready:       make(chan struct{}, 1024),
		sessionErrs: make(chan error, 1024),
		chunkReady:  make(chan *pb.ChunkReady, 1024),
		confirmed:   make(map[string]position.Position),
	}
	if cfg.FlowTotalBytes <= 0 {
		cfg.FlowTotalBytes = 512 << 20
	}
	if cfg.FlowPerWorkerMin <= 0 {
		cfg.FlowPerWorkerMin = 16 << 20
	}
	c.budget = newFlowBudget(cfg.FlowTotalBytes, cfg.FlowPerWorkerMin)
	c.runID = time.Now().UTC().Format("2006-01-02T15:04:05Z") + "-" + randSuffix(6)
	c.supervisor = newSupervisor(c)
	c.terminate = make(chan error, 1)
	if cfg.MetricsAddr != "" {
		c.metrics = observability.New()
		go func() { _ = c.metrics.Serve(cfg.MetricsAddr, c.statusz) }()
	}
	return c.run(ctx)
}

func (c *Coordinator) run(ctx context.Context) error {
	c.runCtx = ctx

	if cfg := c.cfg.Eventlog; cfg != nil {
		ev, err := eventlog.New(ctx, *cfg)
		if err != nil {
			return fmt.Errorf("coordinator: eventlog: %w", err)
		}
		c.ev = ev
		defer ev.Close()
		if err := c.emit(eventlog.KindJobStarted, map[string]any{
			"pipeline": c.cfg.Spec.Pipeline,
			"source":   c.cfg.Spec.Source.Kind,
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}

	// Source adapter, query connection, introspection — identical to the
	// collapsed runner; only the worker side differs.
	src, err := driver.OpenSource(c.cfg.Spec, source.Runtime{
		ServerID:  c.cfg.ServerID,
		Heartbeat: c.cfg.Heartbeat,
		Logger:    c.log,
	})
	if err != nil {
		return err
	}
	c.src = src
	qsrc, ok := src.(source.QuerySource)
	if !ok {
		return fmt.Errorf("coordinator: source %q has no SQL query surface", c.cfg.Spec.Source.Kind)
	}
	c.qsrc = qsrc
	defer func() { _ = qsrc.CloseQuery() }()
	// The parallel-chunk setting may not exceed the ceiling the source
	// driver declares — fail fast at boot, not mid-snapshot.
	if err := driver.ValidateParallelism(c.cfg.Spec.Source.Kind, c.cfg.MaxParallelChunks); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}

	refs := make([]source.TableRef, 0, len(c.cfg.Spec.Tables))
	canonical := make(map[string]core.Schema, len(c.cfg.Spec.Tables))
	for _, t := range c.cfg.Spec.Tables {
		ref, cs, _, err := src.Introspect(ctx, t)
		if err != nil {
			return err
		}
		refs = append(refs, ref)
		canonical[t.Source] = cs
	}
	c.refs = refs
	c.canonical = canonical

	// Resolve worker groups: explicit worker= or the table's own pod.
	for i, t := range c.cfg.Spec.Tables {
		name := workerName(t)
		w, ok := c.workers[name]
		if !ok {
			w = &workerState{
				name:   name,
				queue:  make(chan queuedBatch, workerQueueCap),
				ticket: []byte("urutau/" + name),
			}
			c.workers[name] = w
			c.byTicket[string(w.ticket)] = w
			c.index[name] = newPositionIndex(c.runID)
		}
		w.refs = append(w.refs, refs[i])
		c.route[t.Target] = w
	}
	for _, w := range c.workers {
		c.log.Info("coordinator worker group", "worker", w.name, "tables", len(w.refs))
		if err := c.emit(eventlog.KindWorkerCreated, map[string]any{
			"worker": w.name,
			"tables": tableNames(w.refs),
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}
	if cfg := c.cfg.Checkpoint; cfg != nil {
		cp, err := newCheckpoint(ctx, *cfg)
		if err != nil {
			return fmt.Errorf("coordinator: checkpoint: %w", err)
		}
		c.cp = cp
		go cp.run(ctx, c.runID, c.index, c.log)
		c.log.Info("coordinator checkpoint", "uri", cfg.URI, "interval", cp.interval)
	}

	// Sink + tables: the coordinator owns DDL.
	snk, err := driver.OpenSink(ctx, c.cfg.Spec)
	if err != nil {
		return fmt.Errorf("coordinator: catalog: %w", err)
	}
	c.snk = snk
	for _, ref := range refs {
		if err := snk.EnsureTable(ctx, ref, canonical[ref.Source], nil, core.CastPolicy{}); err != nil {
			return fmt.Errorf("coordinator: ensure %s: %w", ref.Target, err)
		}
	}

	resume, needsSnapshot, err := c.resumeFrom(ctx, refs)
	if err != nil {
		return err
	}
	c.log.Info("coordinator resume", "from", resumeOrNone(resume), "snapshot_tables", len(needsSnapshot))

	// Serve gRPC (control) + Flight (data) on one listener.
	lis, err := net.Listen("tcp", c.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("coordinator: listen: %w", err)
	}
	defer func() { _ = lis.Close() }()
	c.log.Info("coordinator listening", "addr", lis.Addr().String())

	grpcServer := grpc.NewServer(
		// Keepalive agreement with the worker: MinTime ≤ client Time, else
		// the server GOAWAYs a healthy worker for pinging too much.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 5 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: false,
		}),
		// Flight batches can be a full snapshot chunk; 128Mi covers the
		// default batching ceiling.
		grpc.MaxRecvMsgSize(128<<20),
		grpc.MaxSendMsgSize(128<<20),
	)
	pb.RegisterUrutauControlServer(grpcServer, &controlServer{c: c})
	flight.RegisterFlightServiceServer(grpcServer, &flightServer{c: c})
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	// Wait for every expected worker session.
	wait := c.cfg.WaitWorker
	if wait <= 0 {
		wait = 2 * time.Minute
	}
	if err := c.waitWorkers(ctx, wait); err != nil {
		return err
	}

	// Reader + stream, then snapshot — the DBLog loop the collapsed runner
	// runs, routed over the wire instead of an in-process channel.
	rdr, err := c.src.Open(ctx, refs)
	if err != nil {
		return err
	}
	defer rdr.Close()
	// The slot's confirmed point tracks the minimum position workers have
	// durably committed (their Acks), never the decode position — otherwise
	// a crash between decode and commit would lose the in-flight window.
	rdr.SetConfirmed(c.confirmedPosition)

	start := resume
	if start == nil {
		m, err := c.src.InitialPosition(ctx)
		if err != nil {
			return fmt.Errorf("coordinator: initial position: %w", err)
		}
		start = m
	}
	out, streamErr := rdr.Stream(ctx, start)

	// Pump: every decoded change becomes one Flight batch. The FIFO queue
	// preserves the wire ordering the window protocol needs, so the
	// in-process flushReq drain disappears.
	go c.pump(ctx, out)

	snapCfg := snapshot.SnapshotConfig{
		WindowTimeout: c.cfg.WindowTimeout,
		CaughtUpPoll:  c.cfg.CaughtUpPoll,
	}
	for _, ref := range needsSnapshot {
		c.log.Info("coordinator snapshot", "table", ref.Source)
		if err := c.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
		chunker, err := c.qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
		if err != nil {
			return err
		}
		if err := c.snapshotTable(ctx, rdr, chunker, ref, snapCfg); err != nil {
			return fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
		}
		c.log.Info("coordinator snapshot done", "table", ref.Source)
		if err := c.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}

	// Supervision after the snapshot phase: acks only flow once the stream
	// is live, so a long snapshot must not look like a stale worker.
	go c.supervisor.run(ctx, supervisionConfig(c.cfg), c.terminate)

	// Block until the world ends.
	select {
	case <-ctx.Done():
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "shutdown"})
		return ctx.Err()
	case err := <-streamErr:
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "stream"})
		return fmt.Errorf("coordinator: stream: %w", err)
	case err := <-c.sessionErrs:
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "session"})
		return fmt.Errorf("coordinator: worker session: %w", err)
	case err := <-c.terminate:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobTerminated, map[string]any{"reason": "crashloop"})
		return err
	}
}

// supervisionConfig maps the Config knobs to the supervisor defaults.
func supervisionConfig(cfg Config) SupervisorConfig {
	return SupervisorConfig{
		AckTimeout:  cfg.AckTimeout,
		MaxResets:   cfg.MaxResets,
		ResetWindow: cfg.ResetWindow,
	}
}

// emitLog writes an event and logs any failure (best-effort trail).
func (c *Coordinator) emitLog(kind string, fields map[string]any) {
	if err := c.emit(kind, fields); err != nil {
		c.log.Warn("coordinator: eventlog emit", "kind", kind, "err", err)
	}
}

// statusz renders the live coordinator state for /statusz (design §13.4).
func (c *Coordinator) statusz(w http.ResponseWriter, r *http.Request) {
	type workerStatus struct {
		Phase     string            `json:"phase"`
		Epoch     uint64            `json:"epoch"`
		Attached  bool              `json:"attached"`
		Inflight  int64             `json:"inflight_bytes"`
		Committed map[string]string `json:"committed,omitempty"`
	}
	st := map[string]any{
		"run_id": c.runID,
	}
	ws := map[string]*workerStatus{}
	for name, w := range c.workers {
		c.mu.Lock()
		ws[name] = &workerStatus{
			Phase:     "attached",
			Epoch:     w.epoch,
			Attached:  w.attached,
			Inflight:  c.budget.inFlight(name),
			Committed: w.committed,
		}
		c.mu.Unlock()
	}
	st["workers"] = ws
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(st); err != nil {
		c.log.Warn("statusz encode", "err", err)
	}
}

// emit writes one event to the audit trail when configured; best-effort by
// contract (a lost trail must never fail the pipeline).
func (c *Coordinator) emit(kind string, fields map[string]any) error {
	if c.ev == nil {
		return nil
	}
	if err := c.ev.Emit(context.Background(), kind, fields); err != nil {
		return err
	}
	return nil
}

// waitWorkers blocks until every expected group has a session attached.
func (c *Coordinator) waitWorkers(ctx context.Context, wait time.Duration) error {
	for range c.workers {
		select {
		case <-c.ready:
		case <-time.After(wait):
			return fmt.Errorf("coordinator: not all workers connected within %s", wait)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// pump encodes reader events into the data queue. While a DBLog window is
// open (gateOn), events of the gated table are buffered instead — released
// InWindow-tagged by flushWindow after the worker confirms ChunkReady. Other
// tables flow freely.
func (c *Coordinator) pump(ctx context.Context, out <-chan change.Change) {
	for {
		select {
		case ch, ok := <-out:
			if !ok {
				return
			}
			if c.metrics != nil {
				c.metrics.EventsDecoded.Inc()
			}
			if c.gateHold(ch) {
				continue
			}
			if err := c.enqueueBatch(ctx, []change.Change{ch}, batchMeta(ch)); err != nil {
				c.log.Warn("coordinator: enqueue failed", "err", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// gateHold buffers an event when a window is open for its table.
func (c *Coordinator) gateHold(ch change.Change) bool {
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	if !c.gateOn || ch.Table != c.gateTgt {
		return false
	}
	c.gateBuf = append(c.gateBuf, ch)
	return true
}

// openWindow pauses the pump for one table, tagging the current chunk. The
// gate stays open for the WHOLE snapshot of the table (design §3.1: the
// coordinator pauses relaying while it works the table); flushWindow drains
// per chunk without closing it, and closeWindow seals it at the end. A gate
// that opened and closed per chunk would let gap events (positioned AFTER
// the gate's backlog) flow straight through, then release older backlog
// after them — a reordering that resurrects old values.
func (c *Coordinator) openWindow(target string) {
	c.gateMu.Lock()
	c.gateOn, c.gateTgt = true, target
	c.gateMu.Unlock()
}

// flushWindow drains the gated events collected since the last drain,
// InWindow-tagged for the given chunk, then returns (gate stays open).
func (c *Coordinator) flushWindow(ctx context.Context, chunkID uint32) error {
	c.gateMu.Lock()
	buf, tgt := c.gateBuf, c.gateTgt
	c.gateBuf = nil
	c.gateMu.Unlock()

	if len(buf) == 0 {
		return nil
	}
	// One batch, InWindow-tagged: the worker deletes each key from the
	// chunk's window (the live version won) and applies the change.
	meta := &pb.BatchMeta{
		Table:  tgt,
		Window: &pb.WindowTag{InWindow: true, ChunkId: chunkID},
	}
	return c.enqueueBatch(ctx, buf, meta)
}

// closeWindow releases any remaining gated events (post-last-chunk) and
// closes the gate. The trailing events are ordinary live changes: no window
// tag.
func (c *Coordinator) closeWindow(ctx context.Context) error {
	c.gateMu.Lock()
	buf, tgt := c.gateBuf, c.gateTgt
	c.gateOn, c.gateBuf = false, nil
	c.gateMu.Unlock()

	if len(buf) == 0 {
		return nil
	}
	return c.enqueueBatch(ctx, buf, &pb.BatchMeta{Table: tgt})
}

// recordConfirmed stores a table's latest durably-committed position and
// recomputes the pipeline-wide minimum. The minimum uses the position's own
// ordering — LSNs and GTID sets are not lexicographically ordered, and a
// wrong minimum would advance the source slot past data still in flight.
func (c *Coordinator) recordConfirmed(table string, pos position.Position) {
	if pos == nil {
		return
	}
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	c.confirmed[table] = pos
}

// confirmedPosition returns the minimum committed position across all
// target tables; nil while nothing is durably committed.
func (c *Coordinator) confirmedPosition() position.Position {
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	if len(c.confirmed) == 0 {
		return nil
	}
	vals := make([]position.Position, 0, len(c.confirmed))
	for _, p := range c.confirmed {
		vals = append(vals, p)
	}
	return position.Min(vals)
}

// waitChunkReady blocks until the worker reports the chunk SELECT done.
func (c *Coordinator) waitChunkReady(ctx context.Context, table string, chunkID uint32) error {
	for {
		select {
		case cr := <-c.chunkReady:
			if cr.Table == table && cr.ChunkId == chunkID {
				return nil
			}
			c.log.Warn("coordinator: unexpected ChunkReady", "table", cr.Table, "chunk", cr.ChunkId)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// snapshotTable runs the DBLog snapshot for one table with the chunk SELECT
// executed by the worker (design §3.1): for each chunk the coordinator pauses
// the pump (openWindow), sends ChunkRequest bounds, waits ChunkReady, proves
// caught-up, then releases the gated live events InWindow-tagged and the
// Closes marker. The worker holds the chunk rows in its window; the window
// is what InWindow events drain and the Closes marker flushes.
func (c *Coordinator) snapshotTable(ctx context.Context, rdr source.SourceReader, chunker source.ChunkSource, ref source.TableRef, cfg snapshot.SnapshotConfig) error {
	bounds, err := chunker.Bounds(ctx)
	if err != nil {
		return err
	}
	chunks := snapshot.Chunks(bounds)
	w, ok := c.route[ref.Target]
	if !ok {
		return fmt.Errorf("coordinator: snapshot: no worker owns %s", ref.Target)
	}

	for i, ch := range chunks {
		chunkID := uint32(i)
		if err := ctx.Err(); err != nil {
			return err
		}
		if i == 0 {
			c.openWindow(ref.Target)
		}

		boundsB, err := transport.EncodeBounds(ch.Low, ch.High)
		if err != nil {
			return fmt.Errorf("coordinator: chunk %d bounds: %w", chunkID, err)
		}
		req := &pb.ChunkRequest{Table: ref.Source, ChunkId: chunkID, Bounds: boundsB}

		select {
		case w.out <- &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Chunk{Chunk: req}}:
		case <-ctx.Done():
			return ctx.Err()
		}

		if err := c.waitChunkReady(ctx, ref.Source, chunkID); err != nil {
			return err
		}
		c.log.Info("chunk ready", "table", ref.Source, "chunk", chunkID)

		// The worker has the chunk rows in its window; prove the reader is
		// caught up before releasing anything that touches this window. The
		// high watermark is the source's FIXED position after the SELECT —
		// never a live master — so a busy source cannot keep the window open
		// forever.
		high, err := rdr.Master(ctx)
		if err != nil {
			return fmt.Errorf("dblog: chunk %d: master: %w", chunkID, err)
		}
		if err := snapshot.WaitCaughtUp(ctx, rdr, high, cfg); err != nil {
			return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
		}
		at := rdr.Synced()

		// Release this chunk's gated live events (InWindow-tagged) ahead of
		// the Closes marker — FIFO keeps them before it. The gate stays
		// open: the next chunk's backlog must not race ahead of these.
		if err := c.flushWindow(ctx, chunkID); err != nil {
			return err
		}
		if err := c.enqueueBatch(ctx, nil, &pb.BatchMeta{
			Table:  ref.Target,
			LowPos: at.String(),
			Window: &pb.WindowTag{Closes: true, ChunkId: chunkID},
		}); err != nil {
			return err
		}
	}
	// Seal the gate and release anything collected after the last chunk.
	return c.closeWindow(ctx)
}

// batchMeta derives the wire window tag from one change.
func batchMeta(ch change.Change) *pb.BatchMeta {
	m := &pb.BatchMeta{Table: ch.Table, LowPos: ch.Position, HighPos: ch.Position}
	if ch.Window != nil {
		m.Window = &pb.WindowTag{
			InWindow: ch.Window.InWindow,
			Closes:   ch.Window.Closes,
			ChunkId:  ch.Window.ChunkID,
		}
	}
	return m
}

// enqueueBatch encodes rows, charges the worker's share of the global flow
// budget, and queues the serialized batch on its Flight stream. A full
// budget blocks here — the backpressure that stalls the pump and, through
// it, the reader. The charge is released when the worker's Ack covers the
// batch's position (onAck).
func (c *Coordinator) enqueueBatch(ctx context.Context, rows []change.Change, meta *pb.BatchMeta) error {
	w, ok := c.route[meta.Table]
	if !ok {
		return fmt.Errorf("coordinator: no worker owns table %s", meta.Table)
	}
	meta.BatchId = c.batchSeq.Add(1)
	// Resolve the canonical schema for typed wire encoding.
	var cs core.Schema
	for _, ref := range c.refs {
		if ref.Target == meta.Table {
			cs = c.canonical[ref.Source]
			break
		}
	}
	body, metaBytes, err := transport.EncodeBatch(rows, cs, meta)
	if err != nil {
		return err
	}
	n := int64(len(body) + len(metaBytes))
	if err := c.budget.acquire(ctx, w.name, n); err != nil {
		return err
	}
	// Marker batches (window closes) carry their position in LowPos.
	posStr := meta.HighPos
	if posStr == "" {
		posStr = meta.LowPos
	}
	var high position.Position
	if posStr != "" {
		high, err = c.src.ParsePosition(posStr)
		if err != nil {
			c.budget.release(w.name, n)
			return fmt.Errorf("coordinator: batch %s position %q: %w", meta.Table, posStr, err)
		}
	}
	select {
	case w.queue <- queuedBatch{body: body, meta: metaBytes}:
		c.index[w.name].add(inflightBatch{id: meta.BatchId, table: meta.Table, high: high, bytes: n})
		return nil
	case <-ctx.Done():
		c.budget.release(w.name, n)
		return ctx.Err()
	}
}

// onHello processes a worker's ready Hello: it carries the phase and the
// committed positions the worker read from Iceberg. A Hello with a stale
// epoch is a zombie from a superseded generation — reject it (design §5.5).
func (c *Coordinator) onHello(worker string, h *pb.Hello) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.workers[worker]
	if !ok {
		c.log.Warn("coordinator: Hello for unknown worker", "worker", worker)
		return
	}
	if h.Epoch != w.epoch {
		c.log.Warn("coordinator: stale Hello epoch", "worker", worker, "have", w.epoch, "got", h.Epoch)
		return
	}
	w.committed = h.Committed
	c.log.Info("worker hello", "worker", worker, "phase", h.Phase.String(),
		"committed", len(h.Committed))
}

// onAck advances the worker's position index: every head batch the commit
// covers leaves the flight window and returns its bytes to the budget.
func (c *Coordinator) onAck(worker string, ack *pb.Ack) {
	c.supervisor.noteAck(worker, time.Now())
	pos, err := c.src.ParsePosition(ack.Position)
	if err != nil {
		c.log.Warn("coordinator: ack position", "worker", worker, "err", err)
		return
	}
	freed := c.index[worker].truncate(ack.Table, pos)
	if freed > 0 {
		c.budget.release(worker, freed)
	}
	// The ack is evidence of a durable commit: record it and recompute the
	// pipeline-wide minimum the source's retention may advance to.
	c.recordConfirmed(ack.Table, pos)
	if c.metrics != nil {
		c.metrics.InflightBytes.WithLabelValues(worker).Set(float64(c.budget.inFlight(worker)))
		c.metrics.CommitsTotal.WithLabelValues(ack.Table).Inc()
	}
	c.log.Info("worker ack", "worker", worker, "table", ack.Table,
		"rows", ack.Rows, "position", ack.Position, "inflight", c.budget.inFlight(worker))
	if err := c.emit(eventlog.KindCommit, map[string]any{
		"worker":   worker,
		"table":    ack.Table,
		"rows":     ack.Rows,
		"deletes":  ack.Deletes,
		"position": ack.Position,
	}); err != nil {
		c.log.Warn("coordinator: eventlog emit", "err", err)
	}
}

// assignmentFor builds one worker's table assignment with its own ticket.
// The table schema travels as Arrow IPC derived from the canonical schema —
// the same typed discipline as the Flight data plane, no JSON on the wire.
func (c *Coordinator) assignmentFor(w *workerState) (*pb.CoordinatorMessage, error) {
	assign := &pb.Assignment{
		WorkerName: w.name,
		Epoch:      uint64(1),
		RunId:      c.runID,
		Ticket:     w.ticket,
		SourceKind: c.cfg.Spec.Source.Kind,
		SourceDsn:  c.cfg.Spec.Source.URI,
		ChunkSize:  uint32(c.cfg.ChunkSize),
		Batching: &pb.BatchConfig{
			MaxInterval: durationpb.New(2 * time.Second),
		},
	}
	for _, ref := range w.refs {
		schemaB, err := transport.EncodeTableSchema(c.canonical[ref.Source])
		if err != nil {
			return nil, fmt.Errorf("coordinator: schema %s: %w", ref.Source, err)
		}
		assign.Tables = append(assign.Tables, &pb.TableAssignment{
			SourceTable:       ref.Source,
			TargetTable:       ref.Target,
			WriteMode:         pb.WriteMode_WRITE_MODE_UPSERT,
			PrimaryKey:        ref.PrimaryKey,
			CreateIfNotExists: true,
			SchemaArrow:       schemaB,
		})
	}
	return &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: assign}}, nil
}

// resumeFrom reads cdc.position per target table; the minimum across tables
// is the resume point, tables without one need the snapshot.
func (c *Coordinator) resumeFrom(ctx context.Context, refs []source.TableRef) (position.Position, []source.TableRef, error) {
	var positions []position.Position
	var needsSnapshot []source.TableRef
	for _, ref := range refs {
		pos, err := c.snk.Position(ctx, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("coordinator: %s: %w", ref.Target, err)
		}
		if pos != "" {
			p, err := c.src.ParsePosition(pos)
			if err != nil {
				return nil, nil, fmt.Errorf("coordinator: %s cdc.position %q: %w", ref.Target, pos, err)
			}
			positions = append(positions, p)
		} else {
			needsSnapshot = append(needsSnapshot, ref)
		}
	}
	if len(positions) == 0 {
		return nil, needsSnapshot, nil
	}
	best := positions[0]
	for _, p := range positions[1:] {
		if p.Compare(best) < 0 {
			best = p
		}
	}
	return best, needsSnapshot, nil
}

// ── gRPC control plane ───────────────────────────────────────────────

type controlServer struct {
	pb.UnimplementedUrutauControlServer
	c *Coordinator
}

// Session accepts one worker: the Hello names the group it claims (unknown
// names and second connects are rejected; epoch validation arrives with
// supervision). The server then streams assignments and collects acks until
// the worker goes.
func (s *controlServer) Session(stream pb.UrutauControl_SessionServer) (retErr error) {
	c := s.c
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := msg.GetHello()
	if hello == nil {
		return errors.New("coordinator: first worker message must be Hello")
	}

	c.mu.Lock()
	w, known := c.workers[hello.WorkerName]
	if known && w.attached {
		c.mu.Unlock()
		return fmt.Errorf("coordinator: worker %q already connected", hello.WorkerName)
	}
	// The supervisor cancels this ctx to force a reset; the worker sees the
	// stream die and suicides.
	sessCtx, sessCancel := context.WithCancel(stream.Context())
	sess := &workerSession{
		out:  make(chan *pb.CoordinatorMessage, 16),
		done: make(chan error, 1),
	}
	// Publish the session BEFORE signaling ready: the ready send
	// happens-before run's receive, so run may use w.out the moment it
	// wakes — attaching after the signal is a data race.
	if known {
		w.out, w.attached = sess.out, true
		w.cancel = sessCancel
		c.supervisor.noteAttach(hello.WorkerName)
	}
	c.mu.Unlock()
	if !known {
		sessCancel()
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}

	defer func() {
		c.mu.Lock()
		w.attached, w.out, w.cancel = false, nil, nil
		c.mu.Unlock()
		sessCancel()
		// A supervisor reset is not a worker failure, and neither is the
		// death of a worker mid-reset (it suicides on channel loss); the
		// supervisor owns the outcome (crashloop or recovery).
		if !errors.Is(retErr, errSessionReset) && !c.supervisor.isPending(hello.WorkerName) {
			c.sessionErrs <- retErr
		}
	}()

	select {
	case c.ready <- struct{}{}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	c.log.Info("worker session", "worker", hello.WorkerName)

	// Assignment on every attach (including after a reset): the worker
	// waits for it before opening Flight, and a resurrected worker needs a
	// fresh one.
	if msg, err := c.assignmentFor(w); err != nil {
		return err
	} else {
		select {
		case sess.out <- msg:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	// Recv loop: acks and worker errors.
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				sess.done <- err
				return
			}
			switch m := msg.Msg.(type) {
			case *pb.WorkerMessage_Ack:
				c.onAck(hello.WorkerName, m.Ack)
			case *pb.WorkerMessage_Hello:
				c.onHello(hello.WorkerName, m.Hello)
			case *pb.WorkerMessage_ChunkReady:
				// A full chunkReady buffer with no draining snapshot loop
				// (stale replies after a reset) must not wedge this recv
				// goroutine; it aborts on session cancellation instead.
				select {
				case c.chunkReady <- m.ChunkReady:
				case <-sessCtx.Done():
					sess.done <- context.Canceled
					return
				}
			case *pb.WorkerMessage_Error:
				sess.done <- errors.New("coordinator: worker error: " + m.Error.Detail)
				return
			}
		}
	}()

	// Send loop: assignments and future control messages.
	for {
		select {
		case m := <-sess.out:
			if err := stream.Send(m); err != nil {
				return err
			}
		case err := <-sess.done:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-sessCtx.Done():
			return errSessionReset
		}
	}
}

// errSessionReset marks a session cancelled by the supervisor; not a worker
// failure, so it must not kill the run via sessionErrs.
var errSessionReset = errors.New("session reset")

// workerSession is one connected worker's session-local surface; the group's
// durable state lives in workerState.
type workerSession struct {
	out  chan *pb.CoordinatorMessage
	done chan error
}

// Control is the urgent-signal plane on the same ClientConn as Session and
// DoGet. The first frame must be a Hello naming the worker so urgent
// signals route to the right stream. No data rides here.
func (s *controlServer) Control(stream pb.UrutauControl_ControlServer) (retErr error) {
	c := s.c
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := msg.GetHello()
	if hello == nil {
		return errors.New("coordinator: Control first message must be Hello")
	}
	c.mu.Lock()
	w, known := c.workers[hello.WorkerName]
	if known {
		w.control = stream
	}
	c.mu.Unlock()
	if !known {
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}
	defer func() {
		c.mu.Lock()
		w.control = nil
		c.mu.Unlock()
		// A worker mid-reset suicides and closes this stream too; only a
		// non-reset death is a real session failure.
		if !c.supervisor.isPending(hello.WorkerName) {
			c.sessionErrs <- retErr
		}
	}()
	// The worker never writes again; its death is the stream ending.
	<-stream.Context().Done()
	return stream.Context().Err()
}

// gracefulShutdown tells every connected worker to drain: flush + commit +
// ack what is in flight, then exit 0 (design §5.3.2). Called on shutdown
// and before terminal exits.
func (c *Coordinator) gracefulShutdown() {
	for _, w := range c.workers {
		c.mu.Lock()
		ctrl := w.control
		c.mu.Unlock()
		if ctrl == nil {
			continue
		}
		msg := &pb.ControlMessage{Msg: &pb.ControlMessage_Shutdown{
			Shutdown: &pb.Shutdown{
				Grace: durationpb.New(30 * time.Second),
				Drain: true,
			},
		}}
		if err := ctrl.Send(msg); err != nil {
			c.log.Warn("coordinator: shutdown send", "worker", w.name, "err", err)
		}
	}
}

// ── Flight data plane ────────────────────────────────────────────────

type flightServer struct {
	flight.BaseFlightServer
	c *Coordinator
}

// DoGet streams the worker's queued batches; the ticket (from its
// Assignment) selects which queue. Each FlightData carries one complete IPC
// stream in DataBody and a BatchMeta proto in AppMetadata — both produced
// at enqueue time, so the server only moves bytes.
func (s *flightServer) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	w, ok := s.c.byTicket[string(req.Ticket)]
	if !ok {
		return fmt.Errorf("coordinator: unknown flight ticket %q", string(req.Ticket))
	}
	for {
		select {
		case qb := <-w.queue:
			if err := stream.Send(&flight.FlightData{
				DataHeader:  []byte("urutau-batch"),
				DataBody:    qb.body,
				AppMetadata: qb.meta,
			}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// ── Positions ─────────────────────────────────────────────────────────

func resumeOrNone(p position.Position) string {
	if p == nil {
		return "none"
	}
	return p.String()
}

// randSuffix returns n hex chars of crypto randomness (run-id suffix).
func randSuffix(n int) string {
	b := make([]byte, n/2+1)
	if _, err := rand.Read(b); err != nil {
		return "000000"
	}
	return hex.EncodeToString(b)[:n]
}

// tableNames renders a group's source tables for the audit trail.
func tableNames(refs []source.TableRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Source
	}
	return out
}
