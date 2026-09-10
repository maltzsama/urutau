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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/grpctls"
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

	// TLS is the mutual-TLS material for the control plane. Empty means
	// plaintext — a loud warning is logged, because the Assignment carries
	// the source DSN (CD-1b).
	TLS grpctls.Config

	ChunkSize     int
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
	ServerID      uint32
	Heartbeat     time.Duration
	// MaxParallelChunks would cap concurrent chunk SELECTs during snapshot.
	// The snapshot is currently strictly sequential (one chunk in flight,
	// waitChunkReady blocks), so the knob is validated but reserved — it is
	// not yet wired to concurrency.
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
	// snapshotActive is true while the snapshot phase runs. A worker
	// session lost during it (reset OR death) must fail the run fast: the
	// worker's in-memory window died with it, so the protocol would either
	// wait out AckTimeout/MaxResets (15min) or let a stale ChunkReady from
	// the old generation satisfy the wait against an empty window — a
	// silently incomplete snapshot.
	snapshotActive atomic.Bool
	batchSeq       atomic.Uint64 // monotonic BatchMeta.batch_id
	mu             sync.Mutex    // guards session attach/detach

	// DBLog window gate (design §3.1): while a chunk's SELECT is in flight
	// on the worker, live events of that table are held here instead of
	// being shipped — a live event racing ahead of the chunk's rows would
	// miss the window delete and duplicate the row. On ChunkReady the held
	// events are released InWindow-tagged, then the Closes marker.
	gateMu  sync.Mutex
	gateOn  bool
	gateTgt string
	// gateBuf holds source batches (live changes) while their table's
	// snapshot window is open. Raw pre-encode batches: released by
	// flushWindow/closeWindow after they are queued.
	gateBuf []*dataplane.Batch
	// gateDrain wakes a pump blocked on a full gate when flushWindow/
	// closeWindow drains it (audit #5: the gate was the only buffer without
	// a structural bound).
	gateDrain chan struct{}

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

	// resend holds a batch popped from the queue whose Flight Send failed:
	// the next DoGet delivers it before draining the queue. A pop-then-send
	// that dropped the batch on stream death silently lost data (audit #2).
	resendMu sync.Mutex
	resend   *queuedBatch

	// committed: target table → position the worker reported after its last
	// commit. Refreshed on every ready Hello (design §5.6.1).
	committed map[string]string

	// activeGet guards one DoGet stream per worker. Two concurrent streams
	// on the same ticket would each pop the queue, splitting batches across
	// readers — a batch sent to a dying stream is lost (the one-slot resend
	// cannot cover two readers).
	activeGet atomic.Bool
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
	// Defaults land before the struct captures cfg (audit #16): a later read
	// of c.cfg must never see the zero flow knobs.
	if cfg.FlowTotalBytes <= 0 {
		cfg.FlowTotalBytes = 512 << 20
	}
	if cfg.FlowPerWorkerMin <= 0 {
		cfg.FlowPerWorkerMin = 16 << 20
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
		gateDrain:   make(chan struct{}),
		confirmed:   make(map[string]position.Position),
	}
	c.budget = newFlowBudget(cfg.FlowTotalBytes, cfg.FlowPerWorkerMin)
	c.runID = time.Now().UTC().Format("2006-01-02T15:04:05Z") + "-" + randSuffix(6)
	c.supervisor = newSupervisor(c)
	c.terminate = make(chan error, 1)
	if cfg.MetricsAddr != "" {
		c.metrics = observability.New()
		go func() {
			// A busy port silently disables observability otherwise — say so.
			if err := c.metrics.Serve(cfg.MetricsAddr, c.statusz); err != nil {
				c.log.Warn("coordinator: metrics server stopped", "addr", cfg.MetricsAddr, "err", err)
			}
		}()
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
	tableBySource := make(map[string]spec.Table, len(c.cfg.Spec.Tables))
	for _, t := range c.cfg.Spec.Tables {
		ref, cs, _, err := src.Introspect(ctx, t)
		if err != nil {
			return err
		}
		refs = append(refs, ref)
		canonical[t.Source] = cs
		tableBySource[t.Source] = t
	}
	c.refs = refs
	c.canonical = canonical

	// Resolve worker groups: explicit worker= or the table's own pod.
	for i, t := range c.cfg.Spec.Tables {
		name := workerName(t)
		w, ok := c.workers[name]
		if !ok {
			// A 128-bit random ticket colliding is ~0, but the queue-lookup
			// map is keyed by it — a collision would silently orphan a
			// worker's stream, so regenerate rather than assume.
			for {
				ticket := randTicket()
				if _, taken := c.byTicket[string(ticket)]; taken {
					continue
				}
				w = &workerState{
					name:   name,
					queue:  make(chan queuedBatch, workerQueueCap),
					ticket: ticket,
				}
				c.byTicket[string(ticket)] = w
				break
			}
			c.workers[name] = w
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
	// The sink is opened on every error path between here and the defers;
	// Close on every exit, not just the happy one (audit #14).
	defer func() { _ = c.snk.Close() }()
	for _, ref := range refs {
		tbl := tableBySource[ref.Source]
		// The cast policy must reach DDL: an empty policy here creates a
		// table whose types diverge from the collapsed runner's (audit #8).
		cast, err := coreCastOf(tbl)
		if err != nil {
			return err
		}
		if err := snk.EnsureTable(ctx, ref, canonical[ref.Source], tbl.PartitionBy, cast, tbl.WriteMode.ChangeMode()); err != nil {
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

	opts := []grpc.ServerOption{
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
		grpc.MaxRecvMsgSize(128 << 20),
		grpc.MaxSendMsgSize(128 << 20),
	}
	if c.cfg.TLS.Enabled() {
		tlsOpt, err := c.cfg.TLS.ServerOption()
		if err != nil {
			return fmt.Errorf("coordinator: tls: %w", err)
		}
		opts = append(opts, tlsOpt)
		c.log.Info("coordinator: control plane mTLS enabled")
	} else {
		c.log.Warn("coordinator: control plane is PLAINTEXT — the Assignment carries the source DSN; set TLS cert/key/CA")
	}
	grpcServer := grpc.NewServer(opts...)
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
	// A source that can take the resolved schema (optional interface) gets
	// it now: the source boundary then gates on drift against the native
	// shape and encodes stable batches. Keyed by the TARGET the changes are
	// addressed to.
	if si, ok := rdr.(source.SchemaSetter); ok {
		byTarget := make(map[string]core.Schema, len(refs))
		for _, ref := range refs {
			if cs, ok := c.canonical[ref.Source]; ok {
				byTarget[ref.Target] = cs
			}
		}
		si.SetSourceSchemas(byTarget)
	}
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
	if err := rdr.Start(ctx, start); err != nil {
		return fmt.Errorf("coordinator: start stream: %w", err)
	}
	// Batch-native pump (G0/M4): the reader's batches are forwarded whole
	// and serialized once per batch — no decode back to changes, no
	// per-change one-row Flight batch. The FIFO queue preserves the wire
	// ordering the window protocol needs.
	out, streamErr := sourceBatches(ctx, rdr)
	go c.pump(ctx, out)

	// The snapshot runs in its own goroutine: run's terminal select must
	// stay live underneath it. A worker dying mid-snapshot otherwise wedges
	// the run forever — the snapshot loop blocks on waitChunkReady, the
	// session error lands in sessionErrs, and nobody reads it (audit #1).
	snapCtx, snapCancel := context.WithCancel(ctx)
	defer snapCancel()
	c.snapshotActive.Store(true)
	snapDone := make(chan error, 1)
	go func() {
		defer c.snapshotActive.Store(false)
		defer snapCancel()
		defer close(snapDone)
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
				snapDone <- fmt.Errorf("coordinator: chunker %s: %w", ref.Source, err)
				return
			}
			if err := c.snapshotTable(snapCtx, rdr, chunker, ref, snapCfg); err != nil {
				snapDone <- fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
				return
			}
			c.log.Info("coordinator snapshot done", "table", ref.Source)
			if err := c.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source}); err != nil {
				c.log.Warn("coordinator: eventlog emit", "err", err)
			}
		}
		snapDone <- nil
	}()

	// Wait for the snapshot, aborting on any terminal signal: the snapshot
	// cannot progress without its worker, and a session or stream death
	// mid-snapshot is a real run error — not a wedge to wait out.
	select {
	case err := <-snapDone:
		if err != nil {
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "snapshot"})
			return err
		}
	case <-ctx.Done():
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "shutdown"})
		return ctx.Err()
	case err := <-c.sessionErrs:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "session"})
		return fmt.Errorf("coordinator: worker session: %w", err)
	case err := <-streamErr:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "stream"})
		return fmt.Errorf("coordinator: stream: %w", err)
	case err := <-c.terminate:
		c.gracefulShutdown()
		c.emitLog(eventlog.KindJobTerminated, map[string]any{"reason": "crashloop"})
		return err
	}

	// Supervision after the snapshot phase: acks only flow once the stream
	// is live, so a long snapshot must not look like a stale worker.
	go c.supervisor.run(ctx, supervisionConfig(c.cfg), c.terminate)

	// Block until the world ends. ctx.Done is checked first on every pass so
	// a cancelled run never races a session defer's context.Canceled into
	// the report as a spurious worker failure (audit #11); the ctx.Err()
	// guard on the error cases closes the residual race. gracefulShutdown
	// runs on every exit.
	for {
		select {
		case <-ctx.Done():
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "shutdown"})
			return ctx.Err()
		case err := <-c.terminate:
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobTerminated, map[string]any{"reason": "crashloop"})
			return err
		case err := <-streamErr:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "stream"})
			return fmt.Errorf("coordinator: stream: %w", err)
		case err := <-c.sessionErrs:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.gracefulShutdown()
			c.emitLog(eventlog.KindJobStopped, map[string]any{"reason": "session"})
			return fmt.Errorf("coordinator: worker session: %w", err)
		}
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
	// Point-in-time enrichment is visible to the operator: enriched columns
	// are NOT reproducible by replay (the reference is a snapshot, not
	// CDC), and that trade is declared, not hidden.
	for _, t := range c.cfg.Spec.Tables {
		if len(t.Enrich) > 0 {
			st["enrichment"] = "point-in-time"
			break
		}
	}
	ws := map[string]*workerStatus{}
	// One lock across the whole iteration: statusz runs from the metrics
	// server, which boots BEFORE run() populates c.workers — a per-entry
	// lock still races the map write on boot (audit #13).
	c.mu.Lock()
	for name, w := range c.workers {
		// Phase reflects reality: the field used to hardcode "attached" for
		// every worker, which lied about detached/pending ones.
		phase := "detached"
		if w.attached {
			phase = "attached"
		}
		ws[name] = &workerStatus{
			Phase:     phase,
			Epoch:     w.epoch,
			Attached:  w.attached,
			Inflight:  c.budget.inFlight(name),
			Committed: w.committed,
		}
	}
	c.mu.Unlock()
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
// Counting ready signals would miscount a flapping worker that attaches,
// dies, and reattaches inside the window (audit #4) — so the wait checks
// the attached flag directly, woken by each attach and the poll.
func (c *Coordinator) waitWorkers(ctx context.Context, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	allAttached := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, w := range c.workers {
			if !w.attached {
				return false
			}
		}
		return true
	}
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	for {
		if allAttached() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("coordinator: not all workers connected within %s", wait)
		}
		select {
		case <-c.ready:
		case <-poll.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// pump encodes reader events into the data queue. While a DBLog window is
// open (gateOn), events of the gated table are buffered instead — released
// InWindow-tagged by flushWindow after the worker confirms ChunkReady. Other
// tables flow freely.
func (c *Coordinator) pump(ctx context.Context, out <-chan *dataplane.Batch) {
	for {
		select {
		case b, ok := <-out:
			if !ok {
				return
			}
			if c.metrics != nil {
				c.metrics.EventsDecoded.Inc()
			}
			if c.gateHold(ctx, b) {
				continue
			}
			// enqueueBatch takes ownership of b (serializes + releases).
			if err := c.enqueueBatch(ctx, b, nil); err != nil {
				c.log.Warn("coordinator: enqueue failed", "err", err)
				// A pump death is a real failure: the reader stalls behind the
				// closed out channel and the coordinator stays "alive" doing
				// nothing (audit #9). Surface it on the terminal plane; on a
				// cancelled pipeline run's select already owns the exit.
				if ctx.Err() == nil {
					select {
					case c.terminate <- fmt.Errorf("coordinator: pump: %w", err):
					default:
					}
				}
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// gateMaxEvents bounds one snapshot window's held live batches. Beyond it
// the pump blocks until flushWindow drains — the gate's structural
// backpressure (audit #5). Tuned to a few minutes of a busy table at
// ~10k/s; batches are source-sized (≤ batchTarget rows), so the row volume
// held is bounded by gateMaxEvents × batchTarget.
const gateMaxEvents = 1024

// gateHold buffers a batch when a window is open for its table. A full
// gate blocks the pump until the snapshot drains it, instead of growing the
// buffer without bound. Ownership: when gateHold returns true the batch is
// in the gate and released by flushWindow/closeWindow.
//
// If the context dies while the pump waits on a full gate, gateHold returns
// false and the batch is treated as live (not gated). That is only reachable
// during shutdown, where the pump exits on ctx.Done immediately after — it
// must not be relied on in any live path.
func (c *Coordinator) gateHold(ctx context.Context, b *dataplane.Batch) bool {
	c.gateMu.Lock()
	if !c.gateOn || b.Table != c.gateTgt {
		c.gateMu.Unlock()
		return false
	}
	full := len(c.gateBuf) >= gateMaxEvents
	c.gateMu.Unlock()
	if full {
		select {
		case <-c.gateDrain:
		case <-ctx.Done():
			return false
		}
	}
	c.gateMu.Lock()
	// Re-check after the wait: the gate may have drained, closed, or the
	// table changed while the pump was asleep.
	if !c.gateOn || b.Table != c.gateTgt {
		c.gateMu.Unlock()
		return false
	}
	c.gateBuf = append(c.gateBuf, b)
	c.gateMu.Unlock()
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

// flushWindow drains the gated batches collected since the last drain,
// each InWindow-tagged for the given chunk, then returns (gate stays open).
// Batch ownership transfers to enqueueBatch per drain.
func (c *Coordinator) flushWindow(ctx context.Context, chunkID uint32) error {
	c.gateMu.Lock()
	buf, tgt := c.gateBuf, c.gateTgt
	c.gateBuf = nil
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()

	meta := &pb.BatchMeta{
		Table:  tgt,
		Window: &pb.WindowTag{InWindow: true, ChunkId: chunkID},
	}
	for i, b := range buf {
		if err := c.enqueueBatch(ctx, b, meta); err != nil {
			for _, rest := range buf[i+1:] {
				rest.Release()
			}
			return err
		}
	}
	return nil
}

// closeWindow releases any remaining gated batches (post-last-chunk) and
// closes the gate. The trailing events are ordinary live changes: no window
// tag.
func (c *Coordinator) closeWindow(ctx context.Context) error {
	c.gateMu.Lock()
	buf, tgt := c.gateBuf, c.gateTgt
	c.gateOn, c.gateBuf = false, nil
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()

	meta := &pb.BatchMeta{Table: tgt}
	for i, b := range buf {
		if err := c.enqueueBatch(ctx, b, meta); err != nil {
			for _, rest := range buf[i+1:] {
				rest.Release()
			}
			return err
		}
	}
	return nil
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

// enqueueBatch queues ONE serialized batch on a worker's Flight stream and
// charges its share of the global flow budget. A full budget blocks here —
// the backpressure that stalls the pump and, through it, the reader. The
// charge is released when the worker's Ack covers the batch's position
// (onAck).
//
// Two shapes:
//   - b != nil: a source batch, serialized ONCE as-is (no per-row re-encode).
//     meta may be nil (plain live) or carry a window tag. Table and the
//     commit position are derived from the batch when the meta lacks them.
//     OWNERSHIP: enqueueBatch always releases b on every exit.
//   - b == nil: a marker batch (window Closes) — an empty record whose meta
//     carries the position and the window tag.
func (c *Coordinator) enqueueBatch(ctx context.Context, b *dataplane.Batch, meta *pb.BatchMeta) error {
	if b != nil {
		defer b.Release()
	}
	if b == nil && (meta == nil || meta.Table == "") {
		return fmt.Errorf("coordinator: marker batch requires a table in meta")
	}
	if meta == nil {
		meta = &pb.BatchMeta{}
	}
	w, ok := c.route[meta.Table]
	if !ok {
		if b != nil {
			meta.Table = b.Table
		}
		w, ok = c.route[meta.Table]
		if !ok {
			return fmt.Errorf("coordinator: no worker owns table %s", meta.Table)
		}
	}
	meta.BatchId = c.batchSeq.Add(1)

	var body []byte
	var metaBytes []byte
	var err error
	if b == nil {
		// Resolve the canonical schema for typed wire encoding of the
		// zero-row marker record. A marker whose table is not in refs would
		// otherwise encode with a zero-value schema (0 data columns) and be
		// rejected downstream with an error pointing at the wrong place.
		var cs core.Schema
		found := false
		for _, ref := range c.refs {
			if ref.Target == meta.Table {
				cs = c.canonical[ref.Source]
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("coordinator: marker batch: table %q has no canonical schema (not in refs)", meta.Table)
		}
		body, metaBytes, err = transport.EncodeBatch(nil, cs, meta, nil)
		if err != nil {
			return err
		}
	} else {
		// The batch already carries the wire schema (data + metadata
		// columns); serialize it whole. The commit position is the last
		// row's __pos — ack truncation frees the batch once the worker
		// commits at or past it.
		if meta.HighPos == "" {
			reader, rerr := transport.NewBatchReader(b.Record, nil)
			if rerr != nil {
				return rerr
			}
			if reader.NumRows() > 0 {
				meta.HighPos = reader.Position(reader.NumRows() - 1)
			}
		}
		body, err = transport.EncodeRecord(b.Record)
		if err != nil {
			return err
		}
		metaBytes, err = proto.Marshal(meta)
		if err != nil {
			return fmt.Errorf("coordinator: marshal batch meta: %w", err)
		}
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
	// The audit trail upload is a synchronous S3 put; on the ack hot path a
	// slow endpoint would delay budget release and trip the supervisor's
	// stale-ack resets (audit #15). Fire it and forget.
	go func() {
		if err := c.emit(eventlog.KindCommit, map[string]any{
			"worker":   worker,
			"table":    ack.Table,
			"rows":     ack.Rows,
			"deletes":  ack.Deletes,
			"position": ack.Position,
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}()
}

// assignmentFor builds one worker's table assignment with its own ticket.
// The table schema travels as Arrow IPC derived from the canonical schema —
// the same typed discipline as the Flight data plane, no JSON on the wire.
func (c *Coordinator) assignmentFor(w *workerState) (*pb.CoordinatorMessage, error) {
	// The assignment's epoch is the worker's CURRENT generation: the worker
	// echoes it in its ready Hello, and onHello rejects any other value. A
	// hardcoded 1 diverges from w.epoch's lifecycle (0 on first boot, ++ on
	// every reset) and silently drops every ready Hello's committed map
	// (audit #6).
	c.mu.Lock()
	epoch := w.epoch
	c.mu.Unlock()
	assign := &pb.Assignment{
		WorkerName: w.name,
		Epoch:      epoch,
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
		ta := &pb.TableAssignment{
			SourceTable:       ref.Source,
			TargetTable:       ref.Target,
			WriteMode:         pb.WriteMode_WRITE_MODE_UPSERT,
			PrimaryKey:        ref.PrimaryKey,
			CreateIfNotExists: true,
			SchemaArrow:       schemaB,
		}
		// The table's write shape travels with the assignment so the worker's
		// collapse and the coordinator's DDL agree: the per-table write mode
		// (a hardcoded UPSERT here silently flipped append tables to upsert
		// semantics, audit #10) and the cast/metadata JSON (a dropped cast
		// policy made the worker create a table divergent from the
		// coordinator's, audit #8).
		tbl, ok := c.specForSource(ref.Source)
		if ok {
			ta.WriteMode = writeModeToPB(tbl.WriteMode.ChangeMode())
			cast, cerr := coreCastOf(tbl)
			if cerr != nil {
				return nil, cerr
			}
			if castB, err := json.Marshal(cast); err != nil {
				return nil, fmt.Errorf("coordinator: cast %s: %w", ref.Source, err)
			} else {
				ta.CastPolicy = castB
			}
			if metaB, err := json.Marshal(tbl.Metadata); err != nil {
				return nil, fmt.Errorf("coordinator: metadata %s: %w", ref.Source, err)
			} else {
				ta.Metadata = metaB
			}
		}
		// Broadcast reference joins travel with the assignment: the worker
		// owns the join, the coordinator only forwards the declaration.
		for _, t := range c.cfg.Spec.Tables {
			if t.Source != ref.Source {
				continue
			}
			for _, e := range t.Enrich {
				ta.Enrich = append(ta.Enrich, &pb.EnrichRef{
					Table:           e.Table,
					SourceUri:       e.Source.URI,
					SourceQuery:     e.Source.Query,
					On:              e.On,
					Select:          e.Select,
					As:              e.As,
					JoinType:        e.JoinType,
					Refresh:         e.Refresh,
					OnColdStart:     e.OnColdStart,
					BufferMaxEvents: int64(e.BufferLimits.MaxEvents),
					BufferMaxWait:   e.BufferLimits.MaxWait,
				})
			}
			break
		}
		assign.Tables = append(assign.Tables, ta)
	}
	return &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: assign}}, nil
}

// specForSource finds the spec table for a source name.
func (c *Coordinator) specForSource(src string) (spec.Table, bool) {
	for _, t := range c.cfg.Spec.Tables {
		if t.Source == src {
			return t, true
		}
	}
	return spec.Table{}, false
}

// writeModeToPB maps the dataplane write mode onto the wire enum.
func writeModeToPB(m dataplane.WriteMode) pb.WriteMode {
	if m == dataplane.AppendMode {
		return pb.WriteMode_WRITE_MODE_APPEND
	}
	return pb.WriteMode_WRITE_MODE_UPSERT
}

// coreCastOf parses the spec's cast map into the policy the coordinator's
// DDL and the worker's writes must both apply. Parse errors are ignored the
// same way the collapsed runner ignores them (the cast is re-validated on
// the write path); the coordinator must not diverge from the runner.
// coreCastOf parses the table's declared cast policy, failing loud: a
// swallowed parse error would ship an empty policy, creating a sink table
// whose types diverge from the spec (audit #8). The source Introspect
// validates the same string at boot, so this cannot normally fail — but the
// coordinator must not degrade silently if it ever does.
func coreCastOf(tbl spec.Table) (core.CastPolicy, error) {
	cast, err := core.ParseCastPolicy(tbl.Cast)
	if err != nil {
		return core.CastPolicy{}, fmt.Errorf("coordinator: table %s cast: %w", tbl.Target, err)
	}
	return cast, nil
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
		if c := p.Compare(best); c != position.Incomparable && c < 0 {
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
	}
	c.mu.Unlock()
	if known {
		// noteAttach takes the supervisor lock; calling it under c.mu would
		// invert the order supervisor.tick uses (supervisor.mu → c.mu) and
		// deadlock the two (audit #3).
		c.supervisor.noteAttach(hello.WorkerName)
	}
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
		} else if c.snapshotActive.Load() {
			// A reset (or reset-death) mid-snapshot is not a worker failure,
			// but the snapshot cannot continue: fail the run so it restarts
			// and re-snapshots cleanly (CD-5).
			c.sessionErrs <- fmt.Errorf("coordinator: worker %s session lost during snapshot: %w", hello.WorkerName, retErr)
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
//
// A batch is only abandoned once a Send succeeds. If the stream dies
// mid-Send, the batch stays on the worker's resend slot and the next DoGet
// delivers it BEFORE draining the queue — FIFO must not reorder it behind
// younger batches, and the budget charge is only released by an Ack that
// truncates past it (audit #2).
func (s *flightServer) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	w, ok := s.c.byTicket[string(req.Ticket)]
	if !ok {
		return fmt.Errorf("coordinator: unknown flight ticket %q", string(req.Ticket))
	}
	if !w.activeGet.CompareAndSwap(false, true) {
		return status.Error(codes.ResourceExhausted, "coordinator: a DoGet stream is already active for this worker")
	}
	defer w.activeGet.Store(false)
	for {
		// Deliver any resend first: it was popped ahead of the queue's head,
		// so it must land ahead of it too.
		w.resendMu.Lock()
		pending := w.resend
		w.resendMu.Unlock()
		if pending != nil {
			if err := stream.Send(&flight.FlightData{
				DataHeader:  []byte("urutau-batch"),
				DataBody:    pending.body,
				AppMetadata: pending.meta,
			}); err != nil {
				return err // keep resend for the next attempt
			}
			w.resendMu.Lock()
			if w.resend == pending {
				w.resend = nil
			}
			w.resendMu.Unlock()
			continue
		}

		select {
		case qb := <-w.queue:
			if err := stream.Send(&flight.FlightData{
				DataHeader:  []byte("urutau-batch"),
				DataBody:    qb.body,
				AppMetadata: qb.meta,
			}); err != nil {
				w.resendMu.Lock()
				if w.resend == nil {
					w.resend = &qb
				}
				w.resendMu.Unlock()
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
		// Same rule as randTicket: a deterministic fallback would make
		// runIDs collide across restarts and overwrite a prior run's S3
		// manifests. crypto/rand failure is fatal, not degraded.
		panic(fmt.Sprintf("coordinator: crypto/rand: %v", err))
	}
	return hex.EncodeToString(b)[:n]
}

// randTicket is the worker's Flight DoGet ticket: 128 random bits so a
// worker's stream cannot be opened by guessing "urutau/<name>" (audit #3).
// crypto/rand failure is fatal — a deterministic fallback would restore the
// guessable-ticket hole the randomness exists to close.
func randTicket() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("coordinator: crypto/rand: %v", err))
	}
	return []byte(hex.EncodeToString(b))
}

// tableNames renders a group's source tables for the audit trail.
func tableNames(refs []source.TableRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Source
	}
	return out
}

// sourceBatches forwards the reader's columnar batches to the pump — no
// decode, no per-row hop (G0/M4). Ownership: each batch moves to the pump,
// which gates, serializes and releases it.
func sourceBatches(ctx context.Context, rdr source.Reader) (<-chan *dataplane.Batch, <-chan error) {
	out := make(chan *dataplane.Batch, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(out)
		for {
			b, err := rdr.Next(ctx)
			if err != nil {
				errCh <- err
				return
			}
			if b == nil {
				errCh <- nil
				return
			}
			select {
			case out <- b:
			case <-ctx.Done():
				b.Release()
				return
			}
		}
	}()
	return out, errCh
}
