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
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/apache/iceberg-go/table"
	"github.com/maltzsama/urutau/internal/adapter"
	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/position"
	foziceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/source/dblog"
	"github.com/maltzsama/urutau/internal/spec"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
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

	// FlowTotalBytes is the process-wide ceiling on serialized batch bytes
	// in flight (queued or sent, unacked). FlowPerWorkerMin is the floor
	// that keeps a slow worker from starving. Defaults 512Mi / 16Mi.
	FlowTotalBytes   int64
	FlowPerWorkerMin int64

	// Eventlog is optional: when set, the coordinator writes its per-run
	// audit trail (job_started, snapshots, commits, terminal) to S3.
	Eventlog *eventlog.Config

	// WaitWorker bounds how long the coordinator waits for every expected
	// worker session before failing the boot.
	WaitWorker time.Duration

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

	adapt adapter.Source
	qdb   *sql.DB
	refs  []dblog.TableRef
	cat   *rest.Catalog

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
}

// workerState is one worker group's slice of the pipeline: its tables, its
// Flight queue, and — once it connects — its control surface.
type workerState struct {
	name   string
	refs   []dblog.TableRef
	queue  chan queuedBatch
	ticket []byte

	out      chan *pb.CoordinatorMessage // attached by Session
	control  pb.UrutauControl_ControlServer
	attached bool
	epoch    uint64 // last accepted epoch (guards stale Hellos)

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
	}
	if cfg.FlowTotalBytes <= 0 {
		cfg.FlowTotalBytes = 512 << 20
	}
	if cfg.FlowPerWorkerMin <= 0 {
		cfg.FlowPerWorkerMin = 16 << 20
	}
	c.budget = newFlowBudget(cfg.FlowTotalBytes, cfg.FlowPerWorkerMin)
	c.runID = time.Now().UTC().Format("2006-01-02T15:04:05Z") + "-" + randSuffix(6)
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
	adapt, err := adapter.For(c.cfg.Spec, adapter.Runtime{
		ServerID:  c.cfg.ServerID,
		Heartbeat: c.cfg.Heartbeat,
		Logger:    c.log,
	})
	if err != nil {
		return err
	}
	c.adapt = adapt

	qdb, err := adapt.OpenQuery(ctx)
	if err != nil {
		return fmt.Errorf("coordinator: open query db: %w", err)
	}
	defer func() { _ = qdb.Close() }()
	c.qdb = qdb

	refs := make([]dblog.TableRef, 0, len(c.cfg.Spec.Tables))
	schemas := make(map[string]*iceberg.Schema, len(c.cfg.Spec.Tables))
	for _, t := range c.cfg.Spec.Tables {
		ref, is, err := adapt.Introspect(ctx, qdb, t)
		if err != nil {
			return err
		}
		refs = append(refs, ref)
		schemas[t.Source] = is
	}
	c.refs = refs

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
			c.index[name] = newPositionIndex(name)
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

	// Catalog + tables: the coordinator owns DDL.
	cat, err := foziceberg.NewCatalog(ctx, catalogConfig(c.cfg.Spec))
	if err != nil {
		return fmt.Errorf("coordinator: catalog: %w", err)
	}
	c.cat = cat
	if err := foziceberg.EnsureNamespace(ctx, cat, table.Identifier{c.cfg.Spec.Sink.Namespace}); err != nil {
		return err
	}
	for _, ref := range refs {
		if err := foziceberg.EnsureTable(ctx, cat, targetIdent(c.cfg.Spec, ref.Target), schemas[ref.Source]); err != nil {
			return fmt.Errorf("coordinator: ensure %s: %w", ref.Target, err)
		}
	}

	resume, needsSnapshot, err := c.resumeFrom(ctx, adapt, refs)
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

	// Assignment: each worker gets its tables with the resolved schemas and
	// its own Flight ticket.
	if err := c.sendAssignments(ctx, schemas); err != nil {
		return err
	}

	// Reader + stream, then snapshot — the DBLog loop the collapsed runner
	// runs, routed over the wire instead of an in-process channel.
	out := make(chan change.Change, 1024)
	rdr, err := adapt.NewReader(ctx, qdb, refs, out)
	if err != nil {
		return err
	}
	defer rdr.Close()

	start := resume
	if start == nil {
		m, err := adapt.InitialPosition(ctx, qdb)
		if err != nil {
			return fmt.Errorf("coordinator: initial position: %w", err)
		}
		start = m
	}
	streamErr := make(chan error, 1)
	go func() { streamErr <- rdr.Start(ctx, start) }()

	// Pump: every decoded change becomes one Flight batch. The FIFO queue
	// preserves the wire ordering the window protocol needs, so the
	// in-process flushReq drain disappears.
	go c.pump(ctx, out)

	snapCfg := dblog.SnapshotConfig{
		WindowTimeout: c.cfg.WindowTimeout,
		CaughtUpPoll:  c.cfg.CaughtUpPoll,
	}
	for _, ref := range needsSnapshot {
		c.log.Info("coordinator snapshot", "table", ref.Source)
		if err := c.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
		chunker, err := adapt.NewChunker(qdb, ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
		if err != nil {
			return err
		}
		if err := dblog.SnapshotTable(ctx, chunker, rdr, &wireRelay{c: c}, ref.Target, snapCfg); err != nil {
			return fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
		}
		c.log.Info("coordinator snapshot done", "table", ref.Source)
		if err := c.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}

	// Block until the world ends.
	select {
	case <-ctx.Done():
		c.gracefulShutdown()
		c.emit(eventlog.KindJobStopped, map[string]any{"reason": "shutdown"})
		return ctx.Err()
	case err := <-streamErr:
		c.emit(eventlog.KindJobStopped, map[string]any{"reason": "stream"})
		return fmt.Errorf("coordinator: stream: %w", err)
	case err := <-c.sessionErrs:
		c.emit(eventlog.KindJobStopped, map[string]any{"reason": "session"})
		return fmt.Errorf("coordinator: worker session: %w", err)
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

// sendAssignments delivers each worker its table slice over its session.
func (c *Coordinator) sendAssignments(ctx context.Context, schemas map[string]*iceberg.Schema) error {
	for _, w := range c.workers {
		msg, err := c.assignmentFor(w, schemas)
		if err != nil {
			return err
		}
		select {
		case w.out <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// pump encodes reader events into the data queue.
func (c *Coordinator) pump(ctx context.Context, out <-chan change.Change) {
	for {
		select {
		case ch, ok := <-out:
			if !ok {
				return
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
	body, metaBytes, err := transport.EncodeBatch(rows, meta)
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
		high, err = c.adapt.ParsePosition(posStr)
		if err != nil {
			c.budget.release(w.name, n)
			return fmt.Errorf("coordinator: batch %s position %q: %w", meta.Table, posStr, err)
		}
	}
	select {
	case w.queue <- queuedBatch{body: body, meta: metaBytes}:
		c.index[w.name].add(inflightBatch{table: meta.Table, high: high, bytes: n})
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
	pos, err := c.adapt.ParsePosition(ack.Position)
	if err != nil {
		c.log.Warn("coordinator: ack position", "worker", worker, "err", err)
		return
	}
	freed := c.index[worker].truncate(ack.Table, pos)
	if freed > 0 {
		c.budget.release(worker, freed)
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

// wireRelay routes the DBLog orchestrator's calls over the data queue.
type wireRelay struct {
	c *Coordinator
}

func (w *wireRelay) Release(table string, chunkID uint32, at position.Position) {
	// The Closes marker travels as an empty batch; its position rides in
	// LowPos. FIFO ordering guarantees every window row and every InWindow
	// event is already ahead of it.
	meta := &pb.BatchMeta{
		Table:  table,
		LowPos: at.String(),
		Window: &pb.WindowTag{Closes: true, ChunkId: chunkID},
	}
	if err := w.c.enqueueBatch(w.c.runCtx, nil, meta); err != nil {
		w.c.log.Warn("coordinator: release enqueue failed", "err", err)
	}
}

func (w *wireRelay) AddWindowRows(target string, chunkID uint32, rows []change.Change) error {
	return w.c.enqueueBatch(w.c.runCtx, rows, &pb.BatchMeta{
		Table:  target,
		Window: &pb.WindowTag{Snapshot: true, ChunkId: chunkID},
	})
}

// assignmentFor builds one worker's table assignment with its own ticket.
func (c *Coordinator) assignmentFor(w *workerState, schemas map[string]*iceberg.Schema) (*pb.CoordinatorMessage, error) {
	assign := &pb.Assignment{
		WorkerName: w.name,
		Epoch:      uint64(1),
		RunId:      c.runID,
		Ticket:     w.ticket,
		SourceKind: c.cfg.Spec.Source.Kind,
		Batching: &pb.BatchConfig{
			MaxInterval: durationpb.New(2 * time.Second),
		},
	}
	for _, ref := range w.refs {
		schemaJSON, err := json.Marshal(schemas[ref.Source])
		if err != nil {
			return nil, fmt.Errorf("coordinator: schema %s: %w", ref.Source, err)
		}
		assign.Tables = append(assign.Tables, &pb.TableAssignment{
			SourceTable:       ref.Source,
			TargetTable:       ref.Target,
			WriteMode:         pb.WriteMode_WRITE_MODE_UPSERT,
			PrimaryKey:        ref.PrimaryKey,
			CreateIfNotExists: true,
			SchemaJson:        string(schemaJSON),
		})
	}
	return &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: assign}}, nil
}

// resumeFrom reads cdc.position per target table; the minimum across tables
// is the resume point, tables without one need the snapshot.
func (c *Coordinator) resumeFrom(ctx context.Context, adapt adapter.Source, refs []dblog.TableRef) (position.Position, []dblog.TableRef, error) {
	var positions []position.Position
	var needsSnapshot []dblog.TableRef
	for _, ref := range refs {
		tbl, err := c.cat.LoadTable(ctx, targetIdent(c.cfg.Spec, ref.Target))
		if err != nil {
			return nil, nil, fmt.Errorf("coordinator: load %s: %w", ref.Target, err)
		}
		if pos := tbl.Properties()["cdc.position"]; pos != "" {
			p, err := adapt.ParsePosition(pos)
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
	sess := &workerSession{
		out:  make(chan *pb.CoordinatorMessage, 16),
		done: make(chan error, 1),
	}
	// Publish the session BEFORE signaling ready: the ready send
	// happens-before run's receive, so run may use w.out the moment it
	// wakes — attaching after the signal is a data race.
	if known {
		w.out, w.attached = sess.out, true
	}
	c.mu.Unlock()
	if !known {
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}

	defer func() {
		c.mu.Lock()
		w.attached, w.out = false, nil
		c.mu.Unlock()
		c.sessionErrs <- retErr
	}()

	select {
	case c.ready <- struct{}{}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	c.log.Info("worker session", "worker", hello.WorkerName)

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
				c.log.Info("chunk ready", "table", m.ChunkReady.Table, "chunk", m.ChunkReady.ChunkId)
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
		}
	}
}

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
		c.sessionErrs <- retErr
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

// ── Catalog helpers (mirror the runner's) ────────────────────────────

func catalogConfig(s *spec.Spec) foziceberg.Config {
	return foziceberg.Config{
		URI:          s.Sink.URI,
		Warehouse:    s.Sink.Warehouse,
		ClientID:     s.Sink.ClientID,
		ClientSecret: s.Sink.ClientSecret,
		Scope:        s.Sink.Scope,
	}
}

func targetIdent(s *spec.Spec, target string) table.Identifier {
	if ns, name, ok := strings.Cut(target, "."); ok {
		return table.Identifier{ns, name}
	}
	return table.Identifier{s.Sink.Namespace, target}
}

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
func tableNames(refs []dblog.TableRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Source
	}
	return out
}
