// Package coordinator drives the source side of the split pipeline: it owns
// the replication reader and the DBLog snapshot, serves the control plane
// (Session/Assignment) and the Arrow Flight data plane, and streams change
// batches to the connected workers. Tables map to worker groups
// (spec.Tables[].Worker; a table without a group owns its own worker), each
// group gets its own Flight queue, and one batch always routes to exactly
// one worker.
package coordinator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/apache/iceberg-go/table"
	"github.com/maltzsama/urutau/internal/adapter"
	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/position"
	foziceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/source/dblog"
	"github.com/maltzsama/urutau/internal/spec"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
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

	// WaitWorker bounds how long the coordinator waits for every expected
	// worker session before failing the boot.
	WaitWorker time.Duration

	Logger *slog.Logger
}

// workerQueueCap bounds one worker's in-flight batches — the structural
// backpressure hop of design §1.1 (workerCh cap 64).
const workerQueueCap = 64

// queuedBatch is one encoded batch waiting for the Flight stream.
type queuedBatch struct {
	rec  arrow.RecordBatch
	meta []byte
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

	ready       chan struct{} // one send per attached session
	sessionErrs chan error    // first exit wins
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
	attached bool
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
		ready:       make(chan struct{}, 1024),
		sessionErrs: make(chan error, 1024),
	}
	return c.run(ctx)
}

func (c *Coordinator) run(ctx context.Context) error {
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
		}
		w.refs = append(w.refs, refs[i])
		c.route[t.Target] = w
	}
	for _, w := range c.workers {
		c.log.Info("coordinator worker group", "worker", w.name, "tables", len(w.refs))
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

	grpcServer := grpc.NewServer()
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
		chunker, err := adapt.NewChunker(qdb, ref.Source, strings.Join(ref.PrimaryKey, ","), c.cfg.ChunkSize)
		if err != nil {
			return err
		}
		if err := dblog.SnapshotTable(ctx, chunker, rdr, &wireRelay{c: c}, ref.Target, snapCfg); err != nil {
			return fmt.Errorf("coordinator: snapshot %s: %w", ref.Source, err)
		}
		c.log.Info("coordinator snapshot done", "table", ref.Source)
	}

	// Block until the world ends.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-streamErr:
		return fmt.Errorf("coordinator: stream: %w", err)
	case err := <-c.sessionErrs:
		return fmt.Errorf("coordinator: worker session: %w", err)
	}
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

// enqueueBatch encodes rows and queues them on the owning worker's Flight
// stream. Blocking on a full queue is the backpressure: it stalls the pump,
// which stalls the reader.
func (c *Coordinator) enqueueBatch(ctx context.Context, rows []change.Change, meta *pb.BatchMeta) error {
	w, ok := c.route[meta.Table]
	if !ok {
		return fmt.Errorf("coordinator: no worker owns table %s", meta.Table)
	}
	rec, metaBytes, err := transport.EncodeBatch(rows, meta)
	if err != nil {
		return err
	}
	select {
	case w.queue <- queuedBatch{rec: rec, meta: metaBytes}:
		return nil
	case <-ctx.Done():
		rec.Release()
		return ctx.Err()
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
	if err := w.c.enqueueBatch(context.Background(), nil, meta); err != nil {
		w.c.log.Warn("coordinator: release enqueue failed", "err", err)
	}
}

func (w *wireRelay) AddWindowRows(target string, chunkID uint32, rows []change.Change) error {
	return w.c.enqueueBatch(context.Background(), rows, &pb.BatchMeta{
		Table:  target,
		Window: &pb.WindowTag{Snapshot: true, ChunkId: chunkID},
	})
}

// assignmentFor builds one worker's table assignment with its own ticket.
func (c *Coordinator) assignmentFor(w *workerState, schemas map[string]*iceberg.Schema) (*pb.CoordinatorMessage, error) {
	assign := &pb.Assignment{
		WorkerName: w.name,
		Ticket:     w.ticket,
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
				c.log.Info("worker ack", "worker", hello.WorkerName, "table", m.Ack.Table,
					"rows", m.Ack.Rows, "position", m.Ack.Position)
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

// Control is the urgent-signal plane; nothing sends on it in this milestone.
func (s *controlServer) Control(stream pb.UrutauControl_ControlServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

// ── Flight data plane ────────────────────────────────────────────────

type flightServer struct {
	flight.BaseFlightServer
	c *Coordinator
}

// DoGet streams the worker's queued batches; the ticket (from its
// Assignment) selects which queue. Each FlightData carries one complete IPC
// stream in DataBody (schema + one record) and a BatchMeta proto in
// AppMetadata — self-consistent with the worker's reader.
func (s *flightServer) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	w, ok := s.c.byTicket[string(req.Ticket)]
	if !ok {
		return fmt.Errorf("coordinator: unknown flight ticket %q", string(req.Ticket))
	}
	for {
		select {
		case qb := <-w.queue:
			var buf bytes.Buffer
			w := ipc.NewWriter(&buf, ipc.WithSchema(transport.ChangeSchema))
			if err := w.Write(qb.rec); err != nil {
				qb.rec.Release()
				_ = w.Close()
				return err
			}
			if err := w.Close(); err != nil {
				qb.rec.Release()
				return err
			}
			qb.rec.Release()
			if err := stream.Send(&flight.FlightData{
				DataHeader:  []byte("urutau-batch"),
				DataBody:    buf.Bytes(),
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
