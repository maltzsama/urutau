// Package coordinator drives the source side of the split pipeline: it owns
// the replication reader and the DBLog snapshot, serves the control plane
// (Session/Assignment) and the Arrow Flight data plane, and streams change
// batches to connected workers. One worker is validated in this milestone;
// the session registry already scales beyond it.
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

// dataTicket is the Flight ticket the (single) worker's DoGet consumes.
var dataTicket = []byte("urutau-data")

// Config tunes the coordinator for one pipeline.
type Config struct {
	Spec       *spec.Spec
	ListenAddr string // gRPC + Flight listen address ("host:port" or ":0")

	ChunkSize     int
	WindowTimeout time.Duration
	CaughtUpPoll  time.Duration
	ServerID      uint32
	Heartbeat     time.Duration

	// WaitWorker bounds how long the coordinator waits for the first
	// worker session before failing the boot.
	WaitWorker time.Duration

	Logger *slog.Logger
}

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

	queue chan queuedBatch

	session    *workerSession // single-worker milestone
	sessionSet chan struct{}
}

// workerSession is one connected worker's control surface.
type workerSession struct {
	name string
	out  chan *pb.CoordinatorMessage
	done chan error
}

// Run boots the pipeline and blocks until ctx is cancelled or a terminal
// error occurs.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Coordinator{
		cfg:        cfg,
		log:        cfg.Logger,
		queue:      make(chan queuedBatch, 1024),
		sessionSet: make(chan struct{}),
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

	// Wait for the first worker session.
	wait := c.cfg.WaitWorker
	if wait <= 0 {
		wait = 2 * time.Minute
	}
	select {
	case <-c.sessionSet:
	case <-time.After(wait):
		return fmt.Errorf("coordinator: no worker session within %s", wait)
	case <-ctx.Done():
		return ctx.Err()
	}

	// Assignment: tables with their resolved Iceberg schema.
	assign, err := c.assignment(schemas)
	if err != nil {
		return err
	}
	c.session.out <- assign

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
	case err := <-c.session.done:
		return fmt.Errorf("coordinator: worker session: %w", err)
	}
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

// enqueueBatch encodes rows and queues them for the Flight stream.
func (c *Coordinator) enqueueBatch(ctx context.Context, rows []change.Change, meta *pb.BatchMeta) error {
	rec, metaBytes, err := transport.EncodeBatch(rows, meta)
	if err != nil {
		return err
	}
	select {
	case c.queue <- queuedBatch{rec: rec, meta: metaBytes}:
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

// assignment builds the worker's table assignment.
func (c *Coordinator) assignment(schemas map[string]*iceberg.Schema) (*pb.CoordinatorMessage, error) {
	assign := &pb.Assignment{
		Ticket: dataTicket,
		Batching: &pb.BatchConfig{
			MaxInterval: durationpb.New(2 * time.Second),
		},
	}
	for _, ref := range c.refs {
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

// Session accepts one worker: the first Hello claims the single slot; the
// server then streams assignments and collects acks until the worker goes.
func (s *controlServer) Session(stream pb.UrutauControl_SessionServer) error {
	sess := &workerSession{
		out:  make(chan *pb.CoordinatorMessage, 16),
		done: make(chan error, 1),
	}

	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := msg.GetHello()
	if hello == nil {
		return errors.New("coordinator: first worker message must be Hello")
	}
	sess.name = hello.WorkerName
	// Publish the session BEFORE claiming the slot: the sessionSet send
	// happens-before run's receive, so run may read c.session the moment it
	// wakes — writing the field after the send is a data race.
	s.c.session = sess

	select {
	case s.c.sessionSet <- struct{}{}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	s.c.log.Info("worker session", "worker", sess.name)

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
				s.c.log.Info("worker ack", "worker", sess.name, "table", m.Ack.Table,
					"rows", m.Ack.Rows, "position", m.Ack.Position)
			case *pb.WorkerMessage_ChunkReady:
				s.c.log.Info("chunk ready", "table", m.ChunkReady.Table, "chunk", m.ChunkReady.ChunkId)
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
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
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

// DoGet streams the queued batches. Each FlightData carries one complete
// IPC stream in DataBody (schema + one record) and a BatchMeta proto in
// AppMetadata — self-consistent with the worker's reader.
func (s *flightServer) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	if !bytes.Equal(req.Ticket, dataTicket) {
		return errors.New("coordinator: unknown flight ticket")
	}
	for {
		select {
		case qb := <-s.c.queue:
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
