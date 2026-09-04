package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/iceberg-go/table"
	icebergsink "github.com/maltzsama/urutau/internal/sink/iceberg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	"github.com/maltzsama/urutau/internal/drivers"
	"github.com/maltzsama/urutau/internal/position"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// RemoteConfig wires one distributed worker: where the coordinator lives,
// which sink this worker owns, and how it batches.
type RemoteConfig struct {
	Coordinator string             // host:port of the coordinator
	Name        string             // worker name (Hello)
	Sink        icebergsink.Config // catalog access — workers own their writes
	Namespace   string             // fallback namespace for bare targets
	MaxRows     int
	MaxInterval time.Duration
	Logger      *slog.Logger
	// MetricsAddr serves /metrics (Prometheus); empty disables it.
	MetricsAddr string

	// FaultStopAck (test-only): commits normally but withholds the ack, so
	// the coordinator's supervisor sees a stale worker — the crashloop
	// proof.
	FaultStopAck bool
}

// sessionSender serializes Session sends: grpc client streams are not
// concurrent-safe, and commits ack from per-table goroutines.
type sessionSender struct {
	mu sync.Mutex
	s  pb.UrutauControl_SessionClient
}

func (s *sessionSender) send(msg *pb.WorkerMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Send(msg)
}

// sessionWithRetry opens the Session stream, tolerating a coordinator that
// is still booting its listener.
func sessionWithRetry(ctx context.Context, conn *grpc.ClientConn, log *slog.Logger) (pb.UrutauControl_SessionClient, error) {
	var last error
	for attempt := 0; attempt < 10; attempt++ {
		s, err := pb.NewUrutauControlClient(conn).Session(ctx)
		if err == nil {
			return s, nil
		}
		last = err
		log.Warn("worker: session retry", "attempt", attempt+1, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return nil, last
}

// RunRemote connects to the coordinator, applies its assignment, and pulls
// change batches over Arrow Flight until the stream ends. All commits go
// through the same collapsed worker core (batcher, windows, collapse).
func RunRemote(ctx context.Context, cfg RemoteConfig) error {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 1000
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 2 * time.Second
	}

	// One ClientConn, one parent context, three coupled streams: Session,
	// Control and Flight all die together — the split-brain correction of
	// design §5.3. dialOpts carries the keepalive that turns a silently
	// frozen coordinator into an error in ~15s.
	conn, err := grpc.NewClient(cfg.Coordinator, dialOpts()...)
	if err != nil {
		return fmt.Errorf("worker: dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	sessCtx, cancelAll := context.WithCancelCause(ctx)
	defer cancelAll(nil)

	session, err := sessionWithRetry(sessCtx, conn, cfg.Logger)
	if err != nil {
		return fmt.Errorf("worker: session: %w", err)
	}
	sender := &sessionSender{s: session}

	// Handshake.
	if err := sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{
		Hello: &pb.Hello{
			WorkerName: cfg.Name,
			Phase:      pb.WorkerPhase_WORKER_PHASE_STARTING,
			Epoch:      1,
		},
	}}); err != nil {
		return fmt.Errorf("worker: hello: %w", err)
	}

	msg, err := session.Recv()
	if err != nil {
		return fmt.Errorf("worker: await assignment: %w", err)
	}
	assign := msg.GetAssign()
	if assign == nil {
		return errors.New("worker: expected Assignment, got none")
	}
	cfg.Logger.Info("assignment", "tables", len(assign.Tables), "run", assign.RunId)

	// Catalog + writers from the assignment: the coordinator owns DDL and
	// introspection, but the writes are this worker's.
	cat, err := drivers.NewCatalogFromConfig(ctx, cfg.Sink)
	if err != nil {
		return fmt.Errorf("worker: catalog: %w", err)
	}
	w := New(Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval, MetricsAddr: cfg.MetricsAddr})
	pkByTable := make(map[string][]string, len(assign.Tables))
	for _, ta := range assign.Tables {
		// The assignment schema arrives as Arrow IPC derived from the
		// coordinator's canonical schema; rebuilding the Iceberg schema
		// through FromCanonical keeps one encoding discipline on the wire.
		cs, err := transport.DecodeTableSchema(ta.SchemaArrow)
		if err != nil {
			return fmt.Errorf("worker: schema %s: %w", ta.TargetTable, err)
		}
		cs.PrimaryKey = ta.PrimaryKey
		schema, err := icebergsink.FromCanonical(cs)
		if err != nil {
			return fmt.Errorf("worker: schema %s: %w", ta.TargetTable, err)
		}
		ident := targetIdent(cfg.Namespace, ta.TargetTable)
		if ta.CreateIfNotExists {
			if err := drivers.EnsureTable(ctx, cat, ident, schema, nil, core.CastPolicy{}); err != nil {
				return fmt.Errorf("worker: ensure %s: %w", ta.TargetTable, err)
			}
		}
		writer, err := drivers.NewTableWriter(ctx, cat, ident, ta.PrimaryKey, core.CastPolicy{}, nil, "")
		if err != nil {
			return fmt.Errorf("worker: writer %s: %w", ta.TargetTable, err)
		}
		w.Register(ta.TargetTable, writer, change.UpsertMode)
		pkByTable[ta.TargetTable] = ta.PrimaryKey
		// The drift check knows the assigned schema's column set: canonical
		// column names are the target table's columns.
		cols := make(map[string]bool, len(cs.Columns))
		for _, col := range cs.Columns {
			cols[col.Name] = true
		}
		w.SetKnownColumns(ta.TargetTable, cols)
	}
	w.OnSchemaDrift(func(d SchemaDrift) {
		cfg.Logger.Error("schema drift: pipeline paused", "table", d.Table, "column", d.Column,
			"action", "coordinator must assign a schema with the column; declare it in the spec")
	})

	// Report phase + committed positions (design §5.6.1): STREAMING if any
	// of our tables has a commit, SNAPSHOTTING otherwise. The committed map
	// also drives the local skip of batches the Iceberg table already
	// covers — the resume idempotence boundary (failure-analysis case 4).
	parsePos := parsePosition(assign.SourceKind)
	committed := make(map[string]position.Position, len(assign.Tables))
	phase := pb.WorkerPhase_WORKER_PHASE_SNAPSHOTTING
	for _, ta := range assign.Tables {
		pos, err := drivers.CommittedPosition(ctx, cat, targetIdent(cfg.Namespace, ta.TargetTable))
		if err != nil {
			return fmt.Errorf("worker: committed %s: %w", ta.TargetTable, err)
		}
		if pos == "" {
			continue
		}
		p, err := parsePos(pos)
		if err != nil {
			return fmt.Errorf("worker: committed %s %q: %w", ta.TargetTable, pos, err)
		}
		committed[ta.TargetTable] = p
		phase = pb.WorkerPhase_WORKER_PHASE_STREAMING
	}
	if err := sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: &pb.Hello{
		WorkerName: cfg.Name,
		Epoch:      assign.Epoch,
		Phase:      phase,
		Committed:  committedStrings(committed),
	}}}); err != nil {
		return fmt.Errorf("worker: ready hello: %w", err)
	}
	cfg.Logger.Info("worker ready", "phase", phase.String(), "committed", len(committed))

	// Pipelines drain on their own ctx: a graceful shutdown signal cancels
	// the streams (sessCtx) but leaves the flush able to commit in-flight
	// rows (design §5.3.2). Only an anomalous channel death aborts it.
	pipeCtx, pipeCancel := context.WithCancel(ctx)
	defer pipeCancel()
	runErr := make(chan error, 1)
	ingest := make(chan change.Change, 1024)
	go func() { runErr <- w.Run(pipeCtx, ingest) }()

	chunks := newChunkExecutor(assign, w, cfg.Logger, sender.send)

	w.OnCommit(func(b change.Batch, rows int) {
		if cfg.FaultStopAck {
			return
		}
		_ = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Ack{Ack: &pb.Ack{
			Table:    b.Table,
			Epoch:    assign.Epoch,
			Position: b.Position,
			Rows:     uint64(rows),
		}}})
	})

	// Control plane (same ClientConn, urgent signals) — the Hello identifies
	// this stream to the server.
	control, err := pb.NewUrutauControlClient(conn).Control(sessCtx)
	if err != nil {
		return fmt.Errorf("worker: control: %w", err)
	}
	if err := control.Send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: &pb.Hello{
		WorkerName: cfg.Name,
		Epoch:      assign.Epoch,
	}}}); err != nil {
		return fmt.Errorf("worker: control hello: %w", err)
	}

	// Data plane: Arrow Flight over the SAME ClientConn, so a dropped
	// connection tears down every stream at once.
	fl, err := flight.NewFlightServiceClient(conn).DoGet(sessCtx, &flight.Ticket{Ticket: assign.Ticket})
	if err != nil {
		return fmt.Errorf("worker: doget: %w", err)
	}

	recv := &batchReceiver{
		w:         w,
		ingest:    ingest,
		committed: committed,
		parsePos:  parsePos,
		pkByTable: pkByTable,
		log:       cfg.Logger,
	}

	// Chunk work is serialized: the coordinator sends one ChunkRequest at a
	// time and waits for ChunkReady, so a single worker slot is enough and
	// ordering between chunks is preserved.
	chunkWork := make(chan *pb.ChunkRequest, 4)
	chunkErr := make(chan error, 1)
	go func() {
		for req := range chunkWork {
			if err := chunks.run(sessCtx, req); err != nil {
				chunkErr <- err
				cancelAll(fmt.Errorf("worker: chunk: %w", err))
				return
			}
		}
	}()

	// Surveillance is by READING each stream (§5.3.1): the first one to
	// die cancels the shared context, which takes the other two with it.
	loops := []struct {
		name string
		step func() error
	}{
		{"session", func() error {
			m, err := session.Recv()
			if err != nil {
				return err
			}
			if cr := m.GetChunk(); cr != nil {
				chunkWork <- cr
			}
			return nil
		}},
		{"control", func() error {
			m, err := control.Recv()
			if err != nil {
				return err
			}
			if m.GetShutdown() != nil {
				return errShutdown
			}
			return nil
		}},
		{"flight", func() error {
			fd, err := fl.Recv()
			if err != nil {
				return err
			}
			return recv.apply(fd)
		}},
	}
	var loopWG sync.WaitGroup
	for _, l := range loops {
		loopWG.Add(1)
		go func(l struct {
			name string
			step func() error
		}) {
			defer loopWG.Done()
			for {
				if err := l.step(); err != nil {
					if errors.Is(err, io.EOF) {
						cancelAll(fmt.Errorf("%w", errGracefulEOF))
					} else if errors.Is(err, errShutdown) {
						cancelAll(errShutdown)
					} else {
						cancelAll(fmt.Errorf("worker: stream %s: %w", l.name, err))
					}
					return
				}
			}
		}(l)
	}

	<-sessCtx.Done()
	// Every read loop has seen the cancellation (or will within a Recv
	// round-trip); wait for them so no loop can still send into ingest when
	// we close it below.
	loopWG.Wait()
	return workerShutdown(context.Cause(sessCtx), pipeCancel, pipeCtx, runErr, ingest, cfg.Logger)
}

// Sentinel causes distinguishing graceful shutdown from channel death.
var (
	errGracefulEOF = errors.New("worker: coordinator closed the stream cleanly")
	errShutdown    = errors.New("worker: shutdown signal received")
)

// workerShutdown drains and exits cleanly when the coordinator intended to
// shut down (graceful EOF, shutdown signal, or the parent ctx cancelling);
// on an anomalous channel death it aborts in-flight transactions instead —
// a commit that completes after the channel is lost is indistinguishable
// from a zombie's (design §5.5).
func workerShutdown(cause error, pipeCancel context.CancelFunc, pipeCtx context.Context,
	runErr <-chan error, ingest chan<- change.Change, log *slog.Logger) error {

	graceful := errors.Is(cause, errGracefulEOF) || errors.Is(cause, errShutdown) ||
		errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded)

	close(ingest)
	if !graceful {
		pipeCancel()
		log.Error("worker: channel lost, aborting in-flight transactions", "cause", cause)
		select {
		case <-runErr:
		case <-time.After(5 * time.Second):
		}
		return fmt.Errorf("worker: channel lost: %w", cause)
	}

	// Graceful: drain whatever is buffered; pipeCtx is still alive unless
	// the parent ctx itself was cancelled.
	log.Info("worker: draining", "cause", cause)
	select {
	case err := <-runErr:
		return err
	case <-time.After(30 * time.Second):
		pipeCancel()
		return fmt.Errorf("worker: drain timeout: %w", cause)
	}
}

// dialOpts carries keepalive that converts a frozen coordinator into a
// dead channel in ~15s. MinTime on the server must be ≤ Time here, or the
// server GOAWAYs the client for pinging too much. The max message size must
// cover a full snapshot window chunk (default 4Mi is too small for real
// batches).
func dialOpts() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(128<<20),
			grpc.MaxCallSendMsgSize(128<<20),
		),
	}
}

// batchReceiver routes decoded Flight batches into the worker core, skipping
// any batch the Iceberg table already covers (resume idempotence, failure-
// analysis case 4): a batch whose high position is at or before the
// committed position was already applied by an earlier run.
type batchReceiver struct {
	w         *Worker
	ingest    chan<- change.Change
	committed map[string]position.Position // target table → committed
	parsePos  func(string) (position.Position, error)
	pkByTable map[string][]string // target table → primary key columns
	log       *slog.Logger
}

// covered reports whether a positioned batch was already committed. Batches
// without a position (snapshot window rows) are never skipped.
func (r *batchReceiver) covered(meta *pb.BatchMeta) bool {
	cp, ok := r.committed[meta.Table]
	if !ok || meta.HighPos == "" {
		return false
	}
	high, err := r.parsePos(meta.HighPos)
	if err != nil {
		return false
	}
	return high.Compare(cp) <= 0
}

// apply routes one decoded batch: snapshot rows build windows, closes
// markers release them, live rows feed ingest.
func (r *batchReceiver) apply(fd *flight.FlightData) error {
	reader, err := ipc.NewReader(bytes.NewReader(fd.DataBody))
	if err != nil {
		return fmt.Errorf("worker: ipc reader: %w", err)
	}
	defer reader.Release()
	rec, err := reader.Read()
	if err != nil {
		return fmt.Errorf("worker: read flight record: %w", err)
	}
	if rec == nil {
		return errors.New("worker: empty flight batch")
	}
	defer rec.Release()

	// The key rebuild needs the table's PK, which lives in the meta —
	// peek at it first (a tiny proto; the codec unmarshals it again).
	meta := &pb.BatchMeta{}
	if err := proto.Unmarshal(fd.AppMetadata, meta); err != nil {
		return fmt.Errorf("worker: unmarshal batch meta: %w", err)
	}
	rows, _, err := transport.DecodeBatch(rec, fd.AppMetadata, r.pkByTable[meta.Table])
	if err != nil {
		return err
	}
	if r.covered(meta) {
		r.log.Info("worker skip covered batch", "table", meta.Table, "high", meta.HighPos)
		return nil
	}
	switch {
	case meta.Window != nil && meta.Window.Snapshot:
		if err := r.w.AddWindowRows(meta.Table, meta.Window.ChunkId, rows); err != nil {
			return err
		}
	case meta.Window != nil && meta.Window.Closes:
		r.ingest <- change.Change{
			Table:    meta.Table,
			Position: meta.LowPos,
			Window:   &change.Window{Closes: true, ChunkID: meta.Window.ChunkId},
		}
	default:
		var win *change.Window
		if meta.Window != nil && meta.Window.InWindow {
			win = &change.Window{InWindow: true, ChunkID: meta.Window.ChunkId}
		}
		for i := range rows {
			rows[i].Window = win
			r.ingest <- rows[i]
		}
	}
	return nil
}

func targetIdent(namespace, target string) table.Identifier {
	if ns, name, ok := strings.Cut(target, "."); ok {
		return table.Identifier{ns, name}
	}
	return table.Identifier{namespace, target}
}

// parsePosition returns the parser for the assignment's source kind.
func parsePosition(kind string) func(string) (position.Position, error) {
	if kind == "postgres" {
		return func(s string) (position.Position, error) { return position.ParseLSN(s) }
	}
	return func(s string) (position.Position, error) { return position.ParseGTID(s) }
}

// committedStrings renders the committed map for the wire Hello.
func committedStrings(m map[string]position.Position) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.String()
	}
	return out
}
