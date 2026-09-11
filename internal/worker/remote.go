package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// RemoteConfig wires one distributed worker: where the coordinator lives,
// which sink this worker owns, and how it batches.
type RemoteConfig struct {
	Coordinator string      // host:port of the coordinator
	Name        string      // worker name (Hello)
	Sink        sink.Config // catalog access — workers own their writes
	Namespace   string      // fallback namespace for bare targets
	MaxRows     int
	MaxInterval time.Duration
	Logger      *slog.Logger
	// MetricsAddr serves /metrics (Prometheus); empty disables it.
	MetricsAddr string

	// TLS is the mutual-TLS material matching the coordinator's control
	// plane. Empty means plaintext.
	TLS grpctls.Config

	// FaultStopAck (test-only): commits normally but withholds the ack, so
	// the coordinator's supervisor sees a stale worker — the crashloop
	// proof.
	FaultStopAck bool
}

// enrichSpecs converts the assignment's reference joins into the spec shape
// the enrich stage consumes.
func enrichSpecs(refs []*pb.EnrichRef) []spec.Enrich {
	cfgs := make([]spec.Enrich, 0, len(refs))
	for _, e := range refs {
		cfgs = append(cfgs, spec.Enrich{
			Table: e.Table,
			Source: spec.EnrichSource{
				URI:   e.SourceUri,
				Query: e.SourceQuery,
			},
			On:           e.On,
			Select:       e.Select,
			As:           e.As,
			JoinType:     e.JoinType,
			Refresh:      e.Refresh,
			OnColdStart:  e.OnColdStart,
			BufferLimits: spec.EnrichBufferLimits{MaxEvents: int(e.BufferMaxEvents), MaxWait: e.BufferMaxWait},
		})
	}
	return cfgs
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
	conn, err := grpc.NewClient(cfg.Coordinator, dialOpts(cfg.TLS)...)
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
	snk, err := driver.OpenSinkConfig(ctx, sink.Config{
		Type:      cfg.Sink.Type,
		URI:       cfg.Sink.URI,
		Namespace: cfg.Namespace,
		Options:   cfg.Sink.Options,
	})
	if err != nil {
		return fmt.Errorf("worker: catalog: %w", err)
	}
	w := New(Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval, MetricsAddr: cfg.MetricsAddr})
	var stages []*enrich.Stage
	pkByTable := make(map[string][]string, len(assign.Tables))
	for _, ta := range assign.Tables {
		// The assignment schema arrives as Arrow IPC derived from the
		// coordinator's canonical schema; the sink maps it to its own
		// storage schema.
		cs, err := transport.DecodeTableSchema(ta.SchemaArrow)
		if err != nil {
			return fmt.Errorf("worker: schema %s: %w", ta.TargetTable, err)
		}
		cs.PrimaryKey = ta.PrimaryKey
		ref := core.TableRef{Target: ta.TargetTable, PrimaryKey: ta.PrimaryKey}
		// The write shape arrives with the assignment: the coordinator's DDL
		// and this worker's writes must agree on the cast policy, the
		// metadata columns, and the write mode — a hardcoded UPSERT or empty
		// cast here silently diverged append tables and cast overrides
		// (audit #8/#10).
		cast, err := decodeCastPolicy(ta)
		if err != nil {
			return err
		}
		meta, err := decodeMetadata(ta)
		if err != nil {
			return err
		}
		mode := dataplane.UpsertMode
		if ta.WriteMode == pb.WriteMode_WRITE_MODE_APPEND {
			mode = dataplane.AppendMode
		}
		// Broadcast reference joins arrive with the assignment. Their
		// destination columns join the assigned schema BEFORE EnsureTable:
		// the sink table must have the column, or the first enriched
		// batch's values are silently dropped (every sink projects by the
		// table's own columns). Event columns are captured before the
		// extension — they are not event columns.
		//
		// The stage is built and, for any wildcard reference, loaded
		// SYNCHRONOUSLY here — before AddColumns/EnsureTable — so the real
		// wildcard columns are known in time to extend cs (#56). An
		// explicit-select reference is unaffected: LoadWildcards skips it,
		// and Start (below) still loads it asynchronously exactly as
		// before.
		var st *enrich.Stage
		if len(ta.Enrich) > 0 {
			enrichCfgs := enrichSpecs(ta.Enrich)
			// SOURCE view, captured BEFORE the reference-column extension
			// — see FT-1: the destinations are not event columns.
			eventSchema := cs
			var err error
			st, err = enrich.New(enrichCfgs, eventSchema, cfg.Logger)
			if err != nil {
				return err
			}
			if err := st.LoadWildcards(ctx); err != nil {
				return fmt.Errorf("worker: %s: enrich: %w", ta.TargetTable, err)
			}
			cs = enrich.AddColumns(cs, st.RefColumns())
		}
		if ta.CreateIfNotExists {
			if err := snk.EnsureTable(ctx, ref, cs, nil, cast, mode); err != nil {
				return fmt.Errorf("worker: ensure %s: %w", ta.TargetTable, err)
			}
		}
		writer, err := snk.Writer(ctx, ref, cast, meta)
		if err != nil {
			return fmt.Errorf("worker: writer %s: %w", ta.TargetTable, err)
		}
		w.Register(ta.TargetTable, writer, mode)
		pkByTable[ta.TargetTable] = ta.PrimaryKey
		// Start's remaining first loads (any explicit-select reference,
		// plus the refresh ticker for everything) stay asynchronous — the
		// cold-start policy applies to whatever hasn't loaded yet.
		if st != nil {
			st.Start(ctx)
			stages = append(stages, st)
			w.SetEnricher(ta.TargetTable, st)
		}
	}
	// The stages' refresh loops live on sessCtx: they die with the session.
	// Close, though, is deterministic — after the pipelines drain.
	defer func() {
		for _, st := range stages {
			st.Stop()
		}
	}()
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
		pos, err := snk.Position(ctx, core.TableRef{Target: ta.TargetTable})
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
	ingest := make(chan Ingest, 1024)
	go func() { runErr <- w.Run(pipeCtx, ingest) }()

	chunks := newChunkExecutor(assign, w, cfg.Logger, sender.send)
	defer chunks.Close()

	w.OnCommit(func(b *dataplane.Batch, rows int) {
		if cfg.FaultStopAck {
			return
		}
		_ = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Ack{Ack: &pb.Ack{
			Table:    b.Table,
			Epoch:    assign.Epoch,
			Position: string(b.Watermark),
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
		ctx:       sessCtx,
		w:         w,
		ingest:    ingest,
		committed: committed,
		parsePos:  parsePos,
		pkByTable: pkByTable,
		log:       cfg.Logger,
	}

	// Chunk work is serialized: the coordinator sends one ChunkRequest at a
	// time and waits for ChunkReady, so a single worker slot is enough and
	// ordering between chunks is preserved. The worker exits on the shared
	// context so it cannot outlive the session.
	chunkWork := make(chan *pb.ChunkRequest, 4)
	go func() {
		for {
			select {
			case req := <-chunkWork:
				if err := chunks.run(sessCtx, req); err != nil {
					cancelAll(fmt.Errorf("worker: chunk: %w", err))
					return
				}
			case <-sessCtx.Done():
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
				// Hand the request to the chunk worker, aborting when the
				// session ends — a bare send with a full buffer would wedge
				// this loop and stall teardown.
				select {
				case chunkWork <- cr:
				case <-sessCtx.Done():
					return errShutdown
				}
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
	runErr <-chan error, ingest chan<- Ingest, log *slog.Logger) error {

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
func dialOpts(tlsCfg grpctls.Config) []grpc.DialOption {
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	if tlsCfg.Enabled() {
		if c, err := tlsCfg.ClientCreds(); err == nil {
			creds = grpc.WithTransportCredentials(c)
		}
	}
	return []grpc.DialOption{
		creds,
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
	ctx       context.Context
	w         *Worker
	ingest    chan<- Ingest
	committed map[string]position.Position // target table → committed
	parsePos  func(string) (position.Position, error)
	pkByTable map[string][]string // target table → primary key columns
	log       *slog.Logger
}

// sendIngest delivers one change into the pipeline, aborting when the
// session ends. A bare send could block forever if the worker's internal
// pipeline has already died — the session would then hang in its teardown
// wait instead of exiting with the pipeline error.
func (r *batchReceiver) sendIngest(ing Ingest) error {
	select {
	case r.ingest <- ing:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
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
	// A batch is covered only when its high position is at or before the
	// committed point. An Incomparable comparison (opaque plugin offsets)
	// cannot decide — never skip, reprocess is safe.
	c := high.Compare(cp)
	return c != position.Incomparable && c <= 0
}

// apply routes one Flight batch: the demux. It builds a *dataplane.Batch
// from the IPC record and routes by BatchMeta (four-readers rule: routing
// tags live in app_metadata, consumed here, never on the record). Snapshot
// window rows build windows; closes markers release them; live rows feed
// ingest as columnar batches. The transport no longer materializes rows.
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
	rec.Retain() // the demux owns the record; the worker releases the Batch

	meta := &pb.BatchMeta{}
	if err := proto.Unmarshal(fd.AppMetadata, meta); err != nil {
		rec.Release()
		return fmt.Errorf("worker: unmarshal batch meta: %w", err)
	}

	if r.covered(meta) {
		rec.Release()
		r.log.Info("worker skip covered batch", "table", meta.Table, "high", meta.HighPos)
		return nil
	}

	b := &dataplane.Batch{
		Table:     meta.Table,
		Record:    rec,
		Watermark: []byte(meta.HighPos),
	}

	switch {
	case meta.Window != nil && meta.Window.Snapshot:
		// AddWindowRows takes ownership of the batch (the window stores it).
		if err := r.w.AddWindowRows(meta.Table, meta.Window.ChunkId, b); err != nil {
			return err
		}
	case meta.Window != nil && meta.Window.Closes:
		b.Release()
		return r.sendIngest(Ingest{
			Table:    meta.Table,
			Win:      &rowchange.Window{Closes: true, ChunkID: meta.Window.ChunkId},
			Position: meta.LowPos,
		})
	default:
		var win *rowchange.Window
		if meta.Window != nil && meta.Window.InWindow {
			win = &rowchange.Window{InWindow: true, ChunkID: meta.Window.ChunkId}
		}
		return r.sendIngest(Ingest{Table: meta.Table, Batch: b, Win: win})
	}
	return nil
}

// parsePosition returns the parser for the assignment's source kind.
// Getting this wrong is not a minor inconvenience: a Kafka pipeline whose
// committed positions were parsed as GTID sets would fail the worker boot
// the moment the first cdc.position existed.
func parsePosition(kind string) func(string) (position.Position, error) {
	switch kind {
	case "postgres":
		return func(s string) (position.Position, error) { return position.ParseLSN(s) }
	case "kafka":
		return func(s string) (position.Position, error) { return position.ParseOffsets(s) }
	default:
		return func(s string) (position.Position, error) { return position.ParseGTID(s) }
	}
}

// committedStrings renders the committed map for the wire Hello.
func committedStrings(m map[string]position.Position) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.String()
	}
	return out
}

// decodeCastPolicy rebuilds the cast policy the coordinator shipped as JSON.
func decodeCastPolicy(ta *pb.TableAssignment) (core.CastPolicy, error) {
	if len(ta.CastPolicy) == 0 {
		return core.CastPolicy{}, nil
	}
	var cast core.CastPolicy
	if err := json.Unmarshal(ta.CastPolicy, &cast); err != nil {
		return core.CastPolicy{}, fmt.Errorf("worker: cast %s: %w", ta.TargetTable, err)
	}
	return cast, nil
}

// decodeMetadata rebuilds the metadata columns the coordinator shipped as
// JSON.
func decodeMetadata(ta *pb.TableAssignment) ([]core.MetadataColumn, error) {
	if len(ta.Metadata) == 0 {
		return nil, nil
	}
	var meta []core.MetadataColumn
	if err := json.Unmarshal(ta.Metadata, &meta); err != nil {
		return nil, fmt.Errorf("worker: metadata %s: %w", ta.TargetTable, err)
	}
	return meta, nil
}
