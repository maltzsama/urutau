package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/grpctls"
	"github.com/maltzsama/urutau/internal/logging"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/internal/worker"
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
	MaxBytes    int64
	MaxInterval time.Duration
	Logger      *slog.Logger
	LogBuffer   *logging.Buffer
	// MetricsAddr serves /metrics (Prometheus); empty disables it.
	MetricsAddr string

	// TLS is the mutual-TLS material matching the coordinator's control
	// plane. Empty means plaintext.
	TLS grpctls.Config

	// FaultAckGate (test-only): commits normally but withholds the ack
	// while the gate is set. A test flips it AFTER the snapshot, so the
	// worker goes stale while owing nothing (in-flight == 0) — the
	// supervisor's safe-reset path. CD-2: a stale worker WITH in-flight
	// batches terminates for replay instead of resetting, so stopping the
	// acks from the start would only prove the terminate path. Nil
	// disables it.
	FaultAckGate *atomic.Bool
}

// RunRemote connects to the coordinator, applies its assignment, and pulls
// change batches over Arrow Flight until the stream ends. All commits go
// through the same collapsed worker core (batcher, windows, collapse).
func RunRemote(ctx context.Context, cfg RemoteConfig) error {
	normalizeRemoteConfig(&cfg)

	// One ClientConn, one parent context, three coupled streams: Session,
	// Control and Flight all die together — the split-brain correction of
	// design §5.3. dialOpts carries the keepalive that turns a silently
	// frozen coordinator into an error in ~15s.
	conn, err := dialCoordinator(cfg.Coordinator, cfg.TLS)
	if err != nil {
		return fmt.Errorf("worker: dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	sessCtx, cancelAll := context.WithCancelCause(ctx)
	defer cancelAll(nil)

	session, assign, err := workerHandshake(sessCtx, conn, cfg.Name, cfg.Logger)
	if err != nil {
		return err
	}
	sender := &sessionSender{s: session}
	defer startWorkerLogForwarder(cfg.LogBuffer, sender, assign.Epoch)()
	cfg.Logger.Info("assignment", "tables", len(assign.Tables), "run", assign.RunId)

	// Catalog + writers from the assignment: the coordinator owns DDL and
	// introspection, but the writes are this worker's.
	snk, err := openWorkerSink(ctx, cfg, assign)
	if err != nil {
		return err
	}
	// The worker owns the sink and its per-table writers; both must be
	// released when RunRemote returns, success or failure (issue #555).
	defer func() { _ = snk.Close() }()

	w, pkByTable, cleanup, err := applyAssignment(ctx, cfg, snk, sender, assign)
	if err != nil {
		return err
	}
	// The writers close first (registered later, deferred LIFO), then the sink.
	defer cleanup()

	committed, err := reportReady(ctx, cfg, snk, sender, assign)
	if err != nil {
		return err
	}

	return serve(ctx, sessCtx, cancelAll, cfg, conn, session, sender, w, assign, pkByTable, committed)
}

// openWorkerSink opens the sink the assignment's writes go through.
func openWorkerSink(ctx context.Context, cfg RemoteConfig, assign *pb.Assignment) (sink.Sink, error) {
	sc := workerSinkConfig(cfg, assign.GetSink())
	// The sink parses the opaque position strings it stores with this kind
	// (position.Parse): an empty kind defaults to MySQL GTID, so a Postgres
	// LSN or Kafka offset read back on restart would be parsed as a GTID set
	// and resume from the wrong point.
	sc.SourceKind = assign.SourceKind
	snk, err := driver.OpenSinkConfig(ctx, sc)
	if err != nil {
		return nil, fmt.Errorf("worker: sink: %w", err)
	}
	return snk, nil
}

// workerSinkConfig merges the worker's own sink access (URI and credentials
// from its environment) with the sink settings the coordinator assigned from
// the pipeline spec. The spec wins for the type, the namespace and every
// option it sets; a nil assignment (an older coordinator) leaves the worker's
// own config untouched.
func workerSinkConfig(cfg RemoteConfig, assigned *pb.SinkAssignment) sink.Config {
	sc := sink.Config{
		Type:      cfg.Sink.Type,
		URI:       cfg.Sink.URI,
		Namespace: cfg.Namespace,
		Options:   make(map[string]string, len(cfg.Sink.Options)+len(assigned.GetOptions())),
	}
	for k, v := range cfg.Sink.Options {
		sc.Options[k] = v
	}
	if t := assigned.GetType(); t != "" {
		sc.Type = t
	}
	if ns := assigned.GetNamespace(); ns != "" {
		sc.Namespace = ns
	}
	for k, v := range assigned.GetOptions() {
		// Credentials only ever come from the worker's own environment.
		if v == "" || k == driver.OptClientID || k == driver.OptClientSecret {
			continue
		}
		sc.Options[k] = v
	}
	return sc
}

// applyAssignment builds the worker from the assignment: it ensures each
// target table, opens its writer, extends the assigned schema with the
// reference-join columns, starts the enrich stages, and installs the staged
// delivery and schema-drift callbacks. It returns the worker, the per-table
// primary keys, and a cleanup that stops the stages and closes the writers.
func applyAssignment(ctx context.Context, cfg RemoteConfig, snk sink.Sink, sender *sessionSender, assign *pb.Assignment) (*worker.Worker, map[string][]string, func(), error) {
	w := worker.New(worker.Config{MaxRows: cfg.MaxRows, MaxBytes: cfg.MaxBytes, MaxInterval: cfg.MaxInterval, MetricsAddr: cfg.MetricsAddr})
	// Installed BEFORE the assignment loop: SetStaged below refuses a staged
	// table whose delivery callback is unset, so installing this afterwards
	// failed every partitioned run on a staging sink at boot. It depends
	// only on sender and assign, both already bound above.
	// Staged deliveries (WK-001 C5): the data files are written but not
	// committed; the coordinator groups them by cycle and commits. seq 0 is
	// a worker-generated snapshot/window batch, committed on arrival. A send
	// failure must surface — a dropped descriptor is an uncommittable cycle.
	w.OnStaged(func(table string, seq uint64, desc []byte, pos, state string, pending []uint32) error {
		return sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Staged{Staged: &pb.StagedBatch{
			Table:           table,
			Seq:             seq,
			Descriptor_:     desc,
			Position:        pos,
			SnapshotState:   state,
			SnapshotPending: pending,
			Epoch:           assign.Epoch,
		}}})
	})
	var stages []*enrich.Stage
	var writers []sink.TableWriter
	// Registered BEFORE the loop: an EnsureTable/Writer/SetStaged failure
	// mid-way must still stop the stages built for earlier tables and close
	// their writers, or the boot leaks them (issue #555).
	cleanup := func() {
		for _, st := range stages {
			st.Stop()
		}
		for _, wr := range writers {
			_ = wr.Close()
		}
	}
	pkByTable := make(map[string][]string, len(assign.Tables))
	for _, ta := range assign.Tables {
		if err := applyTable(ctx, cfg, snk, w, ta, &stages, &writers, pkByTable); err != nil {
			cleanup()
			return nil, nil, nil, err
		}
	}
	w.OnSchemaDrift(func(d worker.SchemaDrift) {
		cfg.Logger.Error("schema drift: pipeline paused", "table", d.Table, "column", d.Column,
			"action", "coordinator must assign a schema with the column; declare it in the spec")
		// Tell the coordinator WHY the worker is about to die, so it surfaces
		// the reason instead of a bare CrashLoopBackOff (issue #272).
		_ = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_SchemaDrift{SchemaDrift: &pb.SchemaDrift{
			Table: d.Table, Column: d.Column, Kind: d.Kind,
		}}})
	})
	return w, pkByTable, cleanup, nil
}

// applyTable wires one target table: introspect the assigned schema, build the
// enrich stage, ensure the table, open the writer, register the pipeline, and
// install the per-table drop-delete/staged/enrich options.
func applyTable(ctx context.Context, cfg RemoteConfig, snk sink.Sink, w *worker.Worker, ta *pb.TableAssignment, stages *[]*enrich.Stage, writers *[]sink.TableWriter, pkByTable map[string][]string) error {
	// The assignment schema arrives as Arrow IPC derived from the
	// coordinator's canonical schema; the sink maps it to its own storage
	// schema.
	cs, err := transport.DecodeTableSchema(ta.SchemaArrow)
	if err != nil {
		return fmt.Errorf("worker: schema %s: %w", ta.TargetTable, err)
	}
	cs.PrimaryKey = ta.PrimaryKey
	// The SOURCE schema (before enrich extends it), for the drift check and
	// the columnar collapse. The remote path built the pipeline without it,
	// so a distributed upsert snapshot collapsed every row into one — an
	// empty PK groups them all (WK-001 e2e). Mirrors the runner's
	// SetKnownSchema.
	knownSchema := cs
	// Owner is the worker group name (cfg.Name), stable across restarts and
	// rollouts. Sinks that persist a durable position per partition
	// (ClickHouse, Couchbase) use it to keep one position per partition
	// instead of a single last-writer scalar (WK-001 §2.6/C7).
	ref := core.TableRef{Target: ta.TargetTable, PrimaryKey: ta.PrimaryKey, Owner: cfg.Name}
	// The write shape arrives with the assignment: the coordinator's DDL and
	// this worker's writes must agree on the cast policy, the metadata
	// columns, and the write mode — a hardcoded UPSERT or empty cast here
	// silently diverged append tables and cast overrides (audit #8/#10).
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
	// destination columns join the assigned schema BEFORE EnsureTable: the
	// sink table must have the column, or the first enriched batch's values
	// are silently dropped (every sink projects by the table's own columns).
	// Event columns are captured before the extension — they are not event
	// columns.
	//
	// The stage is built and, for any wildcard reference, loaded
	// SYNCHRONOUSLY here — before AddColumns/EnsureTable — so the real
	// wildcard columns are known in time to extend cs (#56). An
	// explicit-select reference is unaffected: LoadWildcards skips it, and
	// Start (below) still loads it asynchronously exactly as before.
	var st *enrich.Stage
	if len(ta.Enrich) > 0 {
		enrichCfgs := enrichSpecs(ta.Enrich)
		// SOURCE view, captured BEFORE the reference-column extension — see
		// FT-1: the destinations are not event columns.
		eventSchema := cs
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
	*writers = append(*writers, writer)
	w.Register(ta.TargetTable, writer, mode)
	if len(knownSchema.Columns) > 0 {
		w.SetKnownSchema(ta.TargetTable, knownSchema)
	}
	if ta.Staged {
		if err := w.SetStaged(ta.TargetTable); err != nil {
			return err
		}
	}
	// onDelete: skip — an append-only table drops deletes (issue #264).
	if ta.OnDelete == string(spec.OnDeleteSkip) {
		w.SetDropDeletes(ta.TargetTable, true)
	}
	pkByTable[ta.TargetTable] = ta.PrimaryKey
	// Start's remaining first loads (any explicit-select reference, plus the
	// refresh ticker for everything) stay asynchronous — the cold-start
	// policy applies to whatever hasn't loaded yet.
	if st != nil {
		st.Start(ctx)
		*stages = append(*stages, st)
		w.SetEnricher(ta.TargetTable, st)
	}
	return nil
}

// reportReady reads each table's committed position, decides the initial
// phase, and sends the ready Hello to the coordinator. The committed map also
// drives the local skip of batches the table already covers — the resume
// idempotence boundary (failure-analysis case 4).
func reportReady(ctx context.Context, cfg RemoteConfig, snk sink.Sink, sender *sessionSender, assign *pb.Assignment) (map[string]position.Position, error) {
	// Report phase + committed positions (design §5.6.1): STREAMING if any
	// of our tables has a commit, SNAPSHOTTING otherwise.
	parsePos := parsePosition(assign.SourceKind)
	committed := make(map[string]position.Position, len(assign.Tables))
	phase := pb.WorkerPhase_WORKER_PHASE_SNAPSHOTTING
	for _, ta := range assign.Tables {
		pos, err := snk.Position(ctx, core.TableRef{Target: ta.TargetTable, Owner: cfg.Name})
		if err != nil {
			return nil, fmt.Errorf("worker: committed %s: %w", ta.TargetTable, err)
		}
		if pos == "" {
			continue
		}
		p, err := parsePos(pos)
		if err != nil {
			return nil, fmt.Errorf("worker: committed %s %q: %w", ta.TargetTable, pos, err)
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
		return nil, fmt.Errorf("worker: ready hello: %w", err)
	}
	cfg.Logger.Info("worker ready", "phase", phase.String(), "committed", len(committed))
	return committed, nil
}

// serve runs the ingest pipelines and the three coupled streams until one
// dies, then performs the graceful/abort shutdown.
func serve(ctx context.Context, sessCtx context.Context, cancelAll context.CancelCauseFunc, cfg RemoteConfig,
	conn *grpc.ClientConn, session pb.UrutauControl_SessionClient, sender *sessionSender,
	w *worker.Worker, assign *pb.Assignment, pkByTable map[string][]string, committed map[string]position.Position) error {

	// Pipelines drain on their own ctx: a graceful shutdown signal cancels
	// the streams (sessCtx) but leaves the flush able to commit in-flight
	// rows (design §5.3.2). Only an anomalous channel death aborts it.
	pipeCtx, pipeCancel := context.WithCancel(ctx)
	defer pipeCancel()
	runErr := make(chan error, 1)
	ingest := make(chan worker.Ingest, 1024)
	go func() { runErr <- w.Run(pipeCtx, ingest) }()
	go reportWorkerMetrics(pipeCtx, w, sender, cfg.Logger)

	chunks := newChunkExecutor(assign, w, cfg.Logger, sender.send)
	defer chunks.Close()

	installAcks(w, sender, assign.Epoch, cfg)

	st, err := openStreams(sessCtx, conn, cfg, w, sender, session, assign, pkByTable, committed, ingest)
	if err != nil {
		return err
	}

	// Chunk work is serialized: the coordinator sends one ChunkRequest at a
	// time and waits for ChunkReady, so a single worker slot is enough and
	// ordering between chunks is preserved. The worker exits on the shared
	// context so it cannot outlive the session.
	go func() {
		for {
			select {
			case req := <-st.chunkWork:
				if err := chunks.run(sessCtx, req); err != nil {
					cancelAll(fmt.Errorf("worker: chunk: %w", err))
					return
				}
			case <-sessCtx.Done():
				return
			}
		}
	}()

	surveilStreams(sessCtx, cancelAll, st, cfg.Logger)
	return workerShutdown(context.Cause(sessCtx), pipeCancel, pipeCtx, runErr, ingest, cfg.Logger)
}

// streams holds the three coupled client streams and the chunk work queue.
type streams struct {
	session   pb.UrutauControl_SessionClient
	control   pb.UrutauControl_ControlClient
	fl        flight.FlightService_DoGetClient
	recv      *batchReceiver
	chunkWork chan *pb.ChunkRequest
}

// openStreams opens the Control and Flight streams on the same ClientConn and
// wires the batch receiver around the session's ingest channel.
func openStreams(sessCtx context.Context, conn *grpc.ClientConn, cfg RemoteConfig, w *worker.Worker, sender *sessionSender,
	session pb.UrutauControl_SessionClient, assign *pb.Assignment, pkByTable map[string][]string,
	committed map[string]position.Position, ingest chan<- worker.Ingest) (*streams, error) {

	// Control plane (same ClientConn, urgent signals) — the Hello identifies
	// this stream to the server.
	control, err := pb.NewUrutauControlClient(conn).Control(sessCtx)
	if err != nil {
		return nil, fmt.Errorf("worker: control: %w", err)
	}
	if err := control.Send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: &pb.Hello{
		WorkerName: cfg.Name,
		Epoch:      assign.Epoch,
	}}}); err != nil {
		return nil, fmt.Errorf("worker: control hello: %w", err)
	}

	// Data plane: Arrow Flight over the SAME ClientConn, so a dropped
	// connection tears down every stream at once.
	fl, err := flight.NewFlightServiceClient(conn).DoGet(sessCtx, &flight.Ticket{Ticket: assign.Ticket})
	if err != nil {
		return nil, fmt.Errorf("worker: doget: %w", err)
	}

	recv := &batchReceiver{
		ctx:       sessCtx,
		w:         w,
		ingest:    ingest,
		committed: committed,
		parsePos:  parsePosition(assign.SourceKind),
		pkByTable: pkByTable,
		log:       cfg.Logger,
		ack: func(table, pos string) {
			_ = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Ack{Ack: &pb.Ack{
				Table:    table,
				Epoch:    assign.Epoch,
				Position: pos,
			}}})
		},
	}
	return &streams{
		session:   session,
		control:   control,
		fl:        fl,
		recv:      recv,
		chunkWork: make(chan *pb.ChunkRequest, 4),
	}, nil
}

// surveilStreams is the surveillance by READING each stream (design §5.3.1):
// the first one to die cancels the shared context, which takes the other two
// with it. It returns once the context is done and every loop has exited.
func surveilStreams(sessCtx context.Context, cancelAll context.CancelCauseFunc, st *streams, log *slog.Logger) {
	loops := []struct {
		name string
		step func() error
	}{
		{"session", func() error {
			m, err := st.session.Recv()
			if err != nil {
				return err
			}
			if cr := m.GetChunk(); cr != nil {
				// Hand the request to the chunk worker, aborting when the
				// session ends — a bare send with a full buffer would wedge
				// this loop and stall teardown.
				select {
				case st.chunkWork <- cr:
				case <-sessCtx.Done():
					return errShutdown
				}
			}
			return nil
		}},
		{"control", func() error {
			m, err := st.control.Recv()
			if err != nil {
				return err
			}
			if m.GetShutdown() != nil {
				return errShutdown
			}
			return nil
		}},
		{"flight", func() error {
			fd, err := st.fl.Recv()
			if err != nil {
				return err
			}
			return st.recv.apply(fd)
		}},
	}
	var loopWG sync.WaitGroup
	for _, l := range loops {
		loopWG.Add(1)
		go func(name string, step func() error) {
			defer loopWG.Done()
			for {
				if err := step(); err != nil {
					if errors.Is(err, io.EOF) {
						cancelAll(fmt.Errorf("%w", errGracefulEOF))
					} else if errors.Is(err, errShutdown) {
						cancelAll(errShutdown)
					} else {
						cancelAll(fmt.Errorf("worker: stream %s: %w", name, err))
					}
					return
				}
			}
		}(l.name, l.step)
	}

	<-sessCtx.Done()
	// Every read loop has seen the cancellation (or will within a Recv
	// round-trip); wait for them so no loop can still send into ingest when
	// workerShutdown closes it.
	loopWG.Wait()
}
