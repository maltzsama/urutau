package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/maltzsama/urutau/internal/change"
	foziceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// RemoteConfig wires one distributed worker: where the coordinator lives,
// which sink this worker owns, and how it batches.
type RemoteConfig struct {
	Coordinator string            // host:port of the coordinator
	Name        string            // worker name (Hello)
	Sink        foziceberg.Config // catalog access — workers own their writes
	Namespace   string            // fallback namespace for bare targets
	MaxRows     int
	MaxInterval time.Duration
	Logger      *slog.Logger
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

	conn, err := grpc.NewClient(cfg.Coordinator, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("worker: dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	session, err := sessionWithRetry(ctx, conn, cfg.Logger)
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
	cat, err := foziceberg.NewCatalog(ctx, cfg.Sink)
	if err != nil {
		return fmt.Errorf("worker: catalog: %w", err)
	}
	w := New(Config{MaxRows: cfg.MaxRows, MaxInterval: cfg.MaxInterval})
	for _, ta := range assign.Tables {
		schema := &iceberg.Schema{}
		if err := json.Unmarshal([]byte(ta.SchemaJson), schema); err != nil {
			return fmt.Errorf("worker: schema %s: %w", ta.TargetTable, err)
		}
		ident := targetIdent(cfg.Namespace, ta.TargetTable)
		if ta.CreateIfNotExists {
			if err := foziceberg.EnsureTable(ctx, cat, ident, schema); err != nil {
				return fmt.Errorf("worker: ensure %s: %w", ta.TargetTable, err)
			}
		}
		writer, err := foziceberg.NewTableWriter(ctx, cat, ident, ta.PrimaryKey)
		if err != nil {
			return fmt.Errorf("worker: writer %s: %w", ta.TargetTable, err)
		}
		w.Register(ta.TargetTable, writer)
	}

	runErr := make(chan error, 1)
	ingest := make(chan change.Change, 1024)
	go func() { runErr <- w.Run(ctx, ingest) }()

	w.OnCommit(func(b change.Batch, rows int) {
		_ = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Ack{Ack: &pb.Ack{
			Table:    b.Table,
			Epoch:    assign.Epoch,
			Position: b.Position,
			Rows:     uint64(rows),
		}}})
	})

	// Flight data plane: pull batches until the coordinator closes the
	// stream (snapshot done, shutdown, or the world ending).
	flightConn, err := flight.NewClientWithMiddleware(cfg.Coordinator, nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("worker: flight dial: %w", err)
	}
	defer func() { _ = flightConn.Close() }()

	dataStream, err := flightConn.DoGet(ctx, &flight.Ticket{Ticket: assign.Ticket})
	if err != nil {
		return fmt.Errorf("worker: doget: %w", err)
	}

	for {
		fd, err := dataStream.Recv()
		// gRPC wraps ctx cancellation in a status error, so match the code
		// too: a shutdown is not a failure.
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) ||
			status.Code(err) == codes.Canceled {
			break
		}
		if err != nil {
			return fmt.Errorf("worker: flight recv: %w", err)
		}
		if err := applyFlightData(w, ingest, fd); err != nil {
			return err
		}
	}

	// Stream over: close ingest so the pipelines flush their remainder.
	close(ingest)
	return <-runErr
}

// applyFlightData routes one decoded batch: snapshot rows build windows,
// closes markers release them, live rows feed ingest.
func applyFlightData(w *Worker, ingest chan<- change.Change, fd *flight.FlightData) error {
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

	rows, meta, err := transport.DecodeBatch(rec, fd.AppMetadata)
	if err != nil {
		return err
	}
	switch {
	case meta.Window != nil && meta.Window.Snapshot:
		if err := w.AddWindowRows(meta.Table, meta.Window.ChunkId, rows); err != nil {
			return err
		}
	case meta.Window != nil && meta.Window.Closes:
		ingest <- change.Change{
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
			ingest <- rows[i]
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
