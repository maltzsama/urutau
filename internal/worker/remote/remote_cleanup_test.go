package remote

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
)

// cleanupSink records the cleanup RunRemote must perform when it returns
// (issue #555). writerErrs scripts the per-call Writer outcome by index.
type cleanupSink struct {
	ensureErr  error
	writerErrs []error
	writer     sink.TableWriter
	closed     atomic.Bool
	writerCall atomic.Int32
}

func (s *cleanupSink) EnsureTable(context.Context, core.TableRef, core.Schema, []string, core.CastPolicy, dataplane.WriteMode) error {
	return s.ensureErr
}

func (s *cleanupSink) Writer(context.Context, core.TableRef, core.CastPolicy, []core.MetadataColumn) (sink.TableWriter, error) {
	i := int(s.writerCall.Add(1)) - 1
	if i < len(s.writerErrs) && s.writerErrs[i] != nil {
		return nil, s.writerErrs[i]
	}
	return s.writer, nil
}

func (s *cleanupSink) Position(context.Context, core.TableRef) (string, error) { return "", nil }
func (s *cleanupSink) SetProperties(context.Context, core.TableRef, map[string]string) error {
	return nil
}
func (s *cleanupSink) Properties(context.Context, core.TableRef) (map[string]string, error) {
	return nil, nil
}
func (s *cleanupSink) Close() error { s.closed.Store(true); return nil }

type cleanupWriter struct{ closed atomic.Int32 }

func (w *cleanupWriter) Commit(context.Context, *dataplane.Batch) error { return nil }
func (w *cleanupWriter) Close() error                                   { w.closed.Add(1); return nil }

var (
	cleanupSinkMu  sync.Mutex
	cleanupSinkCur *cleanupSink
)

func init() {
	_ = driver.RegisterSink("remote-cleanup-test", func(context.Context, sink.Config) (sink.Sink, error) {
		cleanupSinkMu.Lock()
		defer cleanupSinkMu.Unlock()
		return cleanupSinkCur, nil
	})
}

// assigningServer answers the handshake with the given tables and keeps the
// Session open until the worker closes it.
type assigningServer struct {
	pb.UnimplementedUrutauControlServer
	tables []*pb.TableAssignment
}

func (s *assigningServer) Session(stream grpc.BidiStreamingServer[pb.WorkerMessage, pb.CoordinatorMessage]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	assign := &pb.Assignment{Tables: s.tables, Epoch: 1, SourceKind: "mysql"}
	if err := stream.Send(&pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: assign}}); err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			return nil
		}
	}
}

func runRemoteTables(t *testing.T, s *cleanupSink, tables []*pb.TableAssignment) error {
	t.Helper()
	cleanupSinkMu.Lock()
	cleanupSinkCur = s
	cleanupSinkMu.Unlock()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterUrutauControlServer(srv, &assigningServer{tables: tables})
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return RunRemote(ctx, RemoteConfig{
		Coordinator: l.Addr().String(),
		Name:        "w-0",
		Sink:        sink.Config{Type: "remote-cleanup-test"},
		Logger:      slog.New(slog.DiscardHandler),
	})
}

func tableAssignment(t *testing.T, target string, staged bool) *pb.TableAssignment {
	t.Helper()
	arrow, err := transport.EncodeTableSchema(core.Schema{
		Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &pb.TableAssignment{
		TargetTable: target,
		PrimaryKey:  []string{"id"},
		SchemaArrow: arrow,
		Staged:      staged,
	}
}

func TestRunRemoteClosesSinkAfterEnsureFailure(t *testing.T) {
	s := &cleanupSink{ensureErr: errors.New("ensure boom")}
	ta := tableAssignment(t, "t1", false)
	ta.CreateIfNotExists = true

	if err := runRemoteTables(t, s, []*pb.TableAssignment{ta}); err == nil {
		t.Fatal("RunRemote should fail at EnsureTable")
	}
	if !s.closed.Load() {
		t.Fatal("sink was not closed after an EnsureTable failure")
	}
}

func TestRunRemoteClosesWriterAndSinkAfterWriterFailure(t *testing.T) {
	w := &cleanupWriter{}
	s := &cleanupSink{writer: w, writerErrs: []error{nil, errors.New("writer boom")}}

	runErr := runRemoteTables(t, s, []*pb.TableAssignment{
		tableAssignment(t, "t1", false),
		tableAssignment(t, "t2", false),
	})
	if runErr == nil {
		t.Fatal("RunRemote should fail at the second Writer")
	}
	if w.closed.Load() == 0 {
		t.Fatal("the writer created before the failure was not closed")
	}
	if !s.closed.Load() {
		t.Fatal("sink was not closed after a Writer failure")
	}
}

func TestRunRemoteClosesWriterAndSinkAfterSetStagedFailure(t *testing.T) {
	w := &cleanupWriter{}
	s := &cleanupSink{writer: w}

	// cleanupWriter is not a StagingWriter, so SetStaged refuses it.
	if err := runRemoteTables(t, s, []*pb.TableAssignment{tableAssignment(t, "t1", true)}); err == nil {
		t.Fatal("RunRemote should fail at SetStaged")
	}
	if w.closed.Load() == 0 {
		t.Fatal("the writer of the failed staged table was not closed")
	}
	if !s.closed.Load() {
		t.Fatal("sink was not closed after a SetStaged failure")
	}
}
