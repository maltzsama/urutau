package remote

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
)

// droppingServer assigns, then cuts the first sessions the way a killed
// coordinator does; it keeps the session numbered keep open.
type droppingServer struct {
	pb.UnimplementedUrutauControlServer
	keep     int32
	sessions atomic.Int32
	kept     chan struct{}
}

func (s *droppingServer) Session(stream grpc.BidiStreamingServer[pb.WorkerMessage, pb.CoordinatorMessage]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	n := s.sessions.Add(1)
	assign := &pb.Assignment{Epoch: uint64(n), SourceKind: "mysql"}
	if err := stream.Send(&pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: assign}}); err != nil {
		return err
	}
	if n < s.keep {
		return status.Error(codes.Unavailable, "coordinator killed")
	}
	if n == s.keep {
		close(s.kept)
	}
	<-stream.Context().Done()
	return nil
}

// A worker that exits when it loses its coordinator is restarted by the
// kubelet, and every exit lengthens the restart back-off: after a handful of
// coordinator restarts and network faults the Pod sits in CrashLoopBackOff
// for five minutes, the restarted coordinator waits for it, and every table
// of the pipeline stops (#841). The worker process must outlive the loss and
// attach again by itself.
func TestRunAttachesAgainAfterLosingTheCoordinator(t *testing.T) {
	cleanupSinkMu.Lock()
	cleanupSinkCur = &cleanupSink{}
	cleanupSinkMu.Unlock()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &droppingServer{keep: 3, kept: make(chan struct{})}
	srv := grpc.NewServer()
	pb.RegisterUrutauControlServer(srv, server)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, RemoteConfig{
			Coordinator: l.Addr().String(),
			Name:        "w-0",
			Sink:        sink.Config{Type: "remote-cleanup-test"},
			Logger:      slog.New(slog.DiscardHandler),
		})
	}()

	select {
	case <-server.kept:
	case err := <-done:
		t.Fatalf("the worker stopped after %d session(s) instead of attaching again: %v", server.sessions.Load(), err)
	case <-time.After(60 * time.Second):
		t.Fatalf("the worker attached %d time(s) in 60s, want 3", server.sessions.Load())
	}

	// Its own shutdown still ends it.
	cancel()
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("the worker did not stop when its context was cancelled")
	}
}

// What is not the coordinator's loss still ends the worker: reconnecting
// would repeat it forever and hide it.
func TestReconnectableOnlyOnACleanCoordinatorLoss(t *testing.T) {
	lost := CoordinatorLost(errors.New("worker: channel lost: EOF"))
	stuck := CoordinatorLost(pipelineRunning(errors.New("worker: channel lost: EOF")))
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"coordinator lost", lost, true},
		{"coordinator closed the session", nil, true},
		{"pipeline still running", stuck, false},
		{"worker error", errors.New("worker: table t: commit: boom"), false},
	}
	for _, tc := range cases {
		if got := reconnectable(tc.err); got != tc.want {
			t.Errorf("%s: reconnectable = %v, want %v", tc.name, got, tc.want)
		}
	}
}
