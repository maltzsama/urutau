package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/maltzsama/urutau/internal/grpctls"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

type assignOnHello struct {
	pb.UnimplementedUrutauControlServer
}

func (assignOnHello) Session(s grpc.BidiStreamingServer[pb.WorkerMessage, pb.CoordinatorMessage]) error {
	if _, err := s.Recv(); err != nil {
		return err
	}
	if err := s.Send(&pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: &pb.Assignment{}}}); err != nil {
		return err
	}
	_, err := s.Recv()
	if err == io.EOF {
		return nil
	}
	return err
}

// A coordinator down for longer than a minute (a crash, a chaos kill, a
// rollout) must be found within seconds of coming back. The coordinator waits
// only a bounded time for its workers, so a worker whose ClientConn sits in
// gRPC's exponential backoff (up to 120s for both the DNS resolver and the
// subchannel) misses every restart, and the pipeline restarts in a loop with
// no progress — chaos run d582cf2, pr_items stuck at 1-15733 for 9 minutes.
func TestWorkerFindsRestartedCoordinatorPromptly(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a 45s coordinator outage")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close() // the coordinator is down: every dial is refused

	conn, err := dialCoordinator(addr, grpctls.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	type result struct {
		at  time.Time
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, _, err := workerHandshake(ctx, conn, "w-0", slog.New(slog.NewTextHandler(io.Discard, nil)))
		done <- result{time.Now(), err}
	}()

	time.Sleep(45 * time.Second)
	l, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterUrutauControlServer(srv, assignOnHello{})
	go func() { _ = srv.Serve(l) }()
	defer srv.Stop()
	up := time.Now()

	r := <-done
	if r.err != nil {
		t.Fatalf("handshake: %v", r.err)
	}
	if lag := r.at.Sub(up); lag > 10*time.Second {
		t.Fatalf("worker found the restarted coordinator %s after it came back; want ≤ 10s", lag.Round(time.Second))
	}
}

// firstSessionMisfires answers the first Session with a message other than
// the Assignment and, like the coordinator, refuses a second Session while
// the first is still open.
type firstSessionMisfires struct {
	pb.UnimplementedUrutauControlServer
	mu    sync.Mutex
	calls int
	open  bool
}

func (f *firstSessionMisfires) Session(s grpc.BidiStreamingServer[pb.WorkerMessage, pb.CoordinatorMessage]) error {
	if _, err := s.Recv(); err != nil {
		return err
	}
	f.mu.Lock()
	if f.open {
		f.mu.Unlock()
		return errors.New("already connected")
	}
	f.calls++
	first := f.calls == 1
	f.open = true
	f.mu.Unlock()
	defer func() { f.mu.Lock(); f.open = false; f.mu.Unlock() }()
	msg := &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Assign{Assign: &pb.Assignment{}}}
	if first {
		msg = &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Chunk{Chunk: &pb.ChunkRequest{}}}
	}
	if err := s.Send(msg); err != nil {
		return err
	}
	for {
		if _, err := s.Recv(); err != nil {
			return nil
		}
	}
}

// A handshake attempt that fails must close its Session. Left open, the
// coordinator kept the worker attached to it and refused every retry as
// "already connected" until the worker exited (chaos-1M-521691a: 3 minutes).
func TestAFailedHandshakeClosesItsSession(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterUrutauControlServer(srv, &firstSessionMisfires{})
	go func() { _ = srv.Serve(l) }()
	defer srv.Stop()

	conn, err := dialCoordinator(l.Addr().String(), grpctls.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, assign, err := workerHandshake(ctx, conn, "w-0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || assign == nil {
		t.Fatalf("handshake = %v, want the retry to get the Assignment once the failed session is closed", err)
	}
}
