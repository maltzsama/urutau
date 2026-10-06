package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// fakeControlStream is a Control server stream for the reconnect test: Recv
// yields the Hello once, Context blocks until cancelled, Send records.
type fakeControlStream struct {
	pb.UrutauControl_ControlServer
	ctx   context.Context
	hello *pb.Hello
	mu    sync.Mutex
	sent  []*pb.ControlMessage
}

func (f *fakeControlStream) Recv() (*pb.WorkerMessage, error) {
	return &pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: f.hello}}, nil
}

func (f *fakeControlStream) Context() context.Context { return f.ctx }

func (f *fakeControlStream) Send(m *pb.ControlMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeControlStream) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func waitControlStream(t *testing.T, c *Coordinator, w *workerState, want pb.UrutauControl_ControlServer) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		got := w.control
		c.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the Control stream was never installed")
		}
		time.Sleep(time.Millisecond)
	}
}

// A worker that reconnects its Control before the server notices the old
// stream died must keep the new stream: the old handler's defer must not nil
// it, or gracefulShutdown skips the worker (issue #552).
func TestControlReconnectSurvivesTheStaleDefer(t *testing.T) {
	c, w := coordHarness()
	srv := &controlServer{c: c}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	s1 := &fakeControlStream{ctx: ctx1, hello: &pb.Hello{WorkerName: w.name}}
	done1 := make(chan error, 1)
	go func() { done1 <- srv.Control(s1) }()
	waitControlStream(t, c, w, s1)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	s2 := &fakeControlStream{ctx: ctx2, hello: &pb.Hello{WorkerName: w.name}}
	done2 := make(chan error, 1)
	go func() { done2 <- srv.Control(s2) }()
	waitControlStream(t, c, w, s2)

	// The first stream's server-side close lands AFTER the reconnect: its
	// defer must leave the new stream in place.
	cancel1()
	<-done1
	c.mu.Lock()
	got := w.control
	c.mu.Unlock()
	if got != pb.UrutauControl_ControlServer(s2) {
		t.Fatalf("w.control after the stale stream ended = %v, want the reconnected stream", got)
	}

	// gracefulShutdown still reaches the reconnected worker.
	c.gracefulShutdown()
	if n := s2.sentCount(); n != 1 {
		t.Fatalf("shutdown messages to the reconnected stream = %d, want 1", n)
	}

	cancel2()
	<-done2
}
