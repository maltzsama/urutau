package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// orderStream is a worker Session that says Hello, then stays connected
// until the test ends it, recording what the coordinator sends.
type orderStream struct {
	ctx    context.Context
	hello  *pb.WorkerMessage
	mu     sync.Mutex
	sent   []*pb.CoordinatorMessage
	gotOne chan struct{}
}

func (s *orderStream) Recv() (*pb.WorkerMessage, error) {
	if m := s.hello; m != nil {
		s.hello = nil
		return m, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *orderStream) Send(m *pb.CoordinatorMessage) error {
	s.mu.Lock()
	s.sent = append(s.sent, m)
	if len(s.sent) == 2 {
		close(s.gotOne)
	}
	s.mu.Unlock()
	return nil
}

func (s *orderStream) Context() context.Context     { return s.ctx }
func (s *orderStream) SetHeader(metadata.MD) error  { return nil }
func (s *orderStream) SendHeader(metadata.MD) error { return nil }
func (s *orderStream) SetTrailer(metadata.MD)       {}
func (s *orderStream) SendMsg(any) error            { return nil }
func (s *orderStream) RecvMsg(any) error            { return nil }

// A snapshot waiting for a lost worker sends its chunk requests the moment
// the worker is attached again. The Assignment must still reach the worker
// first: a worker whose first message is a chunk request fails its handshake
// (chaos-1M-521691a: "expected Assignment, got none", and the retries were
// refused as "already connected" for 3 minutes).
func TestTheAssignmentIsTheFirstMessageOfASession(t *testing.T) {
	c, w := coordHarness()
	w.attached = false
	c.ready = make(chan struct{}) // unbuffered: the test runs at the attach

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &orderStream{
		ctx:    ctx,
		hello:  &pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: &pb.Hello{WorkerName: w.name}}},
		gotOne: make(chan struct{}),
	}
	done := make(chan error, 1)
	go func() { done <- (&controlServer{c: c}).Session(stream) }()

	// The snapshot sees the worker attached and requests a chunk, while the
	// session is held at its ready signal (unbuffered, not yet received).
	var out chan *pb.CoordinatorMessage
	for deadline := time.Now().Add(5 * time.Second); out == nil; {
		c.mu.Lock()
		if w.attached {
			out = w.out
		}
		c.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("the worker never attached")
		}
		time.Sleep(time.Millisecond)
	}
	out <- &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Chunk{Chunk: &pb.ChunkRequest{}}}
	select {
	case <-c.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the session never signaled ready")
	}

	select {
	case <-stream.gotOne:
	case err := <-done:
		t.Fatalf("session ended: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the session sent fewer than two messages")
	}
	stream.mu.Lock()
	first := stream.sent[0]
	stream.mu.Unlock()
	if first.GetAssign() == nil {
		t.Fatalf("first message = %T, want the Assignment", first.Msg)
	}
}
