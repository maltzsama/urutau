package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"google.golang.org/grpc"
)

// fakeSession records the messages a reporter sends. It stands in for the
// gRPC client stream; only Send (and Recv's signature) are ever touched.
type fakeSession struct {
	grpc.ClientStream
	mu   sync.Mutex
	msgs []*pb.WorkerMessage
	ch   chan *pb.WorkerMessage
}

func (f *fakeSession) Send(m *pb.WorkerMessage) error {
	f.mu.Lock()
	f.msgs = append(f.msgs, m)
	f.mu.Unlock()
	select {
	case f.ch <- m:
	default:
	}
	return nil
}

func (f *fakeSession) Recv() (*pb.CoordinatorMessage, error) { return nil, nil }

// The dashboard's worker series must not wait a full metrics interval after a
// session starts: the first report goes out immediately, then the ticker takes
// over.
func TestReportWorkerMetricsFirstReportIsImmediate(t *testing.T) {
	w := New(Config{})
	sess := &fakeSession{ch: make(chan *pb.WorkerMessage, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		reportWorkerMetrics(ctx, w, &sessionSender{s: sess}, slog.Default())
	}()

	select {
	case m := <-sess.ch:
		if m.GetWorkerMetrics() == nil {
			t.Fatalf("first message = %v, want a WorkerMetrics report", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no report within 2s — the first report waits for the 5s ticker")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the reporter did not stop when its context was cancelled")
	}
}

// A send failure ends the reporter: the session it reported on is gone.
func TestReportWorkerMetricsSendErrorStops(t *testing.T) {
	w := New(Config{})
	sess := &failingSession{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		reportWorkerMetrics(ctx, w, &sessionSender{s: sess}, slog.Default())
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the reporter kept running after a failed send")
	}
	if sess.count() != 1 {
		t.Fatalf("attempts = %d, want 1 (the first send, then stop)", sess.count())
	}
}

// failingSession fails every send.
type failingSession struct {
	grpc.ClientStream
	mu sync.Mutex
	n  int
}

func (f *failingSession) Send(*pb.WorkerMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	return errors.New("session gone")
}

func (f *failingSession) Recv() (*pb.CoordinatorMessage, error) { return nil, nil }

func (f *failingSession) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}
