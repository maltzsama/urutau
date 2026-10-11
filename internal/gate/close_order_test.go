package gate

import (
	"context"
	"sync"
	"testing"
	"time"
)

// slowCloseSink delivers a Close only once it is let through, and records the
// order in which items reached the consumer.
type slowCloseSink struct {
	entered chan struct{}
	proceed chan struct{}

	mu        sync.Mutex
	delivered []int
}

func (s *slowCloseSink) Flush(context.Context, Window, uint64, []int) error { return nil }

func (s *slowCloseSink) Close(_ context.Context, _ Window, items []int) error {
	select {
	case s.entered <- struct{}{}:
		<-s.proceed
	default:
	}
	s.deliver(items...)
	return nil
}

func (s *slowCloseSink) Release([]int) {}

func (s *slowCloseSink) deliver(items ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delivered = append(s.delivered, items...)
}

// TestCloseDeliversItsBacklogBeforeLaterItems: a window's trailing items are
// older than anything the producer reads afterwards, so no later item may
// reach the consumer while Close is still delivering them. The window used to
// read as closed from the moment Close took the backlog, and a producer that
// asked in between routed its item live, ahead of the backlog: an update
// landed after the delete that followed it and the row came back (#818).
func TestCloseDeliversItsBacklogBeforeLaterItems(t *testing.T) {
	sink := &slowCloseSink{entered: make(chan struct{}, 1), proceed: make(chan struct{})}
	g := New[int](16, 0, nil, sink)
	ctx := context.Background()

	g.Open("events", 0)
	for _, item := range []int{1, 2} {
		if held, err := g.Hold(ctx, "events", item); !held || err != nil {
			t.Fatalf("Hold(%d) = (%v, %v), want (true, nil)", item, held, err)
		}
	}

	closed := make(chan error, 1)
	go func() { closed <- g.Close(ctx, "events", 0) }()
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Close never delivered its backlog")
	}

	// The producer reads item 3 while the backlog is still on its way.
	held, err := g.Hold(ctx, "events", 3)
	if err != nil {
		t.Fatalf("Hold(3): %v", err)
	}
	if !held {
		sink.deliver(3) // what the producer does with an item the gate did not take
	}
	close(sink.proceed)
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}

	sink.mu.Lock()
	got := append([]int(nil), sink.delivered...)
	sink.mu.Unlock()
	want := []int{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered %v, want %v: a later item overtook the window's backlog", got, want)
		}
	}
	if g.IsOpen("events", 0) {
		t.Fatal("the window is still open after Close")
	}
	if held, _ := g.Hold(ctx, "events", 4); held {
		t.Fatal("a sealed window held an item")
	}
}
