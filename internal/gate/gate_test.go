package gate

import (
	"context"
	"sync"
	"testing"
	"time"
)

// recSink records every drain for the tests.
type recSink struct {
	mu      sync.Mutex
	flushes []drain
	closes  []drain
	release int
}

type drain struct {
	w        Window
	windowID uint64
	items    []int
}

func (s *recSink) Flush(_ context.Context, w Window, windowID uint64, items []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes = append(s.flushes, drain{w, windowID, append([]int(nil), items...)})
	return nil
}

func (s *recSink) Close(_ context.Context, w Window, items []int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes = append(s.closes, drain{w, 0, append([]int(nil), items...)})
	return nil
}

func (s *recSink) Release(items []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.release += len(items)
}

func (s *recSink) flushedItems() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, f := range s.flushes {
		n += len(f.items)
	}
	return n
}

func newTestGate(maxCount int) (*Gate[int], *recSink) {
	s := &recSink{}
	return New[int](maxCount, 0, nil, s), s
}

// TestOpenHoldFlushClose is the basic lifecycle: a closed gate never holds, an
// open one holds, Flush drains but keeps it open, Close seals it.
func TestOpenHoldFlushClose(t *testing.T) {
	g, sink := newTestGate(16)
	ctx := context.Background()

	if held, err := g.Hold(ctx, "orders", 1); held || err != nil {
		t.Fatalf("Hold on a closed gate = (%v, %v), want (false, nil)", held, err)
	}
	g.Open("orders", 0)
	if held, err := g.Hold(ctx, "orders", 1); !held || err != nil {
		t.Fatalf("Hold on an open gate = (%v, %v), want (true, nil)", held, err)
	}
	if g.Len("orders", 0) != 1 {
		t.Fatalf("Len = %d, want 1", g.Len("orders", 0))
	}

	if err := g.Flush(ctx, "orders", 0, 7); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !g.IsOpen("orders", 0) {
		t.Fatal("Flush must keep the window open")
	}
	if g.Len("orders", 0) != 0 {
		t.Fatalf("Len after Flush = %d, want 0", g.Len("orders", 0))
	}
	if got := sink.flushes; len(got) != 1 || got[0].windowID != 7 || len(got[0].items) != 1 {
		t.Fatalf("flush = %+v, want one batch of 1 for window 7", got)
	}

	if held, err := g.Hold(ctx, "orders", 2); !held || err != nil {
		t.Fatalf("Hold after Flush = (%v, %v), want (true, nil)", held, err)
	}
	if err := g.Close(ctx, "orders", 0); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if g.IsOpen("orders", 0) {
		t.Fatal("Close must seal the window")
	}
	if got := sink.closes; len(got) != 1 || got[0].w.Target != "orders" || len(got[0].items) != 1 {
		t.Fatalf("close = %+v, want one untagged batch of 1 for orders", got)
	}
	if held, _ := g.Hold(ctx, "orders", 3); held {
		t.Fatal("a sealed gate must not hold")
	}
}

// TestBackpressureBlocksThenDrains: a bounded window blocks the producer at
// its bound; a Flush drains it and the producer proceeds.
func TestBackpressureBlocksThenDrains(t *testing.T) {
	g, _ := newTestGate(4)
	ctx := context.Background()
	g.Open("orders", 0)
	for i := 0; i < 4; i++ {
		if held, err := g.Hold(ctx, "orders", i); !held || err != nil {
			t.Fatalf("Hold %d = (%v, %v)", i, held, err)
		}
	}

	done := make(chan bool, 1)
	go func() {
		held, _ := g.Hold(ctx, "orders", 4)
		done <- held
	}()
	select {
	case <-done:
		t.Fatal("the producer passed a full gate before a drain")
	case <-time.After(100 * time.Millisecond):
	}

	if err := g.Flush(ctx, "orders", 0, 1); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	select {
	case held := <-done:
		if !held {
			t.Fatal("the producer was not admitted after the drain")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the drain did not unblock the producer")
	}
	if g.Len("orders", 0) != 1 {
		t.Fatalf("Len = %d, want the one admitted item", g.Len("orders", 0))
	}
}

// TestByteBound: a window blocks at its byte bound however few items it holds,
// and the item that crosses the bound is admitted (one oversized item never
// deadlocks the gate).
func TestByteBound(t *testing.T) {
	s := &recSink{}
	g := New[int](1000, 10, func(i int) int64 { return int64(i) }, s)
	ctx := context.Background()
	g.Open("orders", 0)

	for _, i := range []int{6, 6} { // 6 then 12 bytes: the crossing item is admitted
		if held, err := g.Hold(ctx, "orders", i); !held || err != nil {
			t.Fatalf("Hold %d = (%v, %v)", i, held, err)
		}
	}
	done := make(chan bool, 1)
	go func() {
		held, _ := g.Hold(ctx, "orders", 6)
		done <- held
	}()
	select {
	case <-done:
		t.Fatal("the producer passed a window over its byte bound")
	case <-time.After(100 * time.Millisecond):
	}
	if err := g.Flush(ctx, "orders", 0, 1); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the drain did not unblock the producer")
	}
}

// TestMarkReadyDrainsEarly: once a full window's chunk is ready, Hold drains
// it tagged instead of blocking, and the window stays open.
func TestMarkReadyDrainsEarly(t *testing.T) {
	g, sink := newTestGate(2)
	ctx := context.Background()
	g.Open("orders", 0)
	g.MarkReady("orders", 0, 7)

	for i := 0; i < 5; i++ {
		held, err := g.Hold(ctx, "orders", i)
		if !held || err != nil {
			t.Fatalf("Hold %d = (%v, %v), want (true, nil)", i, held, err)
		}
	}
	if !g.IsOpen("orders", 0) {
		t.Fatal("the early drain must keep the window open")
	}
	if got := sink.flushedItems(); got != 4 {
		t.Fatalf("flushed %d items early, want 4", got)
	}
	for _, f := range sink.flushes {
		if f.windowID != 7 || f.w.Target != "orders" {
			t.Fatalf("early drain = %+v, want window 7 of orders", f)
		}
	}
	if g.Len("orders", 0) != 1 {
		t.Fatalf("Len = %d, want the last item still held", g.Len("orders", 0))
	}
}

// TestClearReadyReinstatesBlocking: a lost consumer's readiness is forgotten,
// so a full window blocks again.
func TestClearReadyReinstatesBlocking(t *testing.T) {
	g, _ := newTestGate(2)
	ctx := context.Background()
	g.Open("orders", 0)
	g.MarkReady("orders", 0, 7)
	g.ClearReady("orders", 0)

	if _, err := g.Hold(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Hold(ctx, "orders", 2); err != nil {
		t.Fatal(err)
	}
	done := make(chan bool, 1)
	go func() {
		held, _ := g.Hold(ctx, "orders", 3)
		done <- held
	}()
	select {
	case <-done:
		t.Fatal("a full window with no readiness must block")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestReleaseAllOnCancel: tearing the gate down releases every held item and
// wakes a producer blocked on a full window, which then routes live.
func TestReleaseAllOnCancel(t *testing.T) {
	g, sink := newTestGate(3)
	ctx := context.Background()
	g.Open("orders", 0)
	g.Open("other", 0)
	for i := 0; i < 3; i++ {
		if held, _ := g.Hold(ctx, "orders", i); !held {
			t.Fatal("Hold must hold while the window is open")
		}
	}
	if held, _ := g.Hold(ctx, "other", 9); !held {
		t.Fatal("the other window must hold")
	}

	done := make(chan bool, 1)
	go func() {
		held, _ := g.Hold(ctx, "orders", 99)
		done <- held
	}()
	select {
	case <-done:
		t.Fatal("the producer passed a full gate before release")
	case <-time.After(100 * time.Millisecond):
	}

	g.ReleaseAll()
	if g.IsOpen("orders", 0) || g.IsOpen("other", 0) {
		t.Fatal("ReleaseAll must close every window")
	}
	if sink.release != 4 {
		t.Fatalf("released %d items, want 4", sink.release)
	}
	select {
	case held := <-done:
		if held {
			t.Fatal("a producer must route live after ReleaseAll")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReleaseAll did not wake the producer")
	}
	if held, _ := g.Hold(ctx, "orders", 100); held {
		t.Fatal("a cleared gate must not hold")
	}
}

// TestHoldCancelledContextRoutesLive: a cancelled ctx unblocks a producer on a
// full window and reports the item live.
func TestHoldCancelledContextRoutesLive(t *testing.T) {
	g, _ := newTestGate(1)
	g.Open("orders", 0)
	if _, err := g.Hold(context.Background(), "orders", 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	held, err := g.Hold(ctx, "orders", 2)
	if held || err != nil {
		t.Fatalf("Hold with a cancelled ctx = (%v, %v), want (false, nil)", held, err)
	}
}

// TestWindowsAreIndependent: a second partition's window is never held or
// drained by the first's.
func TestWindowsAreIndependent(t *testing.T) {
	g, sink := newTestGate(16)
	ctx := context.Background()
	g.Open("orders", 0)
	g.Open("orders", 1)

	if held, _ := g.Hold(ctx, "orders", 1); !held {
		t.Fatal("Hold must hold for the table")
	}
	if key, ok := g.OpenKeyLocked("orders"); !ok || key == "" {
		t.Fatalf("OpenKeyLocked = %q, %v", key, ok)
	}

	if err := g.Close(ctx, "orders", 0); err != nil {
		t.Fatal(err)
	}
	if !g.IsOpen("orders", 1) {
		t.Fatal("closing partition 0 must not close partition 1")
	}
	if len(sink.closes) != 1 || sink.closes[0].w.Partition != 0 {
		t.Fatalf("closes = %+v, want only partition 0", sink.closes)
	}
}

// TestBufferedTablesLocked reports the tables holding items.
func TestBufferedTablesLocked(t *testing.T) {
	g, _ := newTestGate(16)
	ctx := context.Background()
	g.Open("orders", 0)
	g.Open("other", 0)
	if _, err := g.Hold(ctx, "orders", 1); err != nil {
		t.Fatal(err)
	}

	g.Lock()
	got := g.BufferedTablesLocked()
	g.Unlock()
	if len(got) != 1 || got[0] != "orders" {
		t.Fatalf("BufferedTablesLocked = %v, want [orders]", got)
	}
}
