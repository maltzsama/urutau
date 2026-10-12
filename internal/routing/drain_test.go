package routing

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerCapAndDrainTimeout(t *testing.T) {
	if got := WorkerCap(6, 12); got != 6 {
		t.Fatalf("cap = %d, want the table's own 6", got)
	}
	if got := WorkerCap(0, 12); got != 12 {
		t.Fatalf("cap = %d, want the pipeline's 12", got)
	}
	if got := WorkerCap(0, 0); got != DefaultMaxWorkers {
		t.Fatalf("cap = %d, want the default", got)
	}
	if DrainTimeout(0) != DefaultDrainTimeout || DrainTimeout(time.Second) != time.Second {
		t.Fatal("drain timeout does not fall back to its default only when unset")
	}
}

// The drain returns once nothing is owed, waking on the ack notification.
func TestAwaitDrainedReturnsWhenNothingIsOwed(t *testing.T) {
	var left atomic.Int64
	left.Store(2)
	wake := make(chan struct{}, 1)
	go func() {
		for left.Add(-1) > 0 {
			wake <- struct{}{}
		}
		wake <- struct{}{}
	}()
	owed := func() Owed { return Owed{InFlight: int(max(left.Load(), 0))} }
	if err := AwaitDrained(context.Background(), 5*time.Second, wake, owed, func() []uint64 { return nil }); err != nil {
		t.Fatalf("AwaitDrained: %v", err)
	}
}

// Past its bound the drain fails and says what is still owed; a cancelled
// context ends it too.
func TestAwaitDrainedTimesOutWithWhatIsOwed(t *testing.T) {
	owed := func() Owed { return Owed{InFlight: 1, Queued: 2, CyclesOpen: 3, CyclesDone: 4} }
	err := AwaitDrained(context.Background(), 20*time.Millisecond, nil, owed, func() []uint64 { return []uint64{7} })
	if err == nil {
		t.Fatal("the drain returned with batches still owed")
	}
	for _, want := range []string{"10 batch(es)/cycle(s)", "in-flight=1", "queued=2", "staged-cycles=7", "open=3", "done=4", "open-seqs=[7]"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not say %q", err, want)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := AwaitDrained(ctx, time.Minute, nil, owed, func() []uint64 { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestAwaitIdleEnds(t *testing.T) {
	if got := AwaitIdle(context.Background(), time.Minute, nil, func() bool { return false }); got != Idle {
		t.Fatalf("an idle owner: end = %v, want Idle", got)
	}
	if got := AwaitIdle(context.Background(), 20*time.Millisecond, nil, func() bool { return true }); got != IdleTimedOut {
		t.Fatalf("an owner that never drains: end = %v, want IdleTimedOut", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := AwaitIdle(ctx, time.Minute, nil, func() bool { return true }); got != IdleCancelled {
		t.Fatalf("a cancelled wait: end = %v, want IdleCancelled", got)
	}
}
