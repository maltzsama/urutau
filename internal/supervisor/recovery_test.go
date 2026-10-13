package supervisor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// The crash that reaches the limit is a crash loop; a loss that is not a
// crash never is.
func TestWatchBackCallsACrashLoopAtTheLimit(t *testing.T) {
	var w Watch
	now := time.Now()
	for i := int32(1); i <= 3; i++ {
		w.Health.Loss("w1", uint64(i), now)
		back := w.Back("w1", oom("pod-a", i), true, 0, now, 3)
		if !back.Crashed || back.Crashes != int(i) || back.Loop != (i == 3) {
			t.Fatalf("crash %d: %+v, want a crash and a loop only at the third", i, back)
		}
	}
	w.Health.Loss("w2", 1, now)
	if back := w.Back("w2", oom("pod-b", 0), true, 0, now, 1); back.Crashed || back.Loop {
		t.Fatalf("a reconnect with no restart: %+v, want no crash", back)
	}
}

func TestDefaults(t *testing.T) {
	if MaxCrashes(0) != DefaultMaxConsecutiveCrashes || MaxCrashes(7) != 7 {
		t.Fatal("MaxCrashes does not fall back to its default only when unset")
	}
	if DeliveryTimeout(0) != DefaultDeliveryTimeout || DeliveryTimeout(time.Second) != time.Second {
		t.Fatal("DeliveryTimeout does not fall back to its default only when unset")
	}
	if AckTimeout(0) != DefaultAckTimeout || AckTimeout(time.Second) != time.Second {
		t.Fatal("AckTimeout does not fall back to its default only when unset")
	}
}

// The wait ends when the worker is attached and no longer pending, and with
// ErrNotBack once the timeout has passed since the loss — not since the wait
// began.
func TestWatchAwaitBack(t *testing.T) {
	var w Watch
	var attached atomic.Bool
	w.MarkPending("w1")
	w.Health.Loss("w1", 1, time.Now())
	done := make(chan error, 1)
	go func() { done <- w.AwaitBack(context.Background(), "w1", time.Minute, attached.Load) }()
	attached.Store(true)
	select {
	case err := <-done:
		t.Fatalf("returned %v while the worker is still awaited", err)
	case <-time.After(3 * reattachPoll):
	}
	w.NoteAttach("w1", time.Now())
	if err := <-done; err != nil {
		t.Fatalf("AwaitBack = %v, want nil once the worker is back", err)
	}

	var late Watch
	late.MarkPending("w1")
	late.Health.Loss("w1", 1, time.Now().Add(-2*time.Minute))
	if err := late.AwaitBack(context.Background(), "w1", time.Minute, func() bool { return false }); !errors.Is(err, ErrNotBack) {
		t.Fatalf("AwaitBack = %v, want ErrNotBack: the loss is older than the timeout", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	late.Health.Loss("w2", 1, time.Now())
	if err := late.AwaitBack(ctx, "w2", time.Minute, func() bool { return false }); !errors.Is(err, context.Canceled) {
		t.Fatalf("AwaitBack = %v, want context.Canceled", err)
	}
}

// Run hands the first tick error to terminate and stops ticking.
func TestRunStopsOnATickError(t *testing.T) {
	boom := errors.New("worker gone")
	var ticks atomic.Int64
	terminate := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		Run(context.Background(), time.Millisecond, func(time.Time) error {
			if ticks.Add(1) == 3 {
				return boom
			}
			return nil
		}, terminate)
		close(done)
	}()
	select {
	case err := <-terminate:
		if !errors.Is(err, boom) {
			t.Fatalf("terminate got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the tick error never reached terminate")
	}
	<-done
	if n := ticks.Load(); n != 3 {
		t.Fatalf("%d ticks, want it to stop at the failing one", n)
	}
}
