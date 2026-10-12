package snapshot

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// neverCaughtReader never reaches the high watermark, so WaitCaughtUp would
// poll until its window timeout unless the wait is aborted. polled closes on
// the first Synced call, so a test can coordinate with the wait instead of
// racing a sleep.
type neverCaughtReader struct {
	once   sync.Once
	polled chan struct{}
}

func (r *neverCaughtReader) Synced() position.Position {
	r.once.Do(func() { close(r.polled) })
	return position.MustGTID("")
}

func (r *neverCaughtReader) Master(context.Context) (position.Position, error) {
	return position.MustGTID(""), nil
}

func (r *neverCaughtReader) OpenWindow(context.Context, uint32) {}
func (r *neverCaughtReader) ClearWindow()                       {}

var _ source.SourceReader = (*neverCaughtReader)(nil)

// A worker lost while the reader catches up must abort the wait as a worker
// loss (so the partition redoes the chunk, #461) instead of burning the whole
// window timeout and ending the run (issue #526).
func TestWaitCaughtUpOrLostAbortsOnWorkerLoss(t *testing.T) {
	lost := make(chan struct{})
	reader := &neverCaughtReader{polled: make(chan struct{})}

	high := position.MustGTID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	cfg := SnapshotConfig{CaughtUpPoll: time.Millisecond, WindowTimeout: 30 * time.Second}

	// Close lost only once the wait is actually polling, so the abort lands
	// mid-wait without a sleep.
	go func() {
		<-reader.polled
		close(lost)
	}()

	start := time.Now()
	err := WaitCaughtUpOrLost(context.Background(), reader, high, cfg, 0, lost)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrWorkerLost) {
		t.Fatalf("err = %v, want ErrWorkerLost", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("waited %s; the wait must abort on worker loss, not run the window timeout", elapsed)
	}
}

// Await returns only once the pass's last cycle is resolved, with the chunk
// an owner lost; a cancelled context ends it.
func TestFanoutPassesAwait(t *testing.T) {
	var f FanoutPasses
	f.Begin("raw.a")
	f.Note("raw.a", 1, 4)
	f.Note("raw.a", 2, 5)

	type result struct {
		from uint32
		lost bool
		err  error
	}
	done := make(chan result, 1)
	go func() {
		from, lost, err := f.Await(context.Background(), "raw.a")
		done <- result{from, lost, err}
	}()

	f.Resolve("raw.a", 1, true)
	select {
	case r := <-done:
		t.Fatalf("Await returned %+v with a cycle still open", r)
	case <-time.After(3 * fanoutDrainPoll):
	}
	f.Resolve("raw.a", 2, false)
	select {
	case r := <-done:
		if r.err != nil || !r.lost || r.from != 4 {
			t.Fatalf("Await = %+v, want chunk 4 lost", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Await did not return once the pass drained")
	}

	f.Note("raw.a", 3, 6)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := f.Await(ctx, "raw.a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Await on a cancelled context = %v, want context.Canceled", err)
	}
}

func TestDecodeHighKeyOfAWindowWithoutOne(t *testing.T) {
	key, err := DecodeHighKey(nil)
	if key != nil || err != nil {
		t.Fatalf("DecodeHighKey(nil) = %v, %v; want no key", key, err)
	}
}
