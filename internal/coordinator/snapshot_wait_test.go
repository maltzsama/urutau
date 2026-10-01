package coordinator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/snapshot"
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
	c := &Coordinator{}
	lost := make(chan struct{})
	reader := &neverCaughtReader{polled: make(chan struct{})}

	high := position.MustGTID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	cfg := snapshot.SnapshotConfig{CaughtUpPoll: time.Millisecond, WindowTimeout: 30 * time.Second}

	// Close lost only once the wait is actually polling, so the abort lands
	// mid-wait without a sleep.
	go func() {
		<-reader.polled
		close(lost)
	}()

	start := time.Now()
	err := c.waitCaughtUpOrLost(context.Background(), reader, high, cfg, 0, lost)
	elapsed := time.Since(start)

	if !errors.Is(err, errWorkerLost) {
		t.Fatalf("err = %v, want errWorkerLost", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("waited %s; the wait must abort on worker loss, not run the window timeout", elapsed)
	}
}
