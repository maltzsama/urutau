package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// neverCaughtReader never reaches the high watermark, so WaitCaughtUp would
// poll until its window timeout unless the wait is aborted.
type neverCaughtReader struct{}

func (neverCaughtReader) Synced() position.Position { return position.MustGTID("") }

func (neverCaughtReader) Master(context.Context) (position.Position, error) {
	return position.MustGTID(""), nil
}

func (neverCaughtReader) OpenWindow(context.Context, uint32) {}
func (neverCaughtReader) ClearWindow()                       {}

var _ source.SourceReader = neverCaughtReader{}

// A worker lost while the reader catches up must abort the wait as a worker
// loss (so the partition redoes the chunk, #461) instead of burning the whole
// window timeout and ending the run (issue #526).
func TestWaitCaughtUpOrLostAbortsOnWorkerLoss(t *testing.T) {
	c := &Coordinator{}
	lost := make(chan struct{})

	high := position.MustGTID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	cfg := snapshot.SnapshotConfig{CaughtUpPoll: time.Millisecond, WindowTimeout: 30 * time.Second}

	go func() {
		time.Sleep(20 * time.Millisecond)
		close(lost)
	}()

	start := time.Now()
	err := c.waitCaughtUpOrLost(context.Background(), neverCaughtReader{}, high, cfg, lost)
	elapsed := time.Since(start)

	if !errors.Is(err, errWorkerLost) {
		t.Fatalf("err = %v, want errWorkerLost", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("waited %s; the wait must abort on worker loss, not run the window timeout", elapsed)
	}
}
