package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
)

// The snapshot's blocking handshakes must observe ctx: with no relay
// goroutine servicing flushReq, Release would otherwise wait forever and wedg
// boot with no error (issue #551).
func TestRelayReleaseUnblocksWithoutRelay(t *testing.T) {
	r := newRelay(make(chan worker.Ingest, 1), nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := r.Release(ctx, "raw.orders", 0, position.MustGTID(runnerTestUUID+":1-1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Release = %v, want DeadlineExceeded (relay not running)", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Release blocked %s instead of observing ctx", elapsed)
	}
}

func TestRelayGateFlushUnblocksWithoutRelay(t *testing.T) {
	r := newRelay(make(chan worker.Ingest, 1), nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := r.GateFlush(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GateFlush = %v, want DeadlineExceeded (relay not running)", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("GateFlush blocked %s instead of observing ctx", elapsed)
	}
}

// A worker or relay death cancels the snapshot context and is surfaced by
// stop, so newRunner returns the failure instead of hanging.
func TestSnapshotWatchReportsFailure(t *testing.T) {
	workerErr := make(chan error, 1)
	sw := newSnapshotWatch(context.Background(), workerErr, make(chan error, 1))
	workerErr <- errors.New("boom")
	select {
	case <-sw.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("the watch did not cancel the snapshot context")
	}
	if err := sw.stop(); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("stop = %v, want the worker failure", err)
	}
}

// A clean snapshot stops the watch without consuming workerErr/routerDone, so
// Runner.run still sees them afterwards.
func TestSnapshotWatchCleanStopLeavesChannels(t *testing.T) {
	workerErr := make(chan error, 1)
	routerDone := make(chan error, 1)
	sw := newSnapshotWatch(context.Background(), workerErr, routerDone)
	if err := sw.stop(); err != nil {
		t.Fatalf("clean stop = %v, want nil", err)
	}
	select {
	case workerErr <- nil:
	case <-time.After(time.Second):
		t.Fatal("workerErr was consumed by the watch")
	}
	select {
	case routerDone <- nil:
	case <-time.After(time.Second):
		t.Fatal("routerDone was consumed by the watch")
	}
}
