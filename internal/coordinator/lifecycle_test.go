package coordinator

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/observability"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// #493: once boot is done nothing drains ready, so a re-attach must not block
// on a full buffer.
func TestSignalReadySkipsAfterBoot(t *testing.T) {
	c := &Coordinator{ready: make(chan struct{})} // unbuffered, nothing drains
	c.booted.Store(true)

	done := make(chan struct{})
	go func() {
		c.signalReady(context.Background(), context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("signalReady blocked after boot with nothing draining ready (issue #493)")
	}
}

// #493: during boot the send still blocks so waitWorkers is woken.
func TestSignalReadyBlocksDuringBootUntilDrained(t *testing.T) {
	c := &Coordinator{ready: make(chan struct{})}

	done := make(chan struct{})
	go func() {
		c.signalReady(context.Background(), context.Background())
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("signalReady must block during boot until ready is drained")
	case <-time.After(20 * time.Millisecond):
	}
	<-c.ready
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("signalReady stayed blocked after ready was drained")
	}
}

// #494: a saturated emit queue drops the event instead of blocking the ack path.
func TestEmitCommitDropsWhenSaturated(t *testing.T) {
	c := &Coordinator{
		log:     slog.New(slog.DiscardHandler),
		emitSem: make(chan struct{}, 1),
	}
	c.emitSem <- struct{}{} // saturate the single slot

	done := make(chan struct{})
	go func() {
		c.emitCommit("w", &pb.Ack{Table: "t"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emitCommit blocked on a saturated queue (issue #494)")
	}
	if len(c.emitSem) != 1 {
		t.Fatalf("emitSem len = %d, want 1: a dropped event must not enqueue", len(c.emitSem))
	}
}

// #495: shutdownMetrics stops the listener and releases its goroutine.
func TestShutdownMetricsStopsServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := observability.NewServer(ln.Addr().String(), http.NotFoundHandler())
	c := &Coordinator{log: slog.New(slog.DiscardHandler), metricsSrv: srv}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	c.shutdownMetrics()

	select {
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("Serve returned %v, want http.ErrServerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the metrics server was not stopped (issue #495)")
	}
}

// #495: no server configured (MetricsAddr empty) is a no-op, not a panic.
func TestShutdownMetricsNilIsNoOp(t *testing.T) {
	c := &Coordinator{log: slog.New(slog.DiscardHandler)}
	c.shutdownMetrics()
}
