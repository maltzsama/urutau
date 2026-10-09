package coordinator

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/maltzsama/urutau/internal/dashboard"
	"github.com/maltzsama/urutau/internal/observability"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// #493: once boot is done nothing drains ready, so a re-attach must not block
// on a full buffer.
func TestSignalReadySkipsAfterBoot(t *testing.T) {
	c := &Coordinator{ready: make(chan struct{})} // unbuffered, nothing drains
	c.booted = make(chan struct{})
	close(c.booted)

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
	c := &Coordinator{ready: make(chan struct{})} // booted nil: still booting

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

// #493: a session already blocked on a full ready must unblock when boot
// finishes, even though nothing drains ready afterwards.
func TestSignalReadyUnblocksOnBootWithFullReady(t *testing.T) {
	c := &Coordinator{ready: make(chan struct{}, 1)}
	c.ready <- struct{}{} // full: the send cannot proceed
	c.booted = make(chan struct{})

	done := make(chan struct{})
	go func() {
		c.signalReady(context.Background(), context.Background())
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("signalReady returned before boot finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(c.booted)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("signalReady stayed blocked after boot with a full ready (issue #493)")
	}
}

// #548: emitCommit is non-blocking — it records the dashboard event and hands
// the trail write to the eventlog's own bounded queue, so the ack path never
// waits on S3.
func TestEmitCommitIsNonBlocking(t *testing.T) {
	c := &Coordinator{
		log:        slog.New(slog.DiscardHandler),
		dashEvents: dashboard.NewEvents(10),
	}

	done := make(chan struct{})
	go func() {
		c.emitCommit("w", &pb.Ack{Table: "t"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emitCommit blocked (issue #548)")
	}
	if got := c.dashEvents.List("", "", 10); len(got) != 1 {
		t.Fatalf("dashboard events = %d, want 1", len(got))
	}
}

// #495: shutdownMetrics stops the listener and releases its goroutine.
func TestShutdownMetricsStopsServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := observability.NewServer(ln.Addr().String(), http.NotFoundHandler())
	c := &Coordinator{
		log:         slog.New(slog.DiscardHandler),
		metricsSrv:  srv,
		metricsDone: make(chan struct{}),
	}

	serveErr := make(chan error, 1)
	go func() {
		defer close(c.metricsDone)
		serveErr <- srv.Serve(ln)
	}()

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

// #671: a destructive DDL the source carried but the engine did not propagate
// increments the metric and records a dashboard/run event.
func TestReportDestructiveDDLRecordsMetricAndEvent(t *testing.T) {
	c := &Coordinator{
		log:        slog.New(slog.DiscardHandler),
		metrics:    observability.New(),
		dashEvents: dashboard.NewEvents(10),
	}
	c.reportDestructiveDDL(source.DestructiveDDL{Source: "postgres", Kind: "truncate", Table: "public.orders"})
	if got := testutil.ToFloat64(c.metrics.SourceTruncates.WithLabelValues("postgres", "public.orders")); got != 1 {
		t.Fatalf("truncate metric = %v, want 1", got)
	}
	c.reportDestructiveDDL(source.DestructiveDDL{Source: "mysql", Kind: "ddl", Table: "orders"})
	if got := testutil.ToFloat64(c.metrics.SourceDestructiveDDL.WithLabelValues("mysql", "ddl", "orders")); got != 1 {
		t.Fatalf("ddl metric = %v, want 1", got)
	}
	if got := c.dashEvents.List("", "", 10); len(got) != 2 {
		t.Fatalf("dashboard events = %d, want 2", len(got))
	}
}
