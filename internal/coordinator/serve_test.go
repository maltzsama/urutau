package coordinator

import (
	"log/slog"
	"net"
	"testing"
	"time"
)

// #602: a gRPC Serve that returns unexpectedly must surface as a fatal run
// error, not be silently discarded — otherwise the coordinator stays up with
// no control plane and no log.
func TestStartControlServerSurfacesServeError(t *testing.T) {
	c := &Coordinator{
		cfg:       Config{ListenAddr: "127.0.0.1:0"},
		log:       slog.New(slog.DiscardHandler),
		terminate: make(chan error, 1),
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := c.startControlServer(lis)
	if err != nil {
		t.Fatalf("startControlServer: %v", err)
	}
	defer srv.Stop()

	_ = lis.Close() // force Serve to return an unexpected error

	select {
	case err := <-c.terminate:
		if err == nil {
			t.Fatal("a Serve failure was surfaced as a nil error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a Serve failure was not surfaced as a fatal run error")
	}
}
