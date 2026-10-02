package plugin

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/flightserver"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// Closing an adapter that owns an in-process Flight server (flightwrap) must
// stop the server and remove its socket (issue #496).
func TestSourceAdapterCloseStopsOwnedServer(t *testing.T) {
	srv, err := flightserver.Start(t.TempDir(), "tok", flightserver.NewSourceServer(nil, &spec.Spec{}, source.Runtime{}))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	c, err := client.Connect(context.Background(), srv.Addr(), "tok", slog.New(slog.DiscardHandler))
	if err != nil {
		srv.Stop()
		t.Fatalf("Connect: %v", err)
	}
	a := NewSourceAdapter(c, spec.Source{}, slog.New(slog.DiscardHandler), srv)
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(srv.Addr()); !os.IsNotExist(err) {
		t.Fatalf("socket %s must be removed after Close, stat err = %v", srv.Addr(), err)
	}
	_ = a.Close() // idempotent
}

// A reader's Close releases the adapter that owns its in-process server, so
// the runner's existing rdr.Close() tears the server down (issue #496).
func TestSourceReaderCloseReleasesAdapter(t *testing.T) {
	calls := 0
	r := &sourceReader{cancel: func() {}, closeAdapter: func() error { calls++; return nil }}
	r.Close()
	if calls != 1 {
		t.Fatalf("closeAdapter calls = %d, want 1", calls)
	}
}
