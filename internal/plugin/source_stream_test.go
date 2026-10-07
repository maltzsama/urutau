package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
)

// fakeFlight answers the plugin contract just enough to exercise the adapter:
// GetFlightInfo blocks for the "slow" table until released, and DoGet fails
// for the "bad" table.
type fakeFlight struct {
	flight.BaseFlightServer
	started chan string
	block   chan struct{}
}

func (f *fakeFlight) Handshake(stream flight.FlightService_HandshakeServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&flight.HandshakeResponse{})
}

func (f *fakeFlight) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	var req contract.GetFlightInfoRequest
	if err := json.Unmarshal(desc.Cmd, &req); err != nil {
		return nil, err
	}
	f.started <- req.Table
	if req.Table == "slow" {
		<-f.block
	}
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: []byte(req.Table)}}},
	}, nil
}

func (f *fakeFlight) DoGet(tkt *flight.Ticket, _ flight.FlightService_DoGetServer) error {
	if string(tkt.Ticket) == "bad" {
		return errors.New("boom: decoder")
	}
	return nil // immediate EOF
}

func newFakeReader(t *testing.T, f *fakeFlight, refs []core.TableRef) *sourceReader {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, f)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)

	cl, err := client.Connect(context.Background(), l.Addr().String(), "tok", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &sourceReader{
		client: cl,
		alloc:  memory.NewGoAllocator(),
		refs:   refs,
		ctx:    ctx,
		cancel: cancel,
		logger: slog.New(slog.DiscardHandler),
		out:    make(chan sourceResult, 16),
	}
}

// Two tables must stream CONCURRENTLY: a continuous CDC source's first table
// never ends, so streaming them serially meant the second never started
// (issue #568).
func TestSourceReaderStreamsTablesConcurrently(t *testing.T) {
	f := &fakeFlight{started: make(chan string, 4), block: make(chan struct{})}
	r := newFakeReader(t, f, []core.TableRef{
		{Source: "slow", Target: "t.slow"},
		{Source: "fast", Target: "t.fast"},
	})
	if err := r.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case table := <-f.started:
			seen[table] = true
		case <-deadline:
			t.Fatalf("only %v reached the plugin; tables stream serially, not concurrently", seen)
		}
	}
	close(f.block)
}

// A decoder/transport error must surface as an error, never as (nil, nil) —
// which the caller reads as a clean end and silently truncates the pipeline
// (issue #568).
func TestSourceReaderSurfacesStreamError(t *testing.T) {
	f := &fakeFlight{started: make(chan string, 4), block: make(chan struct{})}
	r := newFakeReader(t, f, []core.TableRef{{Source: "bad", Target: "t.bad"}})
	if err := r.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		b, err := r.Next(ctx)
		if err != nil {
			if b != nil {
				t.Fatal("Next returned a batch alongside an error")
			}
			if !strings.Contains(err.Error(), "boom") {
				t.Fatalf("Next error = %v, want the decoder error", err)
			}
			return
		}
		if b == nil {
			t.Fatal("Next returned (nil, nil): the decoder error surfaced as a clean end")
		}
	}
}

// A multi-table source must keep one resume offset per table instead of the
// last table overwriting the others (issue #568).
func TestSourceReaderPerTablePositions(t *testing.T) {
	r := &sourceReader{}

	r.recordPosition("t.a", "off-a")
	if got := r.Synced().String(); got != "off-a" {
		t.Fatalf("single-table position = %q, want the bare offset off-a", got)
	}

	r.recordPosition("t.b", "off-b")
	composed := r.Synced().String()
	env := decodePositions(StringPosition{Offset: composed})
	if env == nil || env["t.a"] != "off-a" || env["t.b"] != "off-b" {
		t.Fatalf("multi-table position = %q, decoded %v", composed, env)
	}

	// A bare offset is never mistaken for the envelope.
	if env := decodePositions(StringPosition{Offset: "b2ZmLTE="}); env != nil {
		t.Fatalf("a bare offset decoded as an envelope: %v", env)
	}
}
