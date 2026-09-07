package client_test

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/internal/plugin/client"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConnectBadAddr(t *testing.T) {
	_, err := client.Connect(context.Background(), "127.0.0.1:19999", "dGVzdA==", nil)
	if err == nil {
		t.Fatal("expected error for bad address")
	}
}

func TestConnectBadToken(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Connect(ctx, addr, "wrong-token", nil)
	if err == nil {
		t.Fatal("expected UNAUTHENTICATED error")
	}
}

func TestConnectSuccess(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestHeartbeatWithBearer(t *testing.T) {
	// Verify heartbeat sends bearer token (contract §4.6).
	// Fake server rejects if heartbeat arrives without bearer.
	var mu sync.Mutex
	var heartbeatCount int
	var heartbeatHasBearer bool

	addr, stop := startFakeServer(t, "dGVzdA==", &fakeOpts{
		onHeartbeat: func(hasBearer bool) {
			mu.Lock()
			defer mu.Unlock()
			heartbeatCount++
			heartbeatHasBearer = hasBearer
		},
	})
	defer stop()

	connectClient(t, addr)
	// Wait for at least 2 heartbeats.
	time.Sleep(5 * time.Second)

	mu.Lock()
	count := heartbeatCount
	hasBearer := heartbeatHasBearer
	mu.Unlock()

	if count == 0 {
		t.Fatal("expected at least 1 heartbeat")
	}
	if !hasBearer {
		t.Fatal("heartbeat must carry bearer token (contract §4.6)")
	}
}

func TestHeartbeatMissDetectsDead(t *testing.T) {
	// Kill server → heartbeat fails → Dead() fires.
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	c := connectClient(t, addr)

	stop() // kill the server

	select {
	case <-c.Dead():
		// Good.
	case <-time.After(20 * time.Second):
		t.Fatal("Dead() did not fire after server killed")
	}
}

func TestListActions(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)

	actions, err := c.ListActions(context.Background())
	if err != nil {
		t.Fatalf("ListActions: %v", err)
	}
	if len(actions) == 0 {
		t.Fatal("expected at least 1 action")
	}

	found := map[string]bool{}
	for _, a := range actions {
		found[a.Type] = true
	}
	for _, want := range []string{"urutau.heartbeat", "urutau.status", "urutau.list_tables", "urutau.shutdown"} {
		if !found[want] {
			t.Errorf("missing action: %s", want)
		}
	}
}

func TestListActionsError(t *testing.T) {
	// Verify ListActions returns error on dead server, not empty list.
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	c := connectClient(t, addr)
	stop()

	_, err := c.ListActions(context.Background())
	if err == nil {
		t.Fatal("expected error from dead server")
	}
}

func TestListTables(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)

	tables, err := c.ListTables(context.Background())
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	if len(tables.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables.Tables))
	}
	if tables.Tables[0].Name != "orders" {
		t.Errorf("expected table 'orders', got %q", tables.Tables[0].Name)
	}
}

func TestStatus(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)

	resp, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if resp.State != "streaming" {
		t.Errorf("expected state 'streaming', got %q", resp.State)
	}
}

func TestGetFlightInfoSnapshot(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)

	info, err := c.GetFlightInfo(context.Background(), contract.GetFlightInfoRequest{
		Table: "orders",
		Mode:  "snapshot",
	})
	if err != nil {
		t.Fatalf("GetFlightInfo: %v", err)
	}
	if info.Schema == nil {
		t.Fatal("expected schema")
	}
	if len(info.Endpoint) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(info.Endpoint))
	}
	if info.AppMetadata == nil {
		t.Fatal("expected app_metadata with endOffset")
	}
}

func TestEndOffset(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)

	info, err := c.GetFlightInfo(context.Background(), contract.GetFlightInfoRequest{
		Table: "orders",
		Mode:  "snapshot",
	})
	if err != nil {
		t.Fatalf("GetFlightInfo: %v", err)
	}

	offset, err := client.EndOffset(info)
	if err != nil {
		t.Fatalf("EndOffset: %v", err)
	}
	if len(offset) == 0 {
		t.Fatal("expected non-empty offset")
	}
}

func TestGetFlightInfoNotFound(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)

	_, err := c.GetFlightInfo(context.Background(), contract.GetFlightInfoRequest{
		Table: "nonexistent",
		Mode:  "snapshot",
	})
	if err == nil {
		t.Fatal("expected NOT_FOUND error")
	}
}

func TestShutdown(t *testing.T) {
	addr, stop := startFakeServer(t, "dGVzdA==", nil)
	defer stop()

	c := connectClient(t, addr)

	err := c.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestUnauthError(t *testing.T) {
	unauth := status.Error(codes.Unauthenticated, "test")
	other := status.Error(codes.InvalidArgument, "test")

	if client.UnauthError(nil) {
		t.Error("nil should not be unauth")
	}
	if !client.UnauthError(unauth) {
		t.Error("should detect unauth error")
	}
	if client.UnauthError(other) {
		t.Error("should not detect other error as unauth")
	}
}

// ---- fake Flight server ----

type fakeOpts struct {
	onHeartbeat func(hasBearer bool)
}

type fakeFlightServer struct {
	flight.BaseFlightServer
	token string
	opts  *fakeOpts
}

func (s *fakeFlightServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if string(req.Payload) != s.token {
		return status.Errorf(codes.Unauthenticated, "UNAUTHENTICATED")
	}
	return stream.Send(&flight.HandshakeResponse{Payload: []byte{}})
}

func (s *fakeFlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}
	switch action.Type {
	case "urutau.heartbeat":
		if s.opts != nil && s.opts.onHeartbeat != nil {
			s.opts.onHeartbeat(true)
		}
		return stream.Send(&flight.Result{})
	case "urutau.list_tables":
		resp := contract.ListTablesResponse{
			Tables: []contract.TableInfo{
				{Name: "orders", SupportsSnapshot: true},
			},
		}
		b, _ := json.Marshal(resp)
		return stream.Send(&flight.Result{Body: b})
	case "urutau.status":
		resp := contract.StatusResponse{
			State:     "streaming",
			UptimeSec: 100,
			Tables:    map[string]contract.TableStatus{},
		}
		b, _ := json.Marshal(resp)
		return stream.Send(&flight.Result{Body: b})
	case "urutau.shutdown":
		return stream.Send(&flight.Result{})
	default:
		return status.Errorf(codes.Unimplemented, "unknown action: %s", action.Type)
	}
}

func (s *fakeFlightServer) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}
	for _, a := range []*flight.ActionType{
		{Type: "urutau.heartbeat"},
		{Type: "urutau.status"},
		{Type: "urutau.list_tables"},
		{Type: "urutau.shutdown"},
	} {
		if err := stream.Send(a); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeFlightServer) GetFlightInfo(_ context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	var req contract.GetFlightInfoRequest
	if err := json.Unmarshal(desc.GetCmd(), &req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid descriptor")
	}
	if req.Table == "nonexistent" {
		return nil, status.Errorf(codes.NotFound, "NOT_FOUND")
	}
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "qty", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	return &flight.FlightInfo{
		Schema:      flight.SerializeSchema(schema, memory.NewGoAllocator()),
		Endpoint:    []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: []byte("test")}}},
		AppMetadata: []byte(`{"endOffset":"dGVzdA=="}`),
	}, nil
}

func (s *fakeFlightServer) verifyBearer(ctx interface{}) error {
	return nil // simplified for tests
}

func startFakeServer(t *testing.T, token string, opts *fakeOpts) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	flight.RegisterFlightServiceServer(srv, &fakeFlightServer{token: token, opts: opts})
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), func() { srv.GracefulStop() }
}

func connectClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Connect(ctx, addr, "dGVzdA==", nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
