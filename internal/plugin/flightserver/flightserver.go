// Package flightserver serves the plugin Arrow Flight contract (internal/
// plugin/contract) in-process, backed by a real source.Source or sink.Sink.
// It lets a Go .so plugin speak the SAME contract a subprocess plugin
// speaks: RegisterSource/RegisterSink stay unchanged for the plugin author,
// and driver.LoadPlugin wraps the registered driver with this server,
// dialing it back through the existing internal/plugin/client code over a
// unix socket. There is exactly one normative plugin contract — Arrow
// Flight — regardless of transport (in-process or subprocess).
package flightserver

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Server hosts one in-process Flight service (source and/or sink side) on a
// unix socket. Addr()+Token() are what driver.LoadPlugin hands to
// client.Connect — the same call a subprocess plugin's caller makes.
type Server struct {
	lis   net.Listener
	grpc  *grpc.Server
	addr  string
	token string
}

// Start listens on a fresh unix socket in dir and serves svc (a
// flight.FlightServiceServer, normally *sourceServer or *sinkServer from
// this package) with bearer-token auth matching the client's handshake/
// bearer-metadata contract. Returns once the listener is up; Serve runs in
// a background goroutine until Stop.
func Start(dir, token string, svc flight.FlightServer) (*Server, error) {
	sockPath, err := socketPath(dir)
	if err != nil {
		return nil, err
	}
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("flightserver: listen: %w", err)
	}

	authed := &authInterceptor{token: token, inner: svc}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(authed.unary),
		grpc.StreamInterceptor(authed.stream),
	)
	flight.RegisterFlightServiceServer(srv, svc)

	s := &Server{lis: lis, grpc: srv, addr: sockPath, token: token}
	go func() { _ = srv.Serve(lis) }()
	return s, nil
}

// Addr is the unix socket path — the address client.Connect dials.
func (s *Server) Addr() string { return s.addr }

// Token is the bearer token client.Connect authenticates with.
func (s *Server) Token() string { return s.token }

// Stop gracefully stops the server and removes the socket.
func (s *Server) Stop() {
	s.grpc.GracefulStop()
}

// authInterceptor rejects any RPC other than Handshake that lacks the
// correct bearer token (contract §4.6) — Handshake itself carries the
// token as its payload, not as metadata, so it is exempt.
type authInterceptor struct {
	token string
	inner flight.FlightServer
}

func (a *authInterceptor) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod != "/arrow.flight.protocol.FlightService/Handshake" && !hasBearer(ctx, a.token) {
		return nil, status.Error(codes.Unauthenticated, "flightserver: missing or invalid bearer token")
	}
	return handler(ctx, req)
}

func (a *authInterceptor) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if info.FullMethod == "/arrow.flight.protocol.FlightService/Handshake" {
		return handler(srv, ss)
	}
	if !hasBearer(ss.Context(), a.token) {
		return status.Error(codes.Unauthenticated, "flightserver: missing or invalid bearer token")
	}
	return handler(srv, ss)
}

func hasBearer(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, v := range md.Get("authorization") {
		const prefix = "Bearer "
		if len(v) > len(prefix) && v[:len(prefix)] == prefix {
			if subtle.ConstantTimeCompare([]byte(v[len(prefix):]), []byte(token)) == 1 {
				return true
			}
		}
	}
	return false
}

// heartbeatState is shared by DoAction handlers so status/list_tables
// answer consistently. Embedded (not re-declared) by sourceServer/sinkServer.
type heartbeatState struct {
	mu      sync.Mutex
	started int64 // unix seconds, set on construction
}

// socketPath returns a fresh, unpredictable unix socket path under dir
// (os.TempDir() when dir is empty).
func socketPath(dir string) (string, error) {
	if dir == "" {
		dir = os.TempDir()
	}
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("flightserver: rand: %w", err)
	}
	return filepath.Join(dir, "urutau-plugin-"+hex.EncodeToString(b)+".sock"), nil
}
