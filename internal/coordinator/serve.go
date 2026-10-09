package coordinator

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// startControlServer builds the gRPC control + Flight data server, registers
// the services, and serves on lis. It returns the server so the caller can
// Stop it.
//
// The Serve goroutine never discards its error: a Serve that returns
// unexpectedly (a bind race, fd exhaustion) would otherwise leave the
// coordinator up with no control plane and no log. It is surfaced as a fatal
// run error via c.fail (issue #602). grpc.ErrServerStopped is the normal
// return once Stop is called, so it is ignored.
func (c *Coordinator) startControlServer(lis net.Listener) (*grpc.Server, error) {
	opts := []grpc.ServerOption{
		// Keepalive agreement with the worker: MinTime ≤ client Time, else
		// the server GOAWAYs a healthy worker for pinging too much.
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 5 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: false,
		}),
		// Flight batches can be a full snapshot chunk; 128Mi covers the
		// default batching ceiling.
		grpc.MaxRecvMsgSize(128 << 20),
		grpc.MaxSendMsgSize(128 << 20),
	}
	if c.cfg.TLS.Enabled() {
		tlsOpt, err := c.cfg.TLS.ServerOption()
		if err != nil {
			return nil, fmt.Errorf("coordinator: tls: %w", err)
		}
		opts = append(opts, tlsOpt)
		c.log.Info("coordinator: control plane mTLS enabled")
	} else {
		c.log.Warn("coordinator: control plane is PLAINTEXT — the Assignment carries the source DSN; set TLS cert/key/CA (running because --allow-insecure-control-plane was set)")
	}
	srv := grpc.NewServer(opts...)
	pb.RegisterUrutauControlServer(srv, &controlServer{c: c})
	flight.RegisterFlightServiceServer(srv, &flightServer{c: c})
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			c.fail(fmt.Errorf("coordinator: grpc serve: %w", err))
		}
	}()
	return srv, nil
}
