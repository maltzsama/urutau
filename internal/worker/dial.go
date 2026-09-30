package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/maltzsama/urutau/internal/grpctls"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// dialCoordinator opens the worker's one ClientConn to the coordinator.
//
// A bare host:port goes through the passthrough resolver, so every connection
// attempt resolves the name afresh, and reconnects back off at most
// reconnectMaxDelay. gRPC's defaults (the dns resolver, a 120s backoff cap)
// left a worker blind to its coordinator for up to two minutes after an
// outage: longer than the coordinator waits for its workers, so every
// restarted coordinator gave up before the worker looked again (chaos run
// d582cf2).
func dialCoordinator(target string, tlsCfg grpctls.Config) (*grpc.ClientConn, error) {
	opts, err := dialOpts(tlsCfg)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(target, "://") {
		target = "passthrough:///" + target
	}
	opts = append(opts, grpc.WithConnectParams(grpc.ConnectParams{
		Backoff:           backoff.Config{BaseDelay: time.Second, Multiplier: 1.6, Jitter: 0.2, MaxDelay: reconnectMaxDelay},
		MinConnectTimeout: 20 * time.Second,
	}))
	return grpc.NewClient(target, opts...)
}

// reconnectMaxDelay caps the wait between attempts to reach the coordinator.
const reconnectMaxDelay = 5 * time.Second

// dialOpts carries keepalive that converts a frozen coordinator into a
// dead channel in ~15s. MinTime on the server must be ≤ Time here, or the
// server GOAWAYs the client for pinging too much. The max message size must
// cover a full snapshot window chunk (default 4Mi is too small for real
// batches).
func dialOpts(tlsCfg grpctls.Config) ([]grpc.DialOption, error) {
	creds := grpc.WithTransportCredentials(insecure.NewCredentials())
	if tlsCfg.Enabled() {
		c, err := tlsCfg.ClientCreds()
		if err != nil {
			// No correct plaintext fallback when TLS is configured: dialing
			// insecure against a TLS coordinator is a silent downgrade
			// (issue #265).
			return nil, fmt.Errorf("worker: TLS client credentials: %w", err)
		}
		creds = grpc.WithTransportCredentials(c)
	}
	return []grpc.DialOption{
		creds,
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(128<<20),
			grpc.MaxCallSendMsgSize(128<<20),
		),
	}, nil
}

// sessionWithRetry opens the Session stream, tolerating a coordinator that
// is still booting its listener.
func sessionWithRetry(ctx context.Context, conn *grpc.ClientConn, log *slog.Logger) (pb.UrutauControl_SessionClient, error) {
	// Short waits, many tries: the budget still covers a slow rollout
	// (~2min), but a coordinator that comes back is found within
	// reconnectMaxDelay instead of after a 30s+ sleep.
	const maxTries = 30
	const base = 250 * time.Millisecond
	const cap = reconnectMaxDelay
	var last error
	for attempt := 0; attempt < maxTries; attempt++ {
		s, err := pb.NewUrutauControlClient(conn).Session(ctx)
		if err == nil {
			return s, nil
		}
		last = err
		log.Warn("worker: session retry", "attempt", attempt+1, "err", err)
		// Exponential backoff with jitter: a coordinator rolling out (a new
		// Pod, a TLS cert swap) takes longer than a fixed 5s budget, and
		// jitter keeps a fleet of workers from retrying in lockstep
		// (issue #270).
		d := base << min(attempt, 8)
		if d > cap {
			d = cap
		}
		d += time.Duration(rand.Int64N(int64(d)/5 + 1))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d):
		}
	}
	return nil, last
}
