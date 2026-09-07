package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	heartbeatInterval = 2 * time.Second
	heartbeatTimeout  = 1 * time.Second
	healthThreshold   = 7
	handshakeTimeout  = 10 * time.Second
	actionTimeout     = 10 * time.Second
	flushTimeout      = 30 * time.Second
	shutdownTimeout   = 15 * time.Second
)

// Client is a plugin Flight client with auth, heartbeat, and health
// monitoring. No mutex around RPCs: grpc.ClientConn is safe for concurrent
// use and multiplexes over HTTP/2. A shared lock would starve the heartbeat
// during long DoPuts — liveness never waits on the data path.
type Client struct {
	conn   *grpc.ClientConn
	flight flight.Client
	token  string
	mem    memory.Allocator
	logger *slog.Logger

	hbCancel context.CancelFunc
	done     chan struct{}

	healthy  atomic.Bool
	failures atomic.Int32
	dead     chan struct{}
	deadErr  error
	deadOnce sync.Once

	closeOnce sync.Once
	closeErr  error
}

func Connect(ctx context.Context, addr, token string, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = slog.Default()
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	target := addr
	if isUnixPath(addr) {
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", addr)
		}))
		target = "unix://" + addr
	}

	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial plugin: %w", err)
	}

	c := &Client{
		conn:   conn,
		flight: flight.NewClientFromConn(conn, nil),
		token:  token,
		mem:    memory.NewGoAllocator(),
		logger: logger,
		dead:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	c.healthy.Store(true)

	hctx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()
	if err := c.handshake(hctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}

	hbCtx, hbCancel := context.WithCancel(context.Background())
	c.hbCancel = hbCancel
	go c.heartbeatLoop(hbCtx)

	return c, nil
}

func (c *Client) handshake(ctx context.Context) error {
	stream, err := c.flight.Handshake(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&flight.HandshakeRequest{Payload: []byte(c.token)}); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	if len(resp.Payload) != 0 {
		c.logger.Warn("handshake response has non-empty payload", "len", len(resp.Payload))
	}
	return nil
}

func (c *Client) bearerCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

// ---- heartbeat & health ----

func (c *Client) heartbeatLoop(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.heartbeatOnce(ctx); err != nil {
				if UnauthError(err) {
					c.markDead(fmt.Errorf("heartbeat rejected: %w", err))
					return
				}
				n := c.failures.Add(1)
				c.logger.Warn("heartbeat failed", "err", err, "consecutive", n)
				if int(n) >= healthThreshold {
					c.markDead(fmt.Errorf("plugin unhealthy: %d consecutive heartbeat failures", n))
					return
				}
				continue
			}
			c.failures.Store(0)
			c.healthy.Store(true)
		}
	}
}

func (c *Client) heartbeatOnce(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()
	stream, err := c.flight.DoAction(c.bearerCtx(ctx), &flight.Action{
		Type: "urutau.heartbeat",
	})
	if err != nil {
		return err
	}
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return drainActionStream(stream)
}

func (c *Client) markDead(err error) {
	c.healthy.Store(false)
	c.logger.Error("plugin marked unhealthy", "err", err)
	c.deadOnce.Do(func() {
		c.deadErr = err
		close(c.dead)
	})
}

func (c *Client) Dead() <-chan struct{} { return c.dead }
func (c *Client) DeadErr() error        { return c.deadErr }
func (c *Client) Healthy() bool         { return c.healthy.Load() }

// ---- actions (contract §11) ----

func (c *Client) doAction(ctx context.Context, name string, result any) error {
	ctx, cancel := withTimeout(ctx, actionTimeout)
	defer cancel()

	stream, err := c.flight.DoAction(c.bearerCtx(ctx), &flight.Action{Type: name})
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	res, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if err := drainActionStream(stream); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	if result != nil {
		if err := json.Unmarshal(res.Body, result); err != nil {
			return fmt.Errorf("%s: unmarshal: %w", name, err)
		}
	}
	return nil
}

func drainActionStream(stream flight.FlightService_DoActionClient) error {
	if _, err := stream.Recv(); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	return fmt.Errorf("more than one ActionResult (contract §11)")
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

func (c *Client) ListTables(ctx context.Context) (*contract.ListTablesResponse, error) {
	var resp contract.ListTablesResponse
	if err := c.doAction(ctx, "urutau.list_tables", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Status(ctx context.Context) (*contract.StatusResponse, error) {
	var resp contract.StatusResponse
	if err := c.doAction(ctx, "urutau.status", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) Flush(ctx context.Context) error {
	ctx, cancel := withTimeout(ctx, flushTimeout)
	defer cancel()
	return c.doAction(ctx, "urutau.flush", nil)
}

func (c *Client) Shutdown(ctx context.Context) error {
	ctx, cancel := withTimeout(ctx, shutdownTimeout)
	defer cancel()
	return c.doAction(ctx, "urutau.shutdown", nil)
}

func (c *Client) ListActions(ctx context.Context) ([]*flight.ActionType, error) {
	stream, err := c.flight.ListActions(c.bearerCtx(ctx), &flight.Empty{})
	if err != nil {
		return nil, err
	}
	var actions []*flight.ActionType
	for {
		a, err := stream.Recv()
		if err == io.EOF {
			return actions, nil
		}
		if err != nil {
			return nil, err
		}
		actions = append(actions, a)
	}
}

// ---- data plane ----

func (c *Client) GetFlightInfo(ctx context.Context, req contract.GetFlightInfoRequest) (*flight.FlightInfo, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal descriptor: %w", err)
	}
	return c.flight.GetFlightInfo(c.bearerCtx(ctx), &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  b,
	})
}

// EndOffset extracts the snapshot handoff offset from a snapshot
// FlightInfo's app_metadata (contract §6/§7).
func EndOffset(info *flight.FlightInfo) ([]byte, error) {
	var meta struct {
		EndOffset string `json:"endOffset"`
	}
	if len(info.AppMetadata) == 0 {
		return nil, fmt.Errorf("FlightInfo has no app_metadata")
	}
	if err := json.Unmarshal(info.AppMetadata, &meta); err != nil {
		return nil, fmt.Errorf("parse app_metadata: %w", err)
	}
	if meta.EndOffset == "" {
		return nil, fmt.Errorf("app_metadata missing endOffset")
	}
	return base64.StdEncoding.DecodeString(meta.EndOffset)
}

func (c *Client) GetSchema(ctx context.Context, req contract.GetFlightInfoRequest) (*arrow.Schema, error) {
	info, err := c.GetFlightInfo(ctx, req)
	if err != nil {
		return nil, err
	}
	return flight.DeserializeSchema(info.Schema, c.mem)
}

func (c *Client) DoGet(ctx context.Context, ticket *flight.Ticket) (flight.FlightService_DoGetClient, error) {
	return c.flight.DoGet(c.bearerCtx(ctx), ticket)
}

// DoPut writes records to a sink (contract §10). Uses flight.NewRecordWriter
// for correct wire format. Drains ack stream to surface sink errors.
func (c *Client) DoPut(ctx context.Context, desc contract.DoPutRequest, schema *arrow.Schema, records []arrow.RecordBatch) error {
	descJSON, err := json.Marshal(desc)
	if err != nil {
		return fmt.Errorf("marshal descriptor: %w", err)
	}

	stream, err := c.flight.DoPut(c.bearerCtx(ctx))
	if err != nil {
		return err
	}

	w := flight.NewRecordWriter(stream,
		ipc.WithSchema(schema),
		ipc.WithAllocator(c.mem),
	)
	w.SetFlightDescriptor(&flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  descJSON,
	})

	for _, rec := range records {
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	_ = stream.CloseSend()

	// Drain ack: sink errors surface only here (contract §10).
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("sink rejected write: %w", err)
		}
	}
}

func UnauthError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unauthenticated
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.hbCancel()
		<-c.done
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

func isUnixPath(addr string) bool {
	return len(addr) > 0 && (addr[0] == '/' || addr[0] == '.')
}
