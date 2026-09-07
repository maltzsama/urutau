// Package client wraps the Arrow Flight client with plugin-specific auth,
// heartbeat, and lifecycle management.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
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
)

// Client is a plugin Flight client with auth and heartbeat.
type Client struct {
	conn   *grpc.ClientConn
	flight flight.Client
	token  string
	mem    memory.Allocator
	logger *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// Connect creates a new client, performs Handshake, and starts heartbeat.
// addr can be a unix socket path or "host:port".
func Connect(ctx context.Context, addr, token string, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = slog.Default()
	}

	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	target := addr
	// Unix socket: use custom dialer.
	if len(addr) > 0 && (addr[0] == '/' || addr[0] == '.') {
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

	fc := flight.NewClientFromConn(conn, nil)

	// Handshake
	if err := handshake(ctx, fc, token); err != nil {
		_ = conn.Close()
		return nil, err
	}

	c := &Client{
		conn:   conn,
		flight: fc,
		token:  token,
		mem:    memory.NewGoAllocator(),
		logger: logger,
		done:   make(chan struct{}),
	}

	// Start heartbeat goroutine
	hCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.heartbeatLoop(hCtx)

	return c, nil
}

func handshake(ctx context.Context, fc flight.Client, token string) error {
	stream, err := fc.Handshake(ctx)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if err := stream.Send(&flight.HandshakeRequest{Payload: []byte(token)}); err != nil {
		return fmt.Errorf("handshake send: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("handshake recv: %w", err)
	}
	_ = resp
	return nil
}

func (c *Client) heartbeatLoop(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			err := c.doHeartbeat(ctx)
			c.mu.Unlock()
			if err != nil {
				c.logger.Error("heartbeat failed", "err", err)
			}
		}
	}
}

func (c *Client) doHeartbeat(ctx context.Context) error {
	tCtx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()

	stream, err := c.flight.DoAction(tCtx, &flight.Action{
		Type: "urutau.heartbeat",
	})
	if err != nil {
		return err
	}
	_, err = stream.Recv()
	return err
}

func (c *Client) bearerCtx(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

// ListActions returns the actions the plugin supports.
func (c *Client) ListActions(ctx context.Context) ([]*flight.ActionType, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream, err := c.flight.ListActions(c.bearerCtx(ctx), &flight.Empty{})
	if err != nil {
		return nil, err
	}
	var actions []*flight.ActionType
	for {
		a, err := stream.Recv()
		if err != nil {
			break
		}
		actions = append(actions, a)
	}
	return actions, nil
}

// ListTables calls urutau.list_tables on a source plugin.
func (c *Client) ListTables(ctx context.Context) (*contract.ListTablesResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream, err := c.flight.DoAction(c.bearerCtx(ctx), &flight.Action{
		Type: "urutau.list_tables",
	})
	if err != nil {
		return nil, err
	}
	result, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	var resp contract.ListTablesResponse
	if err := json.Unmarshal(result.Body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal list_tables: %w", err)
	}
	return &resp, nil
}

// Status calls urutau.status on the plugin.
func (c *Client) Status(ctx context.Context) (*contract.StatusResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream, err := c.flight.DoAction(c.bearerCtx(ctx), &flight.Action{
		Type: "urutau.status",
	})
	if err != nil {
		return nil, err
	}
	result, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	var resp contract.StatusResponse
	if err := json.Unmarshal(result.Body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal status: %w", err)
	}
	return &resp, nil
}

// Flush calls urutau.flush on a sink plugin.
func (c *Client) Flush(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream, err := c.flight.DoAction(c.bearerCtx(ctx), &flight.Action{
		Type: "urutau.flush",
	})
	if err != nil {
		return err
	}
	_, err = stream.Recv()
	return err
}

// Shutdown calls urutau.shutdown on the plugin.
func (c *Client) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream, err := c.flight.DoAction(c.bearerCtx(ctx), &flight.Action{
		Type: "urutau.shutdown",
	})
	if err != nil {
		return err
	}
	_, err = stream.Recv()
	return err
}

// GetFlightInfo calls GetFlightInfo on the plugin.
func (c *Client) GetFlightInfo(ctx context.Context, req contract.GetFlightInfoRequest) (*flight.FlightInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	return c.flight.GetFlightInfo(c.bearerCtx(ctx), &flight.FlightDescriptor{
		Type: flight.DescriptorCMD,
		Cmd:  b,
	})
}

// GetSchema returns the schema for a table in the given mode.
func (c *Client) GetSchema(ctx context.Context, req contract.GetFlightInfoRequest) (*arrow.Schema, error) {
	info, err := c.GetFlightInfo(ctx, req)
	if err != nil {
		return nil, err
	}
	return flight.DeserializeSchema(info.Schema, c.mem)
}

// DoGet starts a DoGet stream. Returns the flight data stream.
func (c *Client) DoGet(ctx context.Context, ticket *flight.Ticket) (flight.FlightService_DoGetClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flight.DoGet(c.bearerCtx(ctx), ticket)
}

// DoPut sends records to a sink plugin. Schema is embedded in the IPC stream.
func (c *Client) DoPut(ctx context.Context, desc contract.DoPutRequest, schema *arrow.Schema, records []arrow.RecordBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	stream, err := c.flight.DoPut(c.bearerCtx(ctx))
	if err != nil {
		return err
	}

	// Send schema as first message.
	schemaBytes := flight.SerializeSchema(schema, c.mem)
	if err := stream.Send(&flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  mustJSON(desc),
		},
		DataHeader: schemaBytes,
	}); err != nil {
		return err
	}

	// Send records as IPC batches.
	for _, rec := range records {
		var buf bytes.Buffer
		w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(c.mem))
		if err := w.Write(rec); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		if err := stream.Send(&flight.FlightData{DataBody: buf.Bytes()}); err != nil {
			return err
		}
	}

	// Close the send side and wait for ack.
	return stream.Send(nil)
}

// UnauthError returns true if the error is UNAUTHENTICATED.
func UnauthError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unauthenticated
}

// Close stops the heartbeat and closes the connection.
func (c *Client) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.done != nil {
		<-c.done
	}
	return c.conn.Close()
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
