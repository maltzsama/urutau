package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	mu            sync.Mutex
	lastHeartbeat time.Time
	writeCount    int64
	flushCount    int64
	startTime     = time.Now()
)

func main() {
	log.SetPrefix("[fake-sink] ")
	log.SetFlags(0)

	token := os.Getenv("URUTAU_TOKEN")
	if token == "" {
		log.Fatal("URUTAU_TOKEN required")
	}
	configPath := os.Getenv("URUTAU_CONFIG")
	if configPath == "" {
		log.Fatal("URUTAU_CONFIG required")
	}
	bind := os.Getenv("URUTAU_BIND")
	socket := os.Getenv("URUTAU_SOCKET")
	if bind == "" && socket == "" {
		log.Fatal("one of URUTAU_SOCKET or URUTAU_BIND required")
	}
	if bind != "" && socket != "" {
		log.Fatal("only one of URUTAU_SOCKET or URUTAU_BIND allowed")
	}

	_ = os.Getenv("URUTAU_STAGE")
	_ = os.Getenv("URUTAU_PLUGIN_DIR")
	_ = os.Getenv("URUTAU_PROTOCOL_VERSION")

	configBytes, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("read config: %v", err)
	}
	log.Printf("config loaded (%d bytes)", len(configBytes))

	var lis net.Listener
	if socket != "" {
		lis, err = net.Listen("unix", socket)
	} else {
		lis, err = net.Listen("tcp", bind)
	}
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	svc := &sinkService{token: token, mem: memory.NewGoAllocator()}

	grpcServer := grpc.NewServer()
	flight.RegisterFlightServiceServer(grpcServer, svc)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	ready := map[string]any{
		"ready":           true,
		"protocolVersion": contract.ProtocolVersion,
		"pid":             os.Getpid(),
	}
	if bind != "" {
		_, port, _ := net.SplitHostPort(lis.Addr().String())
		ready["port"] = port
	}
	b, _ := json.Marshal(ready)
	fmt.Println(string(b))

	lastHeartbeat = time.Now()
	go watchdog()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	log.Println("shutting down")
	grpcServer.GracefulStop()
	os.Exit(0)
}

func watchdog() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		mu.Lock()
		elapsed := time.Since(lastHeartbeat)
		mu.Unlock()
		if elapsed > 15*time.Second {
			log.Println("no heartbeat for 15s, exiting")
			os.Exit(1)
		}
	}
}

type sinkService struct {
	flight.BaseFlightServer
	token string
	mem   memory.Allocator
}

func (s *sinkService) Handshake(stream flight.FlightService_HandshakeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if string(req.Payload) != s.token {
		return status.Errorf(codes.Unauthenticated, "UNAUTHENTICATED")
	}
	return stream.Send(&flight.HandshakeResponse{Payload: []byte{}})
}

func (s *sinkService) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}

	switch action.Type {
	case "urutau.heartbeat":
		mu.Lock()
		lastHeartbeat = time.Now()
		mu.Unlock()
		return stream.Send(&flight.Result{})

	case "urutau.flush":
		mu.Lock()
		flushCount++
		w := writeCount
		mu.Unlock()
		log.Printf("flush #%d (writes: %d)", flushCount, w)
		return stream.Send(&flight.Result{})

	case "urutau.status":
		mu.Lock()
		uptime := time.Since(startTime)
		mu.Unlock()
		resp := contract.StatusResponse{
			State:     "streaming",
			UptimeSec: int64(uptime.Seconds()),
			Tables:    map[string]contract.TableStatus{},
		}
		b, _ := json.Marshal(resp)
		return stream.Send(&flight.Result{Body: b})

	case "urutau.shutdown":
		go func() {
			time.Sleep(time.Second)
			os.Exit(0)
		}()
		return stream.Send(&flight.Result{})

	default:
		return status.Errorf(codes.Unimplemented, "unknown action: %s", action.Type)
	}
}

func (s *sinkService) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}
	actions := []*flight.ActionType{
		{Type: "urutau.list_tables", Description: "List available tables"},
		{Type: "urutau.status", Description: "Plugin status"},
		{Type: "urutau.heartbeat", Description: "Keepalive"},
		{Type: "urutau.flush", Description: "Flush durable writes"},
		{Type: "urutau.shutdown", Description: "Graceful shutdown"},
	}
	for _, a := range actions {
		if err := stream.Send(a); err != nil {
			return err
		}
	}
	return nil
}

func (s *sinkService) DoPut(stream flight.FlightService_DoPutServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}

	// Read schema from first message
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.DataHeader == nil {
		return status.Errorf(codes.InvalidArgument, "first message must carry schema in DataHeader")
	}

	schema, err := flight.DeserializeSchema(first.DataHeader, s.mem)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid schema: %v", err)
	}
	log.Printf("DoPut: received schema with %d fields", schema.NumFields())

	// Process records from DataBody
	if first.DataBody != nil {
		reader, err := ipc.NewReader(bytes.NewReader(first.DataBody))
		if err == nil {
			for {
				rec, err := reader.Read()
				if err != nil {
					break
				}
				mu.Lock()
				writeCount += rec.NumRows()
				mu.Unlock()
				log.Printf("DoPut: wrote %d rows (total: %d)", rec.NumRows(), writeCount)
				rec.Release()
			}
		}
	}

	// Continue reading more messages
	for {
		msg, err := stream.Recv()
		if err != nil {
			break
		}
		if msg.DataBody != nil {
			reader, err := ipc.NewReader(bytes.NewReader(msg.DataBody))
			if err == nil {
				for {
					rec, err := reader.Read()
					if err != nil {
						break
					}
					mu.Lock()
					writeCount += rec.NumRows()
					mu.Unlock()
					log.Printf("DoPut: wrote %d rows (total: %d)", rec.NumRows(), writeCount)
					rec.Release()
				}
			}
		}
	}

	return nil
}

func (s *sinkService) verifyBearer(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "UNAUTHENTICATED")
	}
	auth := md.Get("authorization")
	if len(auth) == 0 {
		return status.Errorf(codes.Unauthenticated, "UNAUTHENTICATED")
	}
	token := strings.TrimPrefix(auth[0], "Bearer ")
	if token != s.token {
		return status.Errorf(codes.Unauthenticated, "UNAUTHENTICATED")
	}
	return nil
}
