package main

import (
	"bytes"
	"context"
	"encoding/base64"
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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	tableSchema = arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "qty", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	mu            sync.Mutex
	nextOffset    int64
	lastHeartbeat time.Time
	startTime     = time.Now()
	tableOffsets  = map[string]string{}
)

func main() {
	log.SetPrefix("[fake-source] ")
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

	svc := &sourceService{token: token, mem: memory.NewGoAllocator()}

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

type sourceService struct {
	flight.BaseFlightServer
	token string
	mem   memory.Allocator
}

func (s *sourceService) Handshake(stream flight.FlightService_HandshakeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	if string(req.Payload) != s.token {
		return status.Errorf(codes.Unauthenticated, "UNAUTHENTICATED")
	}
	return stream.Send(&flight.HandshakeResponse{Payload: []byte{}})
}

func (s *sourceService) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}

	switch action.Type {
	case "urutau.heartbeat":
		mu.Lock()
		lastHeartbeat = time.Now()
		mu.Unlock()
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
		mu.Lock()
		uptime := time.Since(startTime)
		state := "streaming"
		tables := make(map[string]contract.TableStatus)
		for t, o := range tableOffsets {
			tables[t] = contract.TableStatus{Offset: o}
		}
		mu.Unlock()
		resp := contract.StatusResponse{
			State:     state,
			Tables:    tables,
			UptimeSec: int64(uptime.Seconds()),
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

func (s *sourceService) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}
	actions := []*flight.ActionType{
		{Type: "urutau.list_tables", Description: "List available tables"},
		{Type: "urutau.status", Description: "Plugin status"},
		{Type: "urutau.heartbeat", Description: "Keepalive"},
		{Type: "urutau.shutdown", Description: "Graceful shutdown"},
	}
	for _, a := range actions {
		if err := stream.Send(a); err != nil {
			return err
		}
	}
	return nil
}

func (s *sourceService) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := s.verifyBearer(ctx); err != nil {
		return nil, err
	}

	var req contract.GetFlightInfoRequest
	if err := json.Unmarshal(desc.GetCmd(), &req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid descriptor: %v", err)
	}

	if req.Table != "orders" {
		return nil, status.Errorf(codes.NotFound, "NOT_FOUND: table %q", req.Table)
	}
	if req.Mode != "snapshot" && req.Mode != "changes" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid mode: %s", req.Mode)
	}

	var schema *arrow.Schema
	var appMeta []byte

	if req.Mode == "snapshot" {
		schema = tableSchema
		endOffset := base64.StdEncoding.EncodeToString([]byte("snapshot-end-001"))
		meta := contract.FlightInfoResponse{EndOffset: endOffset}
		appMeta, _ = json.Marshal(meta)
		mu.Lock()
		tableOffsets["orders"] = endOffset
		mu.Unlock()
	} else {
		schema = contract.CDCRecordSchema(tableSchema.Fields())
		meta := contract.FlightInfoResponse{EstimatedLag: ptrInt64(0)}
		appMeta, _ = json.Marshal(meta)
	}

	ticketBytes, _ := json.Marshal(map[string]string{"table": req.Table, "mode": req.Mode})

	return &flight.FlightInfo{
		Schema: flight.SerializeSchema(schema, s.mem),
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: ticketBytes}},
		},
		AppMetadata: appMeta,
	}, nil
}

func ptrInt64(v int64) *int64 { return &v }

func (s *sourceService) DoGet(req *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	if err := s.verifyBearer(stream.Context()); err != nil {
		return err
	}

	var ticketReq struct {
		Table string `json:"table"`
		Mode  string `json:"mode"`
	}
	if err := json.Unmarshal(req.Ticket, &ticketReq); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid ticket")
	}

	if ticketReq.Mode == "snapshot" {
		return s.streamSnapshot(stream)
	}
	return s.streamChanges(stream)
}

func (s *sourceService) streamSnapshot(stream flight.FlightService_DoGetServer) error {
	// Send schema header
	if err := stream.Send(&flight.FlightData{
		DataHeader: flight.SerializeSchema(tableSchema, s.mem),
	}); err != nil {
		return err
	}

	// Send 3 rows
	for i := 0; i < 3; i++ {
		builder := array.NewRecordBuilder(s.mem, tableSchema)
		builder.Field(0).(*array.Int64Builder).Append(int64(i + 1))
		builder.Field(1).(*array.Int32Builder).Append(int32((i + 1) * 10))
		rec := builder.NewRecord()

		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(tableSchema), ipc.WithAllocator(s.mem))
		if err := writer.Write(rec); err != nil {
			rec.Release()
			return err
		}
		writer.Close()

		if err := stream.Send(&flight.FlightData{
			DataBody: buf.Bytes(),
		}); err != nil {
			rec.Release()
			return err
		}
		rec.Release()
		builder.Release()
	}
	return nil
}

func (s *sourceService) streamChanges(stream flight.FlightService_DoGetServer) error {
	cdcSchema := contract.CDCRecordSchema(tableSchema.Fields())

	// Send schema header
	if err := stream.Send(&flight.FlightData{
		DataHeader: flight.SerializeSchema(cdcSchema, s.mem),
	}); err != nil {
		return err
	}

	// Simulate changes
	changes := []struct {
		op     string
		before *struct{ id, qty int64 }
		after  *struct{ id, qty int64 }
	}{
		{op: "u", before: &struct{ id, qty int64 }{1, 10}, after: &struct{ id, qty int64 }{1, 15}},
		{op: "d", before: &struct{ id, qty int64 }{2, 20}, after: nil},
		{op: "c", before: nil, after: &struct{ id, qty int64 }{4, 40}},
	}

	for _, ch := range changes {
		mu.Lock()
		nextOffset++
		offset := base64.StdEncoding.EncodeToString([]byte{byte(nextOffset)})
		tableOffsets["orders"] = offset
		mu.Unlock()

		rec := newCDCRecord(s.mem, cdcSchema, ch.op, ch.before, ch.after, []byte(offset))
		var buf bytes.Buffer
		writer := ipc.NewWriter(&buf, ipc.WithSchema(cdcSchema), ipc.WithAllocator(s.mem))
		if err := writer.Write(rec); err != nil {
			rec.Release()
			return err
		}
		writer.Close()
		rec.Release()

		if err := stream.Send(&flight.FlightData{
			DataBody: buf.Bytes(),
		}); err != nil {
			return err
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Keep streaming empty batches (liveness signal, CONTRACT §8.3)
	for {
		time.Sleep(60 * time.Second)
	}
}

func newCDCRecord(mem memory.Allocator, schema *arrow.Schema, op string, before, after *struct{ id, qty int64 }, offset []byte) arrow.Record {
	bld := array.NewRecordBuilder(mem, schema)
	defer bld.Release()

	bld.Field(0).(*array.StringBuilder).Append(op)

	if before != nil {
		bld.Field(1).(*array.StructBuilder).Append(true)
		bld.Field(1).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(before.id)
		bld.Field(1).(*array.StructBuilder).FieldBuilder(1).(*array.Int32Builder).Append(int32(before.qty))
	} else {
		bld.Field(1).(*array.StructBuilder).AppendNull()
	}

	if after != nil {
		bld.Field(2).(*array.StructBuilder).Append(true)
		bld.Field(2).(*array.StructBuilder).FieldBuilder(0).(*array.Int64Builder).Append(after.id)
		bld.Field(2).(*array.StructBuilder).FieldBuilder(1).(*array.Int32Builder).Append(int32(after.qty))
	} else {
		bld.Field(2).(*array.StructBuilder).AppendNull()
	}

	bld.Field(3).(*array.BinaryBuilder).Append(offset)
	ts, _ := arrow.TimestampFromTime(time.Now(), arrow.Microsecond)
	bld.Field(4).(*array.TimestampBuilder).Append(ts)

	return bld.NewRecord()
}

func (s *sourceService) verifyBearer(ctx context.Context) error {
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
