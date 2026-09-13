package flightserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"

	"bytes"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// SourceServer serves the plugin contract's source RPCs (GetFlightInfo +
// DoGet) over a real source.Source, so the existing internal/plugin/client
// + internal/plugin.SourceAdapter can connect to it exactly as they would a
// subprocess plugin.
type SourceServer struct {
	flight.BaseFlightServer

	src   source.Source
	spec  *spec.Spec
	rt    source.Runtime
	alloc memory.Allocator
}

// NewSourceServer wraps src for in-process Flight serving.
func NewSourceServer(src source.Source, sp *spec.Spec, rt source.Runtime) *SourceServer {
	return &SourceServer{src: src, spec: sp, rt: rt, alloc: memory.NewGoAllocator()}
}

func (s *SourceServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	// The auth interceptor already authenticated the transport; the token
	// on the wire here is not re-checked (contract auth is unix-socket +
	// bearer, both already enforced at the interceptor). Accept once.
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&flight.HandshakeResponse{Payload: []byte{}})
}

func (s *SourceServer) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	for _, t := range []string{"urutau.heartbeat", "urutau.status", "urutau.list_tables", "urutau.shutdown"} {
		if err := stream.Send(&flight.ActionType{Type: t}); err != nil {
			return err
		}
	}
	return nil
}

func (s *SourceServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case "urutau.heartbeat":
		return stream.Send(&flight.Result{})
	case "urutau.status":
		b, _ := json.Marshal(contract.StatusResponse{State: "streaming", UptimeSec: 0, Tables: map[string]contract.TableStatus{}})
		return stream.Send(&flight.Result{Body: b})
	case "urutau.list_tables":
		var tables []contract.TableInfo
		if s.spec != nil {
			for _, t := range s.spec.Tables {
				tables = append(tables, contract.TableInfo{Name: t.Source, SupportsSnapshot: true})
			}
		}
		b, _ := json.Marshal(contract.ListTablesResponse{Tables: tables})
		return stream.Send(&flight.Result{Body: b})
	case "urutau.shutdown":
		return stream.Send(&flight.Result{})
	default:
		return status.Errorf(codes.Unimplemented, "flightserver: unknown action %q", action.Type)
	}
}

// ticketPayload is the opaque ticket GetFlightInfo issues and DoGet
// decodes: the table name plus which schema shape to serve, so DoGet does
// not need a side channel to recover the request mode.
type ticketPayload struct {
	Table string `json:"table"`
	Mode  string `json:"mode"`
}

// GetFlightInfo resolves the schema for one table via source.Source's
// Introspect. Per contract §6/§7/§8: snapshot mode announces the flat
// table schema; changes mode announces the CDC change-record schema
// (§8.1, before/after struct-shaped).
func (s *SourceServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	var req contract.GetFlightInfoRequest
	if err := json.Unmarshal(desc.GetCmd(), &req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "flightserver: invalid descriptor: %v", err)
	}

	tbl, ok := s.tableByName(req.Table)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "flightserver: unknown table %q", req.Table)
	}

	_, cs, _, err := s.src.Introspect(ctx, tbl)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "flightserver: introspect %s: %v", req.Table, err)
	}

	var wireSchema *arrow.Schema
	switch req.Mode {
	case "changes":
		tableFields, err := coreSchemaToArrowFields(cs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "flightserver: schema %s: %v", req.Table, err)
		}
		wireSchema = contract.CDCRecordSchema(tableFields)
	default: // "snapshot"
		tableFields, err := coreSchemaToArrowFields(cs)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "flightserver: schema %s: %v", req.Table, err)
		}
		wireSchema = arrow.NewSchema(tableFields, nil)
	}

	tkt, err := json.Marshal(ticketPayload{Table: req.Table, Mode: req.Mode})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "flightserver: encode ticket: %v", err)
	}
	appMeta, _ := json.Marshal(struct {
		EndOffset string `json:"endOffset"`
	}{})

	return &flight.FlightInfo{
		Schema: flight.SerializeSchema(wireSchema, s.alloc),
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: tkt},
		}},
		AppMetadata: appMeta,
	}, nil
}

// DoGet streams the table's rows in the shape GetFlightInfo announced for
// the ticket's mode: flat rows for snapshot, CDC before/after records for
// changes. It opens the wrapped source.Source's Reader once and re-projects
// every dataplane.Batch (flat wire schema) accordingly.
func (s *SourceServer) DoGet(tkt *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	var tp ticketPayload
	if err := json.Unmarshal(tkt.Ticket, &tp); err != nil {
		return status.Errorf(codes.InvalidArgument, "flightserver: invalid ticket: %v", err)
	}
	tbl, ok := s.tableByName(tp.Table)
	if !ok {
		return status.Errorf(codes.NotFound, "flightserver: unknown table %q", tp.Table)
	}
	ctx := stream.Context()

	ref, cs, _, err := s.src.Introspect(ctx, tbl)
	if err != nil {
		return status.Errorf(codes.Internal, "flightserver: introspect %s: %v", tp.Table, err)
	}

	var (
		wireSchema  *arrow.Schema
		tableFields []arrow.Field
		changesMode = tp.Mode == "changes"
	)
	if changesMode {
		tableFields, err = coreSchemaToArrowFields(cs)
		if err != nil {
			return status.Errorf(codes.Internal, "flightserver: schema %s: %v", tp.Table, err)
		}
		wireSchema = contract.CDCRecordSchema(tableFields)
	} else {
		tableFields, err = coreSchemaToArrowFields(cs)
		if err != nil {
			return status.Errorf(codes.Internal, "flightserver: schema %s: %v", tp.Table, err)
		}
		wireSchema = arrow.NewSchema(tableFields, nil)
	}

	w := &doGetWriter{stream: stream, schema: wireSchema, alloc: s.alloc}
	if err := w.sendSchema(); err != nil {
		return err
	}

	rdr, err := s.src.Open(ctx, []core.TableRef{ref})
	if err != nil {
		return status.Errorf(codes.Internal, "flightserver: open %s: %v", tp.Table, err)
	}
	defer rdr.Close()
	if err := rdr.Start(ctx, nil); err != nil {
		return status.Errorf(codes.Internal, "flightserver: start %s: %v", tp.Table, err)
	}

	for {
		b, err := rdr.Next(ctx)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return status.Errorf(codes.Internal, "flightserver: read %s: %v", tp.Table, err)
		}
		if b == nil {
			return nil
		}
		var werr error
		if changesMode {
			werr = w.writeChangeBatch(b, tableFields)
		} else {
			werr = w.writeFlatBatch(b)
		}
		b.Record.Release()
		if werr != nil {
			return werr
		}
	}
}

func (s *SourceServer) tableByName(name string) (spec.Table, bool) {
	if s.spec == nil {
		return spec.Table{}, false
	}
	for _, t := range s.spec.Tables {
		if t.Source == name {
			return t, true
		}
	}
	return spec.Table{}, false
}

// coreSchemaToArrowFields converts a canonical schema's data columns
// (excluding wire metadata, which the CDC contract carries separately as
// offset/ts_source) into the table fields CDCRecordSchema wraps in
// before/after structs.
func coreSchemaToArrowFields(cs core.Schema) ([]arrow.Field, error) {
	arrowSchema, err := transport.CoreSchemaToArrow(cs)
	if err != nil {
		return nil, err
	}
	// CoreSchemaToArrow appends the 6 wire metadata columns after the data
	// columns; the plugin contract's table/CDC schemas carry only the data
	// columns (metadata is represented separately as offset/ts_source).
	fields := make([]arrow.Field, len(cs.Columns))
	for i := range cs.Columns {
		fields[i] = arrowSchema.Field(i)
	}
	return fields, nil
}

// doGetWriter builds CDC-schema IPC frames from wire batches.
type doGetWriter struct {
	stream flight.FlightService_DoGetServer
	schema *arrow.Schema
	alloc  memory.Allocator
}

func (w *doGetWriter) sendSchema() error {
	return w.stream.Send(&flight.FlightData{DataHeader: flight.SerializeSchema(w.schema, w.alloc)})
}

// writeFlatBatch re-projects one flat wire dataplane.Batch into a plain
// data-columns-only record (contract §7 snapshot schema) and sends it.
func (w *doGetWriter) writeFlatBatch(b *dataplane.Batch) error {
	br, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "flightserver: batch reader: %v", err)
	}
	n := br.NumRows()
	if n == 0 {
		return nil
	}

	bld := array.NewRecordBuilder(w.alloc, w.schema)
	defer bld.Release()

	names := br.DataColumns()
	for i := range n {
		for fi, name := range names {
			v, ok := br.Value(name, i)
			appendScalar(bld.Field(fi), v, ok)
		}
	}

	rec := bld.NewRecordBatch()
	defer rec.Release()

	body, err := recordToIPCBytes(w.schema, rec, w.alloc)
	if err != nil {
		return err
	}
	return w.stream.Send(&flight.FlightData{DataBody: body})
}

// writeChangeBatch re-projects one flat wire dataplane.Batch into one CDC
// struct-shaped record and sends it as a single FlightData frame.
func (w *doGetWriter) writeChangeBatch(b *dataplane.Batch, tableFields []arrow.Field) error {
	br, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "flightserver: batch reader: %v", err)
	}
	n := br.NumRows()
	if n == 0 {
		return nil
	}

	bld := array.NewRecordBuilder(w.alloc, w.schema)
	defer bld.Release()

	opBld := bld.Field(0).(*array.StringBuilder)
	beforeBld := bld.Field(1).(*array.StructBuilder)
	afterBld := bld.Field(2).(*array.StructBuilder)
	offsetBld := bld.Field(3).(*array.BinaryBuilder)
	tsBld := bld.Field(4).(*array.TimestampBuilder)

	for i := range n {
		switch br.Op(i) {
		case rowchange.OpInsert:
			opBld.Append(contract.OpInsert)
			beforeBld.AppendNull()
			appendStructRow(afterBld, tableFields, br, i)
		case rowchange.OpUpdate:
			opBld.Append(contract.OpUpdate)
			beforeBld.AppendNull()
			appendStructRow(afterBld, tableFields, br, i)
		case rowchange.OpDelete:
			opBld.Append(contract.OpDelete)
			appendStructRow(beforeBld, tableFields, br, i)
			afterBld.AppendNull()
		default:
			opBld.Append(contract.OpInsert)
			beforeBld.AppendNull()
			appendStructRow(afterBld, tableFields, br, i)
		}
		offsetBld.Append([]byte(br.Position(i)))
		if ts, ok := br.CommitTS(i); ok {
			tsBld.Append(arrow.Timestamp(ts.UnixNano()))
		} else {
			tsBld.AppendNull()
		}
	}

	rec := bld.NewRecordBatch()
	defer rec.Release()

	body, err := recordToIPCBytes(w.schema, rec, w.alloc)
	if err != nil {
		return err
	}
	return w.stream.Send(&flight.FlightData{DataBody: body})
}

func appendStructRow(sb *array.StructBuilder, fields []arrow.Field, br *transport.BatchReader, row int) {
	sb.Append(true)
	for fi, f := range fields {
		v, ok := br.Value(f.Name, row)
		appendScalar(sb.FieldBuilder(fi), v, ok)
	}
}

func appendScalar(b array.Builder, v any, ok bool) {
	if !ok || v == nil {
		b.AppendNull()
		return
	}
	switch bb := b.(type) {
	case *array.BooleanBuilder:
		bb.Append(v.(bool))
	case *array.Int32Builder:
		bb.Append(toInt32(v))
	case *array.Int64Builder:
		bb.Append(toInt64(v))
	case *array.Uint64Builder:
		bb.Append(v.(uint64))
	case *array.Float32Builder:
		bb.Append(v.(float32))
	case *array.Float64Builder:
		bb.Append(v.(float64))
	case *array.StringBuilder:
		bb.Append(fmt.Sprintf("%v", v))
	case *array.BinaryBuilder:
		if bs, ok := v.([]byte); ok {
			bb.Append(bs)
		} else {
			bb.Append([]byte(fmt.Sprintf("%v", v)))
		}
	case *array.TimestampBuilder:
		if t, ok := v.(time.Time); ok {
			bb.Append(arrow.Timestamp(t.UnixMicro()))
		} else {
			bb.AppendNull()
		}
	default:
		b.AppendNull()
	}
}

func toInt32(v any) int32 {
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	case int:
		return int32(n)
	default:
		return 0
	}
}

// recordToIPCBytes serializes one record as a standalone IPC stream (schema
// message + one record message) — the exact format the client's
// ipc.NewReader(bytes.NewReader(fd.DataBody)) expects (see
// internal/plugin/source.go's readBatches).
func recordToIPCBytes(schema *arrow.Schema, rec arrow.RecordBatch, alloc memory.Allocator) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	if err := w.Write(rec); err != nil {
		return nil, status.Errorf(codes.Internal, "flightserver: ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "flightserver: ipc close: %v", err)
	}
	return buf.Bytes(), nil
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	default:
		return 0
	}
}
