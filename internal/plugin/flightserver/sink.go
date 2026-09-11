package flightserver

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/plugin/contract"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/sink"
)

// SinkServer serves the plugin contract's sink RPC (DoPut) over a real
// sink.Sink, so the existing internal/plugin/client + internal/plugin.
// SinkAdapter can connect to it exactly as they would a subprocess plugin.
// It expects the plugin sink record shape internal/plugin/sink.go's
// recordsFromReader produces: op + stringified data columns + offset +
// ts_source, all in the DESCRIPTOR's table.
type SinkServer struct {
	flight.BaseFlightServer
	heartbeatState

	snk    sink.Sink
	alloc  memory.Allocator
	mu     writerCache
	logger func(format string, args ...any)
}

// NewSinkServer wraps snk for in-process Flight serving.
func NewSinkServer(snk sink.Sink) *SinkServer {
	return &SinkServer{
		snk:   snk,
		alloc: memory.NewGoAllocator(),
		mu:    writerCache{writers: map[string]sink.TableWriter{}},
	}
}

func (s *SinkServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&flight.HandshakeResponse{Payload: []byte{}})
}

func (s *SinkServer) ListActions(_ *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	for _, t := range []string{"urutau.heartbeat", "urutau.status", "urutau.list_tables", "urutau.flush", "urutau.shutdown"} {
		if err := stream.Send(&flight.ActionType{Type: t}); err != nil {
			return err
		}
	}
	return nil
}

func (s *SinkServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	switch action.Type {
	case "urutau.heartbeat":
		return stream.Send(&flight.Result{})
	case "urutau.status":
		b, _ := json.Marshal(contract.StatusResponse{State: "streaming", UptimeSec: 0, Tables: map[string]contract.TableStatus{}})
		return stream.Send(&flight.Result{Body: b})
	case "urutau.list_tables":
		b, _ := json.Marshal(contract.ListTablesResponse{})
		return stream.Send(&flight.Result{Body: b})
	case "urutau.flush":
		// The wrapped sink.Sink commits durably inside TableWriter.Commit
		// (Iceberg and friends have no separate flush step); nothing to do.
		return stream.Send(&flight.Result{})
	case "urutau.shutdown":
		s.mu.closeAll()
		return stream.Send(&flight.Result{})
	default:
		return status.Errorf(codes.Unimplemented, "flightserver: unknown action %q", action.Type)
	}
}

// DoPut receives one plugin-shaped record stream (contract §10: op +
// stringified columns + offset + ts_source) and re-projects it into a
// dataplane.Batch committed through the wrapped sink.Sink's TableWriter.
func (s *SinkServer) DoPut(stream flight.FlightService_DoPutServer) error {
	rdr, err := flight.NewRecordReader(stream, ipc.WithAllocator(s.alloc))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "flightserver: doput reader: %v", err)
	}
	defer rdr.Release()

	desc := rdr.LatestFlightDescriptor()
	if desc == nil {
		return status.Error(codes.InvalidArgument, "flightserver: doput missing descriptor")
	}
	var req contract.DoPutRequest
	if err := json.Unmarshal(desc.GetCmd(), &req); err != nil {
		return status.Errorf(codes.InvalidArgument, "flightserver: invalid descriptor: %v", err)
	}
	if req.Table == "" {
		return status.Error(codes.InvalidArgument, "flightserver: doput descriptor missing table")
	}

	w, err := s.mu.writerFor(stream.Context(), s.snk, req.Table)
	if err != nil {
		return status.Errorf(codes.Internal, "flightserver: writer %s: %v", req.Table, err)
	}

	for rdr.Next() {
		rec := rdr.RecordBatch()
		wire, err := pluginRecordToWire(rec, s.alloc)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "flightserver: %v", err)
		}
		if wire == nil {
			continue
		}
		b := &dataplane.Batch{Table: req.Table, Record: wire, Mode: dataplane.UpsertMode}
		if err := w.Commit(stream.Context(), b); err != nil {
			return status.Errorf(codes.Internal, "flightserver: commit %s: %v", req.Table, err)
		}
	}
	if err := rdr.Err(); err != nil {
		return status.Errorf(codes.Internal, "flightserver: doput read: %v", err)
	}
	return stream.Send(&flight.PutResult{})
}

// writerCache keeps one TableWriter per table for the sink's lifetime — the
// plugin sink contract commits per DoPut call, but the wrapped sink.Sink's
// Writer() is meant to be opened once per table and reused (Close releases
// on shutdown).
type writerCache struct {
	writers map[string]sink.TableWriter
}

func (c *writerCache) writerFor(ctx context.Context, snk sink.Sink, table string) (sink.TableWriter, error) {
	if w, ok := c.writers[table]; ok {
		return w, nil
	}
	ref := core.TableRef{Target: table}
	w, err := snk.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		return nil, err
	}
	c.writers[table] = w
	return w, nil
}

func (c *writerCache) closeAll() {
	for _, w := range c.writers {
		_ = w.Close()
	}
}

// pluginRecordToWire re-projects one plugin sink record (op + stringified
// data columns + offset + ts_source, per internal/plugin/sink.go's
// recordsFromReader) into the flat urutau wire schema. Every non-metadata
// column arrives as a nullable Utf8 string; RecordFromChanges re-infers the
// canonical Go type per rowchange.Change the same way a live source would.
func pluginRecordToWire(rec arrow.RecordBatch, alloc memory.Allocator) (arrow.RecordBatch, error) {
	schema := rec.Schema()
	n := int(rec.NumRows())
	if n == 0 {
		return nil, nil
	}

	opIdx := fieldIndex(schema, "op")
	offsetIdx := fieldIndex(schema, "offset")
	tsIdx := fieldIndex(schema, "ts_source")
	if opIdx < 0 || offsetIdx < 0 {
		return nil, fmt.Errorf("plugin sink record missing op/offset column")
	}

	var dataCols []string
	for i := range schema.NumFields() {
		name := schema.Field(i).Name
		if name == "op" || name == "offset" || name == "ts_source" {
			continue
		}
		dataCols = append(dataCols, name)
	}

	changes := make([]rowchange.Change, 0, n)
	for i := range n {
		op := rec.Column(opIdx).(*array.String).Value(i)
		var rop rowchange.Op
		switch op {
		case "c":
			rop = rowchange.OpInsert
		case "d":
			rop = rowchange.OpDelete
		default:
			rop = rowchange.OpUpdate
		}
		offset := string(rec.Column(offsetIdx).(*array.Binary).Value(i))
		var ts time.Time
		if tsIdx >= 0 {
			tsCol := rec.Column(tsIdx).(*array.Timestamp)
			if !tsCol.IsNull(i) {
				ts = tsCol.Value(i).ToTime(arrow.Microsecond).UTC()
			}
		}

		row := make(map[string]any, len(dataCols))
		for _, name := range dataCols {
			idx := fieldIndex(schema, name)
			col := rec.Column(idx)
			if col.IsNull(i) {
				row[name] = nil
				continue
			}
			row[name] = col.(*array.String).Value(i)
		}

		var chg rowchange.Change
		chg.Op = rop
		chg.Position = offset
		chg.CommitTS = ts
		if rop == rowchange.OpDelete {
			chg.Before = row
		} else {
			chg.After = row
		}
		changes = append(changes, chg)
	}

	cs := transport.MergeSchema(changes, core.Schema{})
	wire, err := transport.RecordFromChanges(changes, cs, alloc)
	if err != nil {
		return nil, err
	}
	return wire, nil
}

func fieldIndex(s *arrow.Schema, name string) int {
	for i := range s.NumFields() {
		if s.Field(i).Name == name {
			return i
		}
	}
	return -1
}
