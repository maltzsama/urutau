package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// chunkExecutor runs DBLog chunk SELECTs against the source (design §11.1:
// the worker owns the snapshot query connections). The coordinator sends
// ChunkRequest bounds; the executor scans the half-open PK range and feeds
// the rows into the window builder, then reports ChunkReady.
type chunkExecutor struct {
	kind     string
	dsn      string
	chunkSz  int
	epoch    uint64                         // the Assignment epoch; echoed on ChunkReady so a stale reply is ignored
	bySource map[string]*pb.TableAssignment // source table → target/PK
	qsrc     source.QuerySource
	w        *Worker
	log      *slog.Logger
	send     func(*pb.WorkerMessage) error // session sender, serialized
}

func newChunkExecutor(assign *pb.Assignment, w *Worker, log *slog.Logger, send func(*pb.WorkerMessage) error) *chunkExecutor {
	bySource := make(map[string]*pb.TableAssignment, len(assign.Tables))
	for _, ta := range assign.Tables {
		bySource[ta.SourceTable] = ta
	}
	return &chunkExecutor{
		kind:     assign.SourceKind,
		dsn:      assign.SourceDsn,
		chunkSz:  int(assign.ChunkSize),
		epoch:    assign.Epoch,
		bySource: bySource,
		w:        w,
		log:      log,
		send:     send,
	}
}

// querySource opens the source's SQL surface on first use. The worker holds
// only kind + dsn from the assignment, so it builds a minimal spec and
// resolves the driver through the registry. Tests preset x.qsrc to bypass
// the driver registry.
func (x *chunkExecutor) querySource(ctx context.Context) (source.QuerySource, error) {
	if x.qsrc != nil {
		return x.qsrc, nil
	}
	src, err := driver.OpenSource(&spec.Spec{Source: spec.Source{Kind: x.kind, URI: x.dsn}}, source.Runtime{Logger: x.log})
	if err != nil {
		return nil, err
	}
	q, ok := src.(source.QuerySource)
	if !ok {
		return nil, fmt.Errorf("worker: source %q has no SQL query surface", x.kind)
	}
	x.qsrc = q
	return q, nil
}

// Close releases the source's query connection. The worker owns the executor
// for its whole life, so this is shutdown hygiene rather than a leak, but
// a dangling pool keeps sockets alive after the session ends.
func (x *chunkExecutor) Close() {
	if x.qsrc != nil {
		_ = x.qsrc.CloseQuery()
		x.qsrc = nil
	}
}

// run executes one chunk SELECT, feeds the window, and acks ChunkReady.
func (x *chunkExecutor) run(ctx context.Context, req *pb.ChunkRequest) error {
	ta, ok := x.bySource[req.Table]
	if !ok {
		return fmt.Errorf("worker: chunk for unassigned source table %s", req.Table)
	}
	if x.chunkSz <= 0 {
		x.chunkSz = 10000
	}
	q, err := x.querySource(ctx)
	if err != nil {
		return err
	}

	var low, high []any
	boundRows, err := transport.DecodeBounds(req.Bounds)
	if err != nil {
		return fmt.Errorf("worker: chunk %d bounds: %w", req.ChunkId, err)
	}
	if len(boundRows) > 0 {
		low = boundRows[0]
	}
	if len(boundRows) > 1 {
		high = boundRows[1]
	}

	chunker, err := q.NewChunker(req.Table, strings.Join(ta.PrimaryKey, ","), x.chunkSz)
	if err != nil {
		return err
	}

	rows := make([]rowchange.Change, 0, x.chunkSz)
	err = chunker.Scan(ctx, source.Chunk{Low: low, High: high}, func(row map[string]any) error {
		key := make([]any, 0, len(ta.PrimaryKey))
		for _, col := range ta.PrimaryKey {
			key = append(key, row[col])
		}
		rows = append(rows, rowchange.Change{
			Op:       rowchange.OpInsert,
			Table:    ta.TargetTable,
			Key:      key,
			After:    row,
			Snapshot: true,
			Phase:    core.PhaseSnapshot,
			IngestTS: time.Now(),
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("worker: chunk %d scan: %w", req.ChunkId, err)
	}

	// Build the record against the introspected schema (the worker's known
	// schema for this target), never a per-batch inference: window rows must
	// carry the stable table shape the sink expects. MergeSchema keeps that
	// shape and only appends columns a row carries that the schema lacks.
	// Snapshot chunk rows are inserts only, so the bridge C-8 delete guard
	// does not apply here.
	rec, err := transport.RecordFromChanges(rows, transport.MergeSchema(rows, x.w.KnownSchema(ta.TargetTable)), nil)
	if err != nil {
		return fmt.Errorf("worker: chunk %d encode: %w", req.ChunkId, err)
	}
	dpb := &dataplane.Batch{Table: ta.TargetTable, Record: rec, Mode: dataplane.AppendMode}
	// AddWindowRows takes ownership of the batch (the window stores it).
	if err := x.w.AddWindowRows(ta.TargetTable, req.ChunkId, dpb); err != nil {
		return err
	}
	return x.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_ChunkReady{ChunkReady: &pb.ChunkReady{
		Table:           req.Table,
		ChunkId:         req.ChunkId,
		Rows:            uint64(len(rows)),
		DroppedByWindow: uint64(x.w.DroppedByWindow(ta.TargetTable)),
		Epoch:           x.epoch,
	}}})
}
