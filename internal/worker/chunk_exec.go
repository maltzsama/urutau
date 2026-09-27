package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
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
	postgres []byte // JSON spec.PostgresSource (#170); empty = use dsn
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
		postgres: assign.Postgres,
		chunkSz:  int(assign.ChunkSize),
		epoch:    assign.Epoch,
		bySource: bySource,
		w:        w,
		log:      log,
		send:     send,
	}
}

// querySource opens the source's SQL surface on first use. The worker holds
// kind + dsn from the assignment, plus each table's source read projection
// (#162 column list, #163 filter) — reconstructed into a spec so the source
// resolves the chunk SELECT the same way the coordinator's does. When the
// assignment carries the structured postgres block (#170), that block is used
// instead of the DSN: an SSH tunnel is a DialFunc, not a DSN. Tests preset
// x.qsrc to bypass the driver registry.
func (x *chunkExecutor) querySource(ctx context.Context) (source.QuerySource, error) {
	if x.qsrc != nil {
		return x.qsrc, nil
	}
	tables, err := specTablesFromAssignment(x.bySource)
	if err != nil {
		return nil, err
	}
	srcSpec, err := sourceSpecFor(x.kind, x.dsn, x.postgres)
	if err != nil {
		return nil, err
	}
	src, err := driver.OpenSource(&spec.Spec{
		Source: srcSpec,
		Tables: tables,
	}, source.Runtime{Logger: x.log})
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

// sourceSpecFor builds the source config the worker opens its snapshot
// connection from: the structured postgres block when the assignment carries
// one (#170 — an SSH tunnel is a DialFunc, not a DSN), else kind + DSN.
func sourceSpecFor(kind, dsn string, postgres []byte) (spec.Source, error) {
	if len(postgres) == 0 {
		return spec.Source{Kind: kind, URI: dsn}, nil
	}
	// A postgres block only makes sense for a postgres source; a mismatched
	// kind would build a nonsensical spec the driver then mis-handles
	// (issue #268).
	if kind != "postgres" {
		return spec.Source{}, fmt.Errorf("worker: postgres config set for non-postgres source kind %q", kind)
	}
	var pg spec.PostgresSource
	if err := json.Unmarshal(postgres, &pg); err != nil {
		return spec.Source{}, fmt.Errorf("worker: postgres config: %w", err)
	}
	return spec.Source{Kind: kind, Postgres: &pg}, nil
}

// specTablesFromAssignment reconstructs the per-table spec the source needs
// to resolve the snapshot projection (#162 column list, #163 filter), which
// the coordinator shipped in the assignment.
func specTablesFromAssignment(bySource map[string]*pb.TableAssignment) ([]spec.Table, error) {
	tables := make([]spec.Table, 0, len(bySource))
	for _, ta := range bySource {
		t := spec.Table{Source: ta.SourceTable, ColumnFilter: ta.ColumnFilter, ChunkColumn: ta.ChunkColumn}
		if len(ta.Filter) > 0 {
			var f spec.Filter
			if err := json.Unmarshal(ta.Filter, &f); err != nil {
				return nil, fmt.Errorf("worker: filter %s: %w", ta.SourceTable, err)
			}
			t.Filter = &f
		}
		tables = append(tables, t)
	}
	return tables, nil
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
	// The coordinator prevents a keyless assignment (requirePartitionKey), but
	// the worker must not depend on that: an empty key joins to "" and
	// NewChunker fails with a cryptic message (issue #271).
	if len(ta.PrimaryKey) == 0 {
		return fmt.Errorf("worker: chunk for %s: the assignment carries no primary key", req.Table)
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

	rec, rows, err := scanChunkRecord(ctx, chunker, source.Chunk{Low: low, High: high}, ta, x.w.KnownSchema(ta.TargetTable))
	if err != nil {
		return fmt.Errorf("worker: chunk %d: %w", req.ChunkId, err)
	}
	dpb := &dataplane.Batch{Table: ta.TargetTable, Record: rec, Mode: dataplane.AppendMode}
	// AddWindowRows takes ownership of the batch (the window stores it).
	if err := x.w.AddWindowRows(ta.TargetTable, req.ChunkId, dpb); err != nil {
		return err
	}
	return x.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_ChunkReady{ChunkReady: &pb.ChunkReady{
		Table:           req.Table,
		ChunkId:         req.ChunkId,
		Rows:            uint64(rows),
		DroppedByWindow: uint64(x.w.DroppedByWindow(ta.TargetTable)),
		Epoch:           x.epoch,
	}}})
}
