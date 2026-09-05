package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/maltzsama/urutau/change"
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
	chunkSz  int
	bySource map[string]*pb.TableAssignment // source table → target/PK
	qsrc     source.QuerySource
	db       *sql.DB
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
		bySource: bySource,
		w:        w,
		log:      log,
		send:     send,
	}
}

// querySource opens the source's SQL surface on first use. The worker holds
// only kind + dsn from the assignment, so it builds a minimal spec and
// resolves the driver through the registry.
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

// queryDB opens the source query connection on first use (snapshot only;
// the design gives workers query conns precisely for this).
func (x *chunkExecutor) queryDB(ctx context.Context) (*sql.DB, error) {
	if x.db != nil {
		return x.db, nil
	}
	q, err := x.querySource(ctx)
	if err != nil {
		return nil, err
	}
	db, err := q.OpenQuery(ctx)
	if err != nil {
		return nil, err
	}
	x.db = db
	return db, nil
}

// Close releases the query connection pool. The worker owns the executor
// for its whole life, so this is shutdown hygiene rather than a leak, but
// a dangling pool keeps sockets alive after the session ends.
func (x *chunkExecutor) Close() {
	if x.db != nil {
		_ = x.db.Close()
		x.db = nil
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
	db, err := x.queryDB(ctx)
	if err != nil {
		return fmt.Errorf("worker: chunk query db: %w", err)
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

	chunker, err := q.NewChunker(db, req.Table, strings.Join(ta.PrimaryKey, ","), x.chunkSz)
	if err != nil {
		return err
	}

	rows := make([]change.Change, 0, x.chunkSz)
	err = chunker.Scan(ctx, source.Chunk{Low: low, High: high}, func(row map[string]any) error {
		key := make([]any, 0, len(ta.PrimaryKey))
		for _, col := range ta.PrimaryKey {
			key = append(key, row[col])
		}
		rows = append(rows, change.Change{
			Op:       change.OpInsert,
			Table:    ta.TargetTable,
			Key:      key,
			After:    row,
			Snapshot: true,
			IngestTS: time.Now(),
		})
		return nil
	})
	if err != nil {
		return fmt.Errorf("worker: chunk %d scan: %w", req.ChunkId, err)
	}

	if err := x.w.AddWindowRows(ta.TargetTable, req.ChunkId, rows); err != nil {
		return err
	}
	return x.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_ChunkReady{ChunkReady: &pb.ChunkReady{
		Table:           req.Table,
		ChunkId:         req.ChunkId,
		Rows:            uint64(len(rows)),
		DroppedByWindow: uint64(x.w.DroppedByWindow(ta.TargetTable)),
	}}})
}
