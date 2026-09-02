package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/maltzsama/urutau/internal/adapter"
	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/source/dblog"
	"github.com/maltzsama/urutau/internal/source/mysql"
	"github.com/maltzsama/urutau/internal/source/postgres"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
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

// queryDB opens the source query connection on first use (snapshot only;
// the design gives workers query conns precisely for this).
func (x *chunkExecutor) queryDB(ctx context.Context) (*sql.DB, error) {
	if x.db != nil {
		return x.db, nil
	}
	db, err := adapter.OpenQueryDB(x.kind, x.dsn)
	if err != nil {
		return nil, err
	}
	x.db = db
	return db, nil
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
	db, err := x.queryDB(ctx)
	if err != nil {
		return fmt.Errorf("worker: chunk query db: %w", err)
	}

	var low, high []any
	if len(req.Low) > 0 {
		if err := json.Unmarshal(req.Low, &low); err != nil {
			return fmt.Errorf("worker: chunk low bounds %q: %w", req.Low, err)
		}
	}
	if len(req.High) > 0 {
		if err := json.Unmarshal(req.High, &high); err != nil {
			return fmt.Errorf("worker: chunk high bounds %q: %w", req.High, err)
		}
	}

	chunker, err := newChunkerFor(x.kind, db, req.Table, ta.PrimaryKey, x.chunkSz)
	if err != nil {
		return err
	}

	rows := make([]change.Change, 0, x.chunkSz)
	err = chunker.Scan(ctx, dblog.Chunk{Low: low, High: high}, func(row map[string]any) error {
		key := make([]any, 0, len(ta.PrimaryKey))
		for _, col := range ta.PrimaryKey {
			key = append(key, row[col])
		}
		rows = append(rows, change.Change{
			Op:    change.OpInsert,
			Table: ta.TargetTable,
			Key:   key,
			After: row,
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

func newChunkerFor(kind string, db *sql.DB, source string, pk []string, size int) (dblog.ChunkSource, error) {
	switch kind {
	case "mysql":
		return mysql.NewChunker(db, source, strings.Join(pk, ","), size)
	case "postgres":
		return postgres.NewChunker(db, source, strings.Join(pk, ","), size)
	default:
		return nil, fmt.Errorf("worker: chunker: unknown source kind %q", kind)
	}
}
