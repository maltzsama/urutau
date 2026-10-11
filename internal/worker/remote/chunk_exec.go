package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/internal/worker"
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
	slot     string // source replication slot (postgres); the window position is read from it
	chunkSz  int
	// perRow is each target's last chunk's bytes per row per column: the
	// next chunk's buffers are sized from it (readChunk).
	perRow map[string][]int
	// chunkers caches one ChunkSource per table, so NewChunker (which may
	// consult source metadata) runs once per table, not once per chunk
	// (issue #586). run is called from a single chunk-work goroutine, so the
	// map needs no lock.
	chunkers  map[string]source.ChunkSource
	epoch     uint64                         // the Assignment epoch; echoed on ChunkReady so a stale reply is ignored
	bySource  map[string]*pb.TableAssignment // source table → target/PK
	qsrc      source.QuerySource
	pos       source.Positioner // captures the source position after each window read (WindowOpen.pos)
	windowSeq atomic.Uint64     // the worker's monotonic window-id sequence
	w         *worker.Worker
	log       *slog.Logger
	send      func(*pb.WorkerMessage) error // session sender, serialized
}

func newChunkExecutor(assign *pb.Assignment, w *worker.Worker, log *slog.Logger, send func(*pb.WorkerMessage) error) *chunkExecutor {
	bySource := make(map[string]*pb.TableAssignment, len(assign.Tables))
	for _, ta := range assign.Tables {
		bySource[ta.SourceTable] = ta
	}
	return &chunkExecutor{
		kind:     assign.SourceKind,
		dsn:      assign.SourceDsn,
		postgres: assign.Postgres,
		slot:     assign.SlotName,
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
	srcSpec, err := sourceSpecFor(x.kind, x.dsn, x.postgres, x.slot)
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
	x.pos = src
	return q, nil
}

// sourceSpecFor builds the source config the worker opens its snapshot
// connection from: the structured postgres block when the assignment carries
// one (#170 — an SSH tunnel is a DialFunc, not a DSN), else kind + DSN. The
// slot name rides along in both forms: the source reads a window's position
// from the slot, and without the name that read fails.
func sourceSpecFor(kind, dsn string, postgres []byte, slot string) (spec.Source, error) {
	if len(postgres) == 0 {
		return spec.Source{Kind: kind, URI: dsn, SlotName: slot}, nil
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
	return spec.Source{Kind: kind, Postgres: &pg, SlotName: slot}, nil
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

// run executes one chunk SELECT, cuts it into byte-capped windows, and acks
// ChunkReady with the window ids it opened.
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

	chunker, err := x.chunkerFor(q, req.Table, ta)
	if err != nil {
		return err
	}
	ch := source.Chunk{Low: low, High: high}

	var total int
	var windowIDs []uint64
	emit := func(page arrow.RecordBatch, n int) error {
		return x.emitWindow(ctx, req, ta, page, n, &windowIDs)
	}
	if s, ok := chunker.(byteCapScanner); ok && len(x.w.KnownSchema(ta.TargetTable).Columns) > 0 {
		total, err = x.readChunkPages(ctx, s, ch, ta, emit)
	} else {
		// No byte-cap surface: one window for the whole chunk, as before.
		var rec arrow.RecordBatch
		rec, total, err = x.readChunk(ctx, chunker, ch, ta)
		if err == nil {
			err = emit(rec, total)
		}
	}
	if err != nil {
		return fmt.Errorf("worker: chunk %d: %w", req.ChunkId, err)
	}

	return x.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_ChunkReady{ChunkReady: &pb.ChunkReady{
		Table:           req.Table,
		ChunkId:         req.ChunkId,
		Rows:            uint64(total),
		DroppedByWindow: uint64(x.w.DroppedByWindow(ta.TargetTable)),
		Epoch:           x.epoch,
		WindowIds:       windowIDs,
	}}})
}

// chunkerFor returns the table's cached ChunkSource, building it once — the
// chunker depends only on the table's PK and projection, both fixed for the
// assignment, so rebuilding it per ChunkRequest is wasted metadata work
// (issue #586).
func (x *chunkExecutor) chunkerFor(q source.QuerySource, table string, ta *pb.TableAssignment) (source.ChunkSource, error) {
	if c, ok := x.chunkers[table]; ok {
		return c, nil
	}
	c, err := q.NewChunker(table, strings.Join(ta.PrimaryKey, ","), x.chunkSz)
	if err != nil {
		return nil, err
	}
	if x.chunkers == nil {
		x.chunkers = map[string]source.ChunkSource{}
	}
	x.chunkers[table] = c
	return c, nil
}

// emitWindow turns one byte-capped page into an open window: it waits out the
// in-flight backpressure, captures the source position AFTER the page was read
// (the interleave invariant), assigns a window id, and announces it before
// storing the page's rows. It takes ownership of page on success.
func (x *chunkExecutor) emitWindow(ctx context.Context, req *pb.ChunkRequest, ta *pb.TableAssignment, page arrow.RecordBatch, n int, windowIDs *[]uint64) error {
	for x.w.OpenWindows(ta.TargetTable) >= snapshotWindowInFlight() {
		select {
		case <-x.w.WindowClosed(ta.TargetTable):
		case <-ctx.Done():
			page.Release()
			return ctx.Err()
		}
	}
	posStr := ""
	if x.pos != nil {
		pos, err := x.pos.InitialPosition(ctx)
		if err != nil {
			page.Release()
			return fmt.Errorf("worker: window position: %w", err)
		}
		posStr = pos.String()
	}
	// The window's high key — the last row's PK tuple — is the cursor a
	// mid-chunk redo resumes from (issue #646): the coordinator uses it to
	// re-request the chunk from the last committed window, not its start.
	var highKey []byte
	if n > 0 && len(ta.PrimaryKey) > 0 {
		reader, err := transport.NewBatchReader(page, ta.PrimaryKey)
		if err != nil {
			page.Release()
			return fmt.Errorf("worker: window high key: %w", err)
		}
		highKey, err = transport.EncodeBounds(reader.Key(int(n)-1), nil)
		if err != nil {
			page.Release()
			return fmt.Errorf("worker: window high key: %w", err)
		}
	}
	seq := x.windowSeq.Add(1)
	if err := x.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_WindowOpen{WindowOpen: &pb.WindowOpen{
		Table:   req.Table,
		ChunkId: req.ChunkId,
		Attempt: x.epoch,
		Seq:     seq,
		Pos:     posStr,
		HighKey: highKey,
	}}}); err != nil {
		page.Release()
		return err
	}
	*windowIDs = append(*windowIDs, seq)
	// AddWindowRows takes ownership of the batch (the window stores it).
	return x.w.AddWindowRows(ta.TargetTable, seq, &dataplane.Batch{Table: ta.TargetTable, Record: page, Mode: dataplane.AppendMode})
}
