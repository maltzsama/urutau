package coordinator

// Batch 5c: the remaining cheap error branches that push the package over
// its 65% floor — enqueueBatch's routing failures, the snapshot entry points'
// early exits, provisionWorkers' no-op gates, and two pure formatters.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
)

// transportSchema is a canonical schema with the wire metadata columns, for
// the marker-encode path.
func transportSchema(t *testing.T) core.Schema {
	t.Helper()
	return transport.InferSchemaFromChanges([]rowchange.Change{{
		Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(1)},
		After: map[string]any{"id": int64(1)}, Position: "0/11", IngestTS: time.Now(),
	}})
}

func TestWriteModeToPBAndTsOrEmpty(t *testing.T) {
	if got := writeModeToPB(dataplane.AppendMode); got != pb.WriteMode_WRITE_MODE_APPEND {
		t.Fatalf("writeModeToPB(append) = %v", got)
	}
	if got := writeModeToPB(dataplane.UpsertMode); got != pb.WriteMode_WRITE_MODE_UPSERT {
		t.Fatalf("writeModeToPB(upsert) = %v", got)
	}
	if got := tsOrEmpty(time.Time{}); got != "" {
		t.Fatalf("tsOrEmpty(zero) = %q", got)
	}
	if got := tsOrEmpty(time.Unix(0, 0)); got == "" {
		t.Fatal("tsOrEmpty(non-zero) must render")
	}
}

func TestProvisionWorkersNoOps(t *testing.T) {
	c := &Coordinator{log: slog.New(slog.DiscardHandler)}
	if err := c.provisionWorkers(context.Background(), nil); err != nil {
		t.Fatalf("provisionWorkers(empty) = %v", err)
	}
	if err := c.provisionWorkers(context.Background(), map[string]string{"w": "t"}); err != nil {
		t.Fatalf("provisionWorkers(no template) = %v", err)
	}
}

// ── enqueueBatch error branches ──────────────────────────────────────

func TestEnqueueBatchUnknownTable(t *testing.T) {
	c, _ := coordHarness()
	b := testWireRecord(t)
	b.Table = "ghost"
	if err := c.enqueueBatch(context.Background(), b, &pb.BatchMeta{}); err == nil {
		t.Fatal("a table no worker owns must error")
	}
}

func TestEnqueueBatchPartitionedErrors(t *testing.T) {
	ctx := context.Background()
	c, w0 := coordHarness()
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.route["raw.orders"] = []*workerState{w0, w1}
	c.index["w1"] = newPositionIndex("run-1")

	// No primary key to route by.
	if err := c.enqueueBatch(ctx, wireBatchIDs(t, 1), &pb.BatchMeta{Table: "raw.orders"}); err == nil {
		t.Fatal("a partitioned table without a primary key must error")
	}

	// Range count does not match owner count.
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.partitionRanges = map[string][]source.Chunk{
		"raw.orders": {{Low: nil, High: nil}},
	}
	if err := c.enqueueBatch(ctx, wireBatchIDs(t, 1), &pb.BatchMeta{Table: "raw.orders"}); err == nil {
		t.Fatal("a range/owner count mismatch must error")
	}

	// A row whose key matches no range.
	c.partitionRanges = map[string][]source.Chunk{
		"raw.orders": {
			{Low: []any{int64(100)}, High: []any{int64(200)}},
			{Low: []any{int64(200)}, High: nil},
		},
	}
	if err := c.enqueueBatch(ctx, wireBatchIDs(t, 1), &pb.BatchMeta{Table: "raw.orders"}); err == nil {
		t.Fatal("a key outside every range must error")
	}
}

// A window-lifecycle marker goes through enqueueTo to one partition owner (the
// production path); enqueueBatch carries source batches only and rejects a nil
// batch (issue #214).
func TestEnqueueToMarkerAndEnqueueBatchRejectsNil(t *testing.T) {
	c, w0 := coordHarness()
	cs := transportSchema(t)
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": cs}

	if err := c.enqueueTo(context.Background(), w0, nil, &pb.BatchMeta{Table: "raw.orders"}); err != nil {
		t.Fatalf("enqueueTo(marker): %v", err)
	}
	if len(w0.queue) != 1 {
		t.Fatalf("marker queue = %d, want 1", len(w0.queue))
	}
	if err := c.enqueueBatch(context.Background(), nil, &pb.BatchMeta{Table: "raw.orders"}); err == nil {
		t.Fatal("enqueueBatch must reject a nil batch")
	}
}

// ── snapshot entry points ────────────────────────────────────────────

type fakeChunkSource struct {
	source.ChunkSource
	bounds [][]any
	err    error
}

func (f fakeChunkSource) Bounds(context.Context) ([][]any, error) { return f.bounds, f.err }

type fakeSourceReader struct{ source.SourceReader }

type seedingSink struct {
	sink.Sink
	seeded []string
}

func (s *seedingSink) SeedPositions(context.Context, core.TableRef, []string) error {
	s.seeded = append(s.seeded, "called")
	return nil
}

func TestSnapshotTableErrors(t *testing.T) {
	ctx := context.Background()
	c, _ := coordHarness()
	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders"}

	// No owner.
	if err := c.snapshotTable(ctx, fakeSourceReader{}, fakeChunkSource{}, ref, snapshot.SnapshotConfig{}); err == nil {
		t.Fatal("no owning worker must error")
	}

	// Range/owner count mismatch.
	c.route["raw.orders"] = []*workerState{{name: "w0"}, {name: "w1"}}
	c.partitionRanges = map[string][]source.Chunk{"raw.orders": {{}}}
	if err := c.snapshotTable(ctx, fakeSourceReader{}, fakeChunkSource{}, ref, snapshot.SnapshotConfig{}); err == nil {
		t.Fatal("range/owner mismatch must error")
	}

	// Bounds error.
	c.partitionRanges = map[string][]source.Chunk{"raw.orders": {{}, {}}}
	boom := errors.New("bounds failed")
	if err := c.snapshotTable(ctx, fakeSourceReader{}, fakeChunkSource{err: boom}, ref, snapshot.SnapshotConfig{}); !errors.Is(err, boom) {
		t.Fatalf("snapshotTable(bounds err) = %v, want boom", err)
	}
}

func TestSnapshotTableSeedsEmptyOwners(t *testing.T) {
	ctx := context.Background()
	c, _ := coordHarness()
	seeder := &seedingSink{}
	c.snk = seeder
	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders"}
	// One owner whose range (0,10) excludes every chunk (the domain starts at 50).
	c.partitionRanges = map[string][]source.Chunk{
		"raw.orders": {{Low: []any{int64(0)}, High: []any{int64(10)}}},
	}
	if err := c.snapshotTable(ctx, fakeSourceReader{}, fakeChunkSource{bounds: [][]any{{int64(50)}}}, ref, snapshot.SnapshotConfig{}); err != nil {
		t.Fatalf("snapshotTable: %v", err)
	}
	if len(seeder.seeded) != 1 {
		t.Fatal("a partition with no chunks must be seeded")
	}
}

func TestSnapshotPartitionEarlyExits(t *testing.T) {
	ctx := context.Background()
	c, w := coordHarness()
	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders"}

	boom := errors.New("bounds failed")
	if err := c.snapshotPartition(ctx, fakeSourceReader{}, fakeChunkSource{err: boom}, ref, source.Chunk{}, 0, w, snapshot.SnapshotConfig{}); !errors.Is(err, boom) {
		t.Fatalf("snapshotPartition(bounds err) = %v, want boom", err)
	}

	// A range that excludes every chunk returns nil without sending.
	err := c.snapshotPartition(ctx, fakeSourceReader{},
		fakeChunkSource{bounds: [][]any{{int64(50)}}},
		ref, source.Chunk{Low: []any{int64(0)}, High: []any{int64(10)}}, 0, w, snapshot.SnapshotConfig{})
	if err != nil {
		t.Fatalf("snapshotPartition(empty clip) = %v", err)
	}
	if len(w.out) != 0 {
		t.Fatal("an empty partition must send no ChunkRequest")
	}
}
