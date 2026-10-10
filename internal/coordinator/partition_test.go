package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/partition"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

func mustPartitionOwner(t *testing.T, ranges []source.Chunk, key []any) int {
	t.Helper()
	p, err := partition.Owner(ranges, key)
	if err != nil {
		t.Fatalf("partition.Owner(%v): %v", key, err)
	}
	return p
}

// testChange is a minimal insert row for building a wire-shape test record.
func testChange(id int64, table string) rowchange.Change {
	return rowchange.Change{
		Op:       rowchange.OpInsert,
		Table:    table,
		Key:      []any{id},
		After:    map[string]any{"id": id, "v": "x"},
		Position: "p1",
		IngestTS: time.Now(),
	}
}

func TestSplitByOwnerRoutesRowsToTheirOwningPartition(t *testing.T) {
	changes := []rowchange.Change{
		testChange(5, "t"), testChange(150, "t"), testChange(250, "t"), testChange(50, "t"),
	}
	cs := transport.InferSchemaFromChanges(changes)
	rec, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	defer rec.Release()

	ranges := []source.Chunk{
		{Low: nil, High: []any{int64(100)}},               // partition 0: id < 100
		{Low: []any{int64(100)}, High: []any{int64(200)}}, // partition 1: 100 <= id < 200
		{Low: []any{int64(200)}, High: nil},               // partition 2: id >= 200
	}
	reader, err := transport.NewBatchReader(rec, []string{"id"})
	if err != nil {
		t.Fatalf("NewBatchReader: %v", err)
	}
	owner := make([]int, reader.NumRows())
	for i := 0; i < reader.NumRows(); i++ {
		owner[i] = mustPartitionOwner(t, ranges, reader.Key(i))
	}

	subs, err := splitByOwner(context.Background(), rec, owner, len(ranges))
	if err != nil {
		t.Fatalf("splitByOwner: %v", err)
	}
	defer func() {
		for _, s := range subs {
			if s != nil {
				s.Release()
			}
		}
	}()

	wantCounts := []int64{2, 1, 1} // ids {5,50}->p0, {150}->p1, {250}->p2
	for p, want := range wantCounts {
		if subs[p] == nil {
			t.Fatalf("partition %d: got nil sub-batch, want %d rows", p, want)
		}
		if subs[p].NumRows() != want {
			t.Fatalf("partition %d: %d rows, want %d", p, subs[p].NumRows(), want)
		}
	}

	// Every row in partition 0's sub-batch must actually have id < 100.
	sub0Reader, err := transport.NewBatchReader(subs[0], []string{"id"})
	if err != nil {
		t.Fatalf("NewBatchReader(sub0): %v", err)
	}
	for i := 0; i < sub0Reader.NumRows(); i++ {
		id := sub0Reader.Key(i)[0].(int64)
		if id >= 100 {
			t.Fatalf("partition 0 contains id=%d, which belongs to a later partition", id)
		}
	}
}

func TestSplitByOwnerOmitsEmptyPartitions(t *testing.T) {
	changes := []rowchange.Change{testChange(5, "t")}
	cs := transport.InferSchemaFromChanges(changes)
	rec, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	defer rec.Release()

	// All rows go to partition 0; partitions 1 and 2 get nothing.
	owner := []int{0}
	subs, err := splitByOwner(context.Background(), rec, owner, 3)
	if err != nil {
		t.Fatalf("splitByOwner: %v", err)
	}
	defer func() {
		for _, s := range subs {
			if s != nil {
				s.Release()
			}
		}
	}()
	if subs[0] == nil || subs[0].NumRows() != 1 {
		t.Fatalf("partition 0 = %+v, want 1 row", subs[0])
	}
	if subs[1] != nil || subs[2] != nil {
		t.Fatalf("empty partitions must be nil, got subs[1]=%v subs[2]=%v", subs[1], subs[2])
	}
}

// #217: the enqueueBatch error path must not double-release the current
// sub-batch (enqueueTo already owns its release) and must free the rest. A
// cancelled context exercises that path without a live stream.
func TestEnqueueBatchErrorReleasesRemainingSubBatches(t *testing.T) {
	c, w0 := coordHarness()
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.setRouteForTest("raw.orders", []*workerState{w0, w1})
	c.index["w1"] = newPositionIndex("run-1")
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.setRangesForTest(map[string][]source.Chunk{
		"raw.orders": {{Low: nil, High: []any{int64(100)}}, {Low: []any{int64(100)}, High: nil}},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	b := wireBatchIDs(t, 1, 150)
	if err := c.enqueueBatch(ctx, b, &pb.BatchMeta{Table: "raw.orders"}); err == nil {
		t.Fatal("a cancelled context must fail the enqueue")
	}
}

// When one partition owns every row, splitByOwner retains the input instead
// of materializing an identity Take (issue #580).
func TestSplitByOwnerAllRowsToOnePartitionRetainsInput(t *testing.T) {
	changes := []rowchange.Change{testChange(5, "t"), testChange(50, "t")}
	cs := transport.InferSchemaFromChanges(changes)
	rec, err := transport.RecordFromChanges(changes, cs, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	defer rec.Release()

	subs, err := splitByOwner(context.Background(), rec, []int{0, 0}, 1)
	if err != nil {
		t.Fatalf("splitByOwner: %v", err)
	}
	defer func() {
		for _, s := range subs {
			if s != nil {
				s.Release()
			}
		}
	}()
	if subs[0] != rec {
		t.Fatal("a single owner must retain the input record, not Take a copy")
	}
}
