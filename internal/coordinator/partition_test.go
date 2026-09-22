package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

func TestComparePKOrdersInt64(t *testing.T) {
	cases := []struct {
		a, b []any
		want int
	}{
		{[]any{int64(1)}, []any{int64(2)}, -1},
		{[]any{int64(5)}, []any{int64(5)}, 0},
		{[]any{int64(9)}, []any{int64(2)}, 1},
	}
	for _, c := range cases {
		if got := sign(comparePK(c.a, c.b)); got != c.want {
			t.Errorf("comparePK(%v, %v) sign = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestComparePKOrdersStrings(t *testing.T) {
	if comparePK([]any{"aaa"}, []any{"aab"}) >= 0 {
		t.Fatal(`comparePK("aaa", "aab") should be < 0`)
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

func TestPartitionOwnerSingleRangeAlwaysZero(t *testing.T) {
	ranges := []source.Chunk{{}}
	if got := partitionOwner(ranges, []any{int64(12345)}); got != 0 {
		t.Fatalf("partitionOwner with one range = %d, want 0", got)
	}
}

func TestPartitionOwnerThreeWayContiguous(t *testing.T) {
	// [-, 100), [100, 200), [200, -)
	ranges := []source.Chunk{
		{Low: nil, High: []any{int64(100)}},
		{Low: []any{int64(100)}, High: []any{int64(200)}},
		{Low: []any{int64(200)}, High: nil},
	}
	cases := []struct {
		key  int64
		want int
	}{
		{0, 0}, {99, 0}, {100, 1}, {150, 1}, {199, 1}, {200, 2}, {1000, 2},
	}
	for _, c := range cases {
		got := partitionOwner(ranges, []any{c.key})
		if got != c.want {
			t.Errorf("partitionOwner(key=%d) = %d, want %d", c.key, got, c.want)
		}
	}
}

func TestPartitionOwnerNoMatchReturnsNegativeOne(t *testing.T) {
	// A gap in the ranges (shouldn't happen from Partitions(), but the
	// function must not silently misroute if it ever does).
	ranges := []source.Chunk{
		{Low: nil, High: []any{int64(10)}},
		{Low: []any{int64(20)}, High: nil},
	}
	if got := partitionOwner(ranges, []any{int64(15)}); got != -1 {
		t.Fatalf("partitionOwner(key=15) = %d, want -1 (gap between ranges)", got)
	}
}

func TestClipChunksToRangeUnpartitionedPassesThrough(t *testing.T) {
	chunks := []source.Chunk{{Low: []any{int64(0)}, High: []any{int64(10)}}}
	got := clipChunksToRange(chunks, source.Chunk{})
	if len(got) != 1 || got[0].Low[0] != int64(0) {
		t.Fatalf("clipChunksToRange with an empty range should pass chunks through unchanged: %+v", got)
	}
}

func TestClipChunksToRangeDropsOutsideChunks(t *testing.T) {
	chunks := []source.Chunk{
		{Low: nil, High: []any{int64(50)}},
		{Low: []any{int64(50)}, High: []any{int64(100)}},
		{Low: []any{int64(100)}, High: nil},
	}
	// Partition range [50, 100) should keep only the middle chunk.
	got := clipChunksToRange(chunks, source.Chunk{Low: []any{int64(50)}, High: []any{int64(100)}})
	if len(got) != 1 {
		t.Fatalf("clipChunksToRange = %+v, want exactly 1 chunk", got)
	}
	if comparePK(got[0].Low, []any{int64(50)}) != 0 || comparePK(got[0].High, []any{int64(100)}) != 0 {
		t.Fatalf("clipped chunk = %+v, want [50,100)", got[0])
	}
}

func TestClipChunksToRangeClampsStraddlingChunk(t *testing.T) {
	// One big chunk [0, 1000) straddles a [200, 400) partition range —
	// the clipped chunk must not leak rows outside [200,400).
	chunks := []source.Chunk{{Low: []any{int64(0)}, High: []any{int64(1000)}}}
	got := clipChunksToRange(chunks, source.Chunk{Low: []any{int64(200)}, High: []any{int64(400)}})
	if len(got) != 1 {
		t.Fatalf("clipChunksToRange = %+v, want 1 clamped chunk", got)
	}
	if comparePK(got[0].Low, []any{int64(200)}) != 0 || comparePK(got[0].High, []any{int64(400)}) != 0 {
		t.Fatalf("clamped chunk = %+v, want [200,400)", got[0])
	}
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
		owner[i] = partitionOwner(ranges, reader.Key(i))
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
