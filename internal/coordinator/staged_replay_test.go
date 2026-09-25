package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// replayBatch builds a raw.orders batch whose row i has key ids[i] at
// position positions[i].
func replayBatch(t *testing.T, ids []int64, positions []string) *dataplane.Batch {
	t.Helper()
	changes := make([]rowchange.Change, len(ids))
	for i, id := range ids {
		changes[i] = rowchange.Change{
			Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{id},
			After: map[string]any{"id": id, "v": "x"}, Position: positions[i], IngestTS: time.Now(),
		}
	}
	rec, err := transport.RecordFromChanges(changes, transport.InferSchemaFromChanges(changes), nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	return &dataplane.Batch{Table: "raw.orders", Record: rec, Watermark: []byte(positions[len(positions)-1]), Mode: dataplane.UpsertMode}
}

// stagedReplayHarness is a two-partition staged table (id < 100 → w0,
// id >= 100 → w1) whose committed position at boot was 0/20.
func stagedReplayHarness(t *testing.T) *Coordinator {
	t.Helper()
	c, w0 := coordHarness()
	c.snk = fakeStagedSink{}
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.setRouteForTest("raw.orders", []*workerState{w0, w1})
	c.index["w1"] = newPositionIndex("run-1")
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.setRangesForTest(map[string][]source.Chunk{
		"raw.orders": {{Low: nil, High: []any{int64(100)}}, {Low: []any{int64(100)}, High: nil}},
	})
	c.bootCommitted = map[string]position.Position{"raw.orders": position.MustLSN("0/20")}
	return c
}

// After a crash, the stream replays from the minimum committed position across
// tables, so a staged table sees batches at or before its own committed
// position. The worker skips such a sub-batch as covered and only acks it: no
// staged delivery ever comes. A cycle that waited for one wedged the table,
// since cycles commit in send order — every later cycle queued behind it and
// nothing was committed again, silently.
func TestStagedReplayCycleWaitsOnlyForUncoveredOwners(t *testing.T) {
	c := stagedReplayHarness(t)
	// id 1 → w0 at 0/10 (covered by 0/20); id 150 → w1 at 0/30 (not covered).
	b := replayBatch(t, []int64{1, 150}, []string{"0/10", "0/30"})
	meta := &pb.BatchMeta{Table: "raw.orders"}
	if err := c.enqueueBatch(context.Background(), b, meta); err != nil {
		t.Fatalf("enqueueBatch: %v", err)
	}
	committable, known := c.staged.deliver(stagedRef("raw.orders", "w1"), meta.BatchId, []byte{1}, "0/30", "", nil)
	if !known || len(committable) != 1 {
		t.Fatalf("w1's delivery: committable=%d known=%v; want the cycle complete without w0 (whose sub-batch is covered)", len(committable), known)
	}
}

// A replayed batch wholly at or before the committed position opens no cycle.
func TestStagedReplayCoveredBatchOpensNoCycle(t *testing.T) {
	c := stagedReplayHarness(t)
	b := replayBatch(t, []int64{1, 150}, []string{"0/10", "0/20"})
	if err := c.enqueueBatch(context.Background(), b, &pb.BatchMeta{Table: "raw.orders"}); err != nil {
		t.Fatalf("enqueueBatch: %v", err)
	}
	if n := c.staged.len(); n != 0 {
		t.Fatalf("%d cycle(s) open for a fully covered batch; want 0", n)
	}
}

// Past the committed position every owner is expected, as before.
func TestStagedCycleExpectsEveryOwnerPastCommitted(t *testing.T) {
	c := stagedReplayHarness(t)
	b := replayBatch(t, []int64{1, 150}, []string{"0/30", "0/40"})
	meta := &pb.BatchMeta{Table: "raw.orders"}
	if err := c.enqueueBatch(context.Background(), b, meta); err != nil {
		t.Fatalf("enqueueBatch: %v", err)
	}
	if committable, _ := c.staged.deliver(stagedRef("raw.orders", "w1"), meta.BatchId, []byte{1}, "0/40", "", nil); len(committable) != 0 {
		t.Fatal("the cycle completed with one of two uncovered owners")
	}
}

func stagedRef(target, owner string) core.TableRef {
	return core.TableRef{Target: target, Owner: owner}
}
