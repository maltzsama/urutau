package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// The source cuts a batch at every change of table, so a binlog interleaving
// several tables reaches the coordinator as batches of one or two rows. On a
// staged table every batch was its own cycle — one Iceberg commit, one tiny
// data file per owner — and the full profile's staged tables committed 40-100
// rows/s against ~500/s of load (#437, #414). Consecutive batches of a staged
// table must travel as one cycle.
func TestStagedTableBatchesTravelAsFewCycles(t *testing.T) {
	c := gateStagedHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var rows, subBatches atomic.Int64
	for _, w := range []*workerState{c.workers["w0"], c.workers["w1"]} {
		go func(w *workerState) {
			for {
				select {
				case q := <-w.queue:
					c.budget.release(w.name, int64(len(q.body)+len(q.meta)))
					subBatches.Add(1)
					rows.Add(queuedRows(t, q))
				case <-ctx.Done():
					return
				}
			}
		}(w)
	}

	const n = 200
	out := make(chan *dataplane.Batch, n)
	for i := 0; i < n; i++ {
		out <- gateBatch(t, int64(i), fmt.Sprintf("0/%X", i+1)) // ids 0-199: both partitions
	}
	go c.pump(ctx, out)

	deadline := time.Now().Add(5 * time.Second)
	for rows.Load() < n && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := rows.Load(); got != n {
		t.Fatalf("%d of %d rows reached the workers", got, n)
	}
	// Two owners: at best one sub-batch each; allow a few cycles for timing.
	if got := subBatches.Load(); got > 10 {
		t.Fatalf("%d rows reached the workers in %d sub-batches: consecutive batches of a staged table were not coalesced", n, got)
	}
}

// queuedRows counts the rows of a queued batch (its Arrow IPC body); a marker
// has none.
func queuedRows(t *testing.T, q queuedBatch) int64 {
	t.Helper()
	if len(q.body) == 0 {
		return 0
	}
	r, err := ipc.NewReader(bytes.NewReader(q.body))
	if err != nil {
		t.Errorf("queued batch body: %v", err)
		return 0
	}
	defer r.Release()
	var n int64
	for r.Next() {
		n += r.RecordBatch().NumRows()
	}
	return n
}

// drainQueue reads what is queued for a worker right now, in order.
func drainQueue(t *testing.T, w *workerState) (metas []*pb.BatchMeta, rows []int64) {
	t.Helper()
	for {
		select {
		case q := <-w.queue:
			m := &pb.BatchMeta{}
			if err := proto.Unmarshal(q.meta, m); err != nil {
				t.Fatal(err)
			}
			metas = append(metas, m)
			rows = append(rows, queuedRows(t, q))
		default:
			return metas, rows
		}
	}
}

// holdAccumulator makes the table's accumulator hold: a cycle is pending, as
// while an Iceberg commit is in flight.
func holdAccumulator(c *Coordinator) {
	c.staged.expect(core.TableRef{Target: "raw.orders"}, 1_000_000, []string{"w0"})
}

// Batches accumulated before a DBLog window opens are older than every event
// the window gates: they reach the worker first, untagged. Sent after the
// window's InWindow events, an older image would overwrite the snapshot's.
func TestAccumulatedBatchesPrecedeTheWindow(t *testing.T) {
	c := gateStagedHarness(t)
	ctx := context.Background()
	holdAccumulator(c)
	for i := int64(1); i <= 3; i++ { // ids 1-3: w0's partition
		if taken, err := c.accumulate(ctx, gateBatch(t, i, fmt.Sprintf("0/%X", i))); !taken || err != nil {
			t.Fatalf("accumulate %d: taken=%v err=%v", i, taken, err)
		}
	}
	if metas, _ := drainQueue(t, c.workers["w0"]); len(metas) != 0 {
		t.Fatalf("%d batch(es) sent while a cycle was pending; want them held", len(metas))
	}

	if err := c.openWindowFlushed(ctx, "raw.orders", 0); err != nil {
		t.Fatal(err)
	}
	if !c.gateHold(ctx, gateBatch(t, 4, "0/4")) {
		t.Fatal("the open window did not hold the live batch")
	}
	if err := c.flushWindow(ctx, "raw.orders", 0, 5); err != nil {
		t.Fatal(err)
	}

	metas, rows := drainQueue(t, c.workers["w0"])
	if len(metas) != 2 {
		t.Fatalf("w0 got %d batches, want the accumulated cycle then the window's event", len(metas))
	}
	if metas[0].Window != nil || rows[0] != 3 || metas[0].HighPos != "0/3" {
		t.Fatalf("first: window=%v rows=%d high=%s; want the 3 accumulated rows untagged, up to 0/3", metas[0].Window, rows[0], metas[0].HighPos)
	}
	if !metas[1].Window.GetInWindow() || metas[1].Window.ChunkId != 5 || metas[1].HighPos != "0/4" {
		t.Fatalf("second: %+v; want the InWindow event at 0/4", metas[1])
	}
}

// A re-slice pauses the table: batches accumulated before the pause are older
// than the ones it held, and go out first when the table resumes.
func TestAccumulatedBatchesPrecedeThePausedOnes(t *testing.T) {
	c := gateStagedHarness(t)
	ctx := context.Background()
	holdAccumulator(c)
	for i := int64(1); i <= 2; i++ {
		if taken, err := c.accumulate(ctx, gateBatch(t, i, fmt.Sprintf("0/%X", i))); !taken || err != nil {
			t.Fatalf("accumulate %d: taken=%v err=%v", i, taken, err)
		}
	}
	c.pausedMu.Lock()
	if c.paused == nil {
		c.paused = map[string]chan struct{}{}
	}
	c.paused["raw.orders"] = make(chan struct{})
	c.pausedMu.Unlock()
	if !c.pauseHold(gateBatch(t, 3, "0/3")) {
		t.Fatal("the paused table did not hold the batch")
	}
	c.pausedMu.Lock()
	delete(c.paused, "raw.orders")
	c.pausedMu.Unlock()
	if err := c.flushPaused(ctx); err != nil {
		t.Fatal(err)
	}

	metas, rows := drainQueue(t, c.workers["w0"])
	if len(metas) != 2 || metas[0].HighPos != "0/2" || rows[0] != 2 || metas[1].HighPos != "0/3" {
		t.Fatalf("w0 got %d batches (%v); want the 2 accumulated rows up to 0/2, then the paused one at 0/3", len(metas), metas)
	}
}

// The coalesced batch is what a bigger source batch would have been: rows in
// source order, the last batch's watermark.
func TestConcatSourceBatchesKeepsSourceOrder(t *testing.T) {
	bs := []*dataplane.Batch{gateBatch(t, 7, "0/1"), gateBatch(t, 3, "0/2"), gateBatch(t, 9, "0/3")}
	merged, err := concatSourceBatches(bs)
	if err != nil {
		t.Fatal(err)
	}
	defer merged.Release()
	for _, b := range bs {
		b.Release()
	}
	if merged.Record.NumRows() != 3 || string(merged.Watermark) != "0/3" {
		t.Fatalf("merged: %d rows, watermark %s", merged.Record.NumRows(), merged.Watermark)
	}
	r, err := transport.NewBatchReader(merged.Record, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"0/1", "0/2", "0/3"} {
		if got := r.Position(i); got != want {
			t.Fatalf("row %d position %s, want %s", i, got, want)
		}
	}
}
