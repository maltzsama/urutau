package coordinator

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// A worker stages a batch of a partitioned table, and its session is lost
// before the coordinator handles what it sent: the worker's generation moves
// on. The staged delivery of the old generation is dropped (it must be), so
// the cycle still owes that owner's share. The ack that came with it must not
// clear the batch: an acked batch is never redelivered, and the cycle would
// wait forever for a delivery nobody will send again — with every later cycle
// of the table queued behind it.
func TestStagedAckFromASupersededGenerationKeepsTheBatchInFlight(t *testing.T) {
	c := stagedReplayHarness(t)
	names := []string{"w0", "w1"}
	b := replayBatch(t, []int64{ownerKey(t, names, 0), ownerKey(t, names, 1)}, []string{"0/30", "0/40"})
	meta := &pb.BatchMeta{Table: "raw.orders"}
	if err := c.enqueueBatch(context.Background(), b, meta); err != nil {
		t.Fatalf("enqueueBatch: %v", err)
	}
	if n := c.index.Of("w0").InFlight(); n != 1 {
		t.Fatalf("setup: w0 has %d batches in flight, want 1", n)
	}

	c.mu.Lock()
	old := c.workers["w0"].epoch
	c.workers["w0"].epoch++ // w0's session was lost: a new generation
	c.mu.Unlock()

	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Seq: meta.BatchId, Epoch: old, Descriptor_: []byte{1}, Position: "0/40"})
	c.onAck("w0", &pb.Ack{Table: "raw.orders", Epoch: old, Position: "0/40", Rows: 1})

	if n := c.index.Of("w0").InFlight(); n != 1 {
		t.Fatalf("after a stale-generation ack w0 has %d batches in flight, want 1: the batch was cleared although its staged delivery was dropped, so nothing will redeliver it", n)
	}
	if open := c.staged.OpenFor(core.TableRef{Target: "raw.orders"}); open != 1 {
		t.Fatalf("%d open cycles, want the one still owed w0's share", open)
	}

	// The current generation restages the redelivered batch: the cycle completes.
	c.onStagedBatch("w1", &pb.StagedBatch{Table: "raw.orders", Seq: meta.BatchId, Epoch: c.workers["w1"].epoch, Descriptor_: []byte{2}, Position: "0/40"})
	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Seq: meta.BatchId, Epoch: old + 1, Descriptor_: []byte{1}, Position: "0/40"})
	c.onAck("w0", &pb.Ack{Table: "raw.orders", Epoch: old + 1, Position: "0/40", Rows: 1})
	if open := c.staged.OpenFor(core.TableRef{Target: "raw.orders"}); open != 0 {
		t.Fatalf("%d open cycles after both owners delivered, want 0", open)
	}
	if n := c.index.Of("w0").InFlight(); n != 0 {
		t.Fatalf("w0 has %d batches in flight after the current generation's ack, want 0", n)
	}
}

// On a table the worker commits itself, an ack is evidence of a durable
// commit whatever generation sent it: it still clears the batch.
func TestDirectAckFromASupersededGenerationStillClearsTheBatch(t *testing.T) {
	c, _ := coordHarness()
	c.setRouteForTest("raw.orders", []*workerState{c.workers["w0"]})
	b := replayBatch(t, []int64{1}, []string{"0/30"})
	if err := c.enqueueBatch(context.Background(), b, &pb.BatchMeta{Table: "raw.orders"}); err != nil {
		t.Fatalf("enqueueBatch: %v", err)
	}
	if n := c.index.Of("w0").InFlight(); n != 1 {
		t.Fatalf("setup: w0 has %d batches in flight, want 1", n)
	}
	c.mu.Lock()
	old := c.workers["w0"].epoch
	c.workers["w0"].epoch++
	c.mu.Unlock()

	c.onAck("w0", &pb.Ack{Table: "raw.orders", Epoch: old, Position: "0/30", Rows: 1})
	if n := c.index.Of("w0").InFlight(); n != 0 {
		t.Fatalf("w0 has %d batches in flight after its commit's ack, want 0", n)
	}
}

// A worker stages a snapshot window of a partitioned table and acks the
// window's Closes marker, and its session is lost before the coordinator
// handles either. The staged delivery of the old generation is dropped, so
// the window's cycle still owes that owner's share. The marker ack that came
// with it must not release the marker: a released marker is not redelivered
// when the worker reconnects, the cycle waits forever, and every later cycle
// of the table — window or live — queues behind it (#855).
func TestMarkerAckFromASupersededGenerationKeepsTheMarkerInFlight(t *testing.T) {
	c := stagedReplayHarness(t)
	w0 := c.workers["w0"]
	if err := c.sendClosesPending(context.Background(), w0, "raw.orders", position.MustLSN("0/30"), 11, nil, 0, nil); err != nil {
		t.Fatalf("sendClosesPending: %v", err)
	}
	held := c.heldChunkMarkers("w0")
	if len(held) != 1 {
		t.Fatalf("setup: w0 holds %d markers, want 1", len(held))
	}
	marker := held[0].ID

	c.mu.Lock()
	old := w0.epoch
	w0.epoch++ // w0's session was lost: a new generation
	c.mu.Unlock()

	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Seq: marker, Epoch: old, Descriptor_: []byte{1}})
	c.onAck("w0", &pb.Ack{Table: "raw.orders", Epoch: old, BatchId: marker})

	if !c.index.Of("w0").Holds(marker) {
		t.Fatal("a stale-generation marker ack released the marker although its staged delivery was dropped: nothing will redeliver it")
	}
	if open := c.staged.OpenFor(core.TableRef{Target: "raw.orders"}); open != 1 {
		t.Fatalf("%d open cycles, want the window's, still owed w0's share", open)
	}

	// The reconnected worker gets the marker again, reports the window it no
	// longer holds, and acks: the cycle completes and the marker is released.
	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Seq: marker, Epoch: old + 1, WindowLost: true})
	c.onAck("w0", &pb.Ack{Table: "raw.orders", Epoch: old + 1, BatchId: marker})
	if open := c.staged.OpenFor(core.TableRef{Target: "raw.orders"}); open != 0 {
		t.Fatalf("%d open cycles after the current generation delivered, want 0", open)
	}
	if c.index.Of("w0").Holds(marker) {
		t.Fatal("the current generation's marker ack did not release the marker")
	}
}

// On a table the worker commits itself, a marker ack is evidence that the
// window's rows are durable, whatever generation sent it.
func TestMarkerAckFromASupersededGenerationReleasesADirectTablesMarker(t *testing.T) {
	c, w0 := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	if err := c.sendClosesPending(context.Background(), w0, "raw.orders", position.MustLSN("0/30"), 11, nil, 0, nil); err != nil {
		t.Fatalf("sendClosesPending: %v", err)
	}
	held := c.heldChunkMarkers("w0")
	if len(held) != 1 {
		t.Fatalf("setup: w0 holds %d markers, want 1", len(held))
	}
	marker := held[0].ID
	c.mu.Lock()
	old := w0.epoch
	w0.epoch++
	c.mu.Unlock()

	c.onAck("w0", &pb.Ack{Table: "raw.orders", Epoch: old, BatchId: marker})
	if c.index.Of("w0").Holds(marker) {
		t.Fatal("the marker of a table the worker commits itself was kept after its ack")
	}
}
