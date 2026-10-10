package coordinator

import (
	"context"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/gate"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// The DBLog window gate is the shared internal/gate implementation (design:
// one orchestration for collapsed and distributed mode, issue #404). This
// file is the coordinator's thin adapter: it wires the gate's drains to the
// coordinator's enqueue path and keeps the method names the rest of the
// package (and its tests) already use.

// gateMaxEvents bounds one snapshot window's held live batches. Beyond it
// the pump blocks until flushWindow drains — the gate's structural
// backpressure (audit #5). Tuned to a few minutes of a busy table at
// ~10k/s; batches are source-sized (≤ batchTarget rows), so the row volume
// held is bounded by gateMaxEvents × batchTarget.
const gateMaxEvents = 1024

// batchSink delivers a gate's drains to the coordinator's enqueue path.
type batchSink struct{ c *Coordinator }

// Flush sends a window's held batches InWindow-tagged for windowID.
func (s batchSink) Flush(ctx context.Context, w gate.Window, windowID uint64, items []*dataplane.Batch) error {
	return s.c.enqueueWindowed(ctx, w.Target, windowID, items)
}

// Close sends a window's trailing batches as ordinary live changes.
func (s batchSink) Close(ctx context.Context, w gate.Window, items []*dataplane.Batch) error {
	return s.c.enqueueHeld(ctx, &pb.BatchMeta{Table: w.Target}, items)
}

// Release discards a torn-down gate's held batches (release-on-cancel).
func (s batchSink) Release(items []*dataplane.Batch) {
	for _, b := range items {
		b.Release()
	}
}

// openWindow pauses the pump for one table's partition, tagging the current
// chunk. The gate stays open for the WHOLE snapshot of that partition
// (design §3.1): flushWindow drains per chunk without closing it, and
// closeWindow seals it at the end.
func (c *Coordinator) openWindow(target string, partition int) {
	c.gate.Open(target, partition)
}

// openWindowFlushed opens a DBLog window after sending the table's
// accumulated batches, which predate it: sent later, they would reach the
// worker after the window's own events. The window opens first, under the
// gate's flush lock, so no new batch joins the accumulator in between and no
// gate drain overtakes the send.
func (c *Coordinator) openWindowFlushed(ctx context.Context, target string, partition int) error {
	var err error
	c.gate.WithFlush(func() {
		c.openWindow(target, partition)
		err = c.sendAccumLocked(ctx, target)
	})
	return err
}

// flushWindow drains the gated batches collected since the last drain for one
// partition's window, each InWindow-tagged for the given chunk, then returns
// (that window stays open). Batch ownership transfers to enqueueBatch per
// drain.
func (c *Coordinator) flushWindow(ctx context.Context, target string, partition int, windowID uint64) error {
	return c.gate.Flush(ctx, target, partition, windowID)
}

// closeWindow releases any remaining gated batches (post-last-chunk) for one
// partition's window and closes just that window — other partitions of the
// same table still snapshotting keep their own windows open. The trailing
// events are ordinary live changes: no window tag.
func (c *Coordinator) closeWindow(ctx context.Context, target string, partition int) error {
	return c.gate.Close(ctx, target, partition)
}

// gateHold buffers a batch when a window is open for a partition its rows
// could belong to (the gate implements the backpressure and early-drain
// semantics). Ownership: when it returns true the batch is in the gate and
// released by flushWindow/closeWindow; a failed early drain releases it here
// and fails the pump.
func (c *Coordinator) gateHold(ctx context.Context, b *dataplane.Batch) bool {
	held, err := c.gate.Hold(ctx, b.Table, b)
	if err != nil {
		c.pumpFail(ctx, err)
		b.Release() // the run is terminating; the batch goes nowhere
		return true
	}
	return held
}

// markWindowReady records that windowID's rows are in the worker's window (a
// WindowOpen/ChunkReady arrived), and wakes a pump blocked on the full gate
// so it drains.
func (c *Coordinator) markWindowReady(target string, partition int, windowID uint64) {
	c.gate.MarkReady(target, partition, windowID)
}

// releaseAllGates closes every open gate and releases the batches held in it.
// Called when the snapshot phase ends, so an aborted snapshot (ctx cancelled
// before closeWindow) does not leak the gated batches (issue #212).
func (c *Coordinator) releaseAllGates() {
	c.gate.ReleaseAll()
}

// openKeyForTableLocked returns the first open gate key for target, if any.
func (c *Coordinator) openKeyForTableLocked(target string) (string, bool) {
	c.gate.Lock()
	defer c.gate.Unlock()
	return c.gate.OpenKeyLocked(target)
}
