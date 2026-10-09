package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// gateKey renders a gateWindow into the map key gateOn/gateBuf/gateWin use.
func gateKey(target string, partition int) string {
	return fmt.Sprintf("%s#%d", target, partition)
}

// openKeyForTableLocked returns the first open gate key for target, if
// any. Callers must hold gateMu. A table has at most as many
// simultaneously-open keys as it has partitions actively snapshotting;
// gateHold only needs to know "is ANY window for this table open" since
// it gates conservatively at the whole-table (not per-row) level.
func (c *Coordinator) openKeyForTableLocked(target string) (string, bool) {
	for k, on := range c.gateOn {
		if !on {
			continue
		}
		if w, ok := c.gateWin[k]; ok && w.target == target {
			return k, true
		}
	}
	return "", false
}

// openWindow pauses the pump for one table's partition, tagging the
// current chunk. The gate stays open for the WHOLE snapshot of that
// partition (design §3.1: the coordinator pauses relaying while it works
// the table); flushWindow drains per chunk without closing it, and
// closeWindow seals it at the end. A gate that opened and closed per
// chunk would let gap events (positioned AFTER the gate's backlog) flow
// straight through, then release older backlog after them — a
// reordering that resurrects old values.
func (c *Coordinator) openWindow(target string, partition int) {
	c.gateMu.Lock()
	key := gateKey(target, partition)
	if c.gateOn == nil {
		c.gateOn = map[string]bool{}
		c.gateWin = map[string]gateWindow{}
		c.gateBuf = map[string][]*dataplane.Batch{}
	}
	c.gateOn[key] = true
	c.gateWin[key] = gateWindow{target: target, partition: partition}
	c.gateMu.Unlock()
}

// flushWindow drains the gated batches collected since the last drain for
// one partition's window, each InWindow-tagged for the given chunk, then
// returns (that window stays open). Batch ownership transfers to
// enqueueBatch per drain. enqueueBatch itself splits a batch across
// partition owners by PK range when the table is partitioned, so a
// drained batch reaches only the rows' actual owning worker(s) even
// though the gate held it at whole-table granularity.
func (c *Coordinator) flushWindow(ctx context.Context, target string, partition int, windowID uint64) error {
	key := gateKey(target, partition)
	c.gateFlushMu.Lock()
	defer c.gateFlushMu.Unlock()
	c.gateMu.Lock()
	buf := c.gateTakeLocked(key)
	// The catch-up is over: the next chunk's batches wait for its own
	// ChunkReady.
	delete(c.gateReady, key)
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()
	return c.enqueueWindowed(ctx, target, windowID, buf)
}

// closeWindow releases any remaining gated batches (post-last-chunk) for
// one partition's window and closes just that window — other partitions
// of the same table still snapshotting keep their own windows open. The
// trailing events are ordinary live changes: no window tag.
func (c *Coordinator) closeWindow(ctx context.Context, target string, partition int) error {
	key := gateKey(target, partition)
	c.gateFlushMu.Lock()
	defer c.gateFlushMu.Unlock()
	c.gateMu.Lock()
	buf := c.gateTakeLocked(key)
	delete(c.gateOn, key)
	delete(c.gateWin, key)
	delete(c.gateReady, key)
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()

	return c.enqueueHeld(ctx, &pb.BatchMeta{Table: target}, buf)
}

// releaseAllGates closes every open gate and releases the batches held in it.
// Called when the snapshot phase ends, so an aborted snapshot (ctx cancelled
// before closeWindow) does not leak the gated batches (issue #212). gateHold
// re-checks the window under gateMu before appending, so a gate cleared here
// cannot be re-populated afterwards.
func (c *Coordinator) releaseAllGates() {
	c.gateMu.Lock()
	var held []*dataplane.Batch
	for k := range c.gateOn {
		held = append(held, c.gateTakeLocked(k)...)
		delete(c.gateOn, k)
		delete(c.gateWin, k)
		delete(c.gateReady, k)
	}
	// Wake any pump blocked on a full gate so it re-checks and sees the gate
	// gone (gateHold returns false and the batch flows as live).
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()
	for _, b := range held {
		b.Release()
	}
}

// gateMaxEvents bounds one snapshot window's held live batches. Beyond it
// the pump blocks until flushWindow drains — the gate's structural
// backpressure (audit #5). Tuned to a few minutes of a busy table at
// ~10k/s; batches are source-sized (≤ batchTarget rows), so the row volume
// held is bounded by gateMaxEvents × batchTarget.
const gateMaxEvents = 1024

// gateWindow identifies one open DBLog window: a table's Nth partition.
// An unpartitioned table's sole window is always {target, 0}.
type gateWindow struct {
	target    string
	partition int
}
