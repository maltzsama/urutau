package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

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

// gateKey renders a gateWindow into the map key gateOn/gateBuf/gateWin use.
func gateKey(target string, partition int) string {
	return fmt.Sprintf("%s#%d", target, partition)
}

// gateHold buffers a batch when a window is open for a partition its rows
// could belong to. A full gate blocks the pump until the snapshot drains
// it, instead of growing the buffer without bound. Ownership: when
// gateHold returns true the batch is in the gate and released by
// flushWindow/closeWindow.
//
// A table with only one partition (the common case) always gates on
// gateKey(target, 0) — the same single-window behavior as before
// partitioning existed. A partitioned table's batch is gated by EVERY
// open window that its PK range could overlap: gateHold does not decode
// rows to know precisely which partitions a batch touches, so it
// conservatively holds a batch against any open window for its table
// rather than risk releasing a row whose partition's snapshot chunk
// hasn't confirmed caught-up yet. This can hold a batch slightly longer
// than strictly necessary (extra latency, never data loss) when multiple
// partitions of the same table snapshot concurrently.
//
// If the context dies while the pump waits on a full gate, gateHold returns
// false and the batch is treated as live (not gated). That is only reachable
// during shutdown, where the pump exits on ctx.Done immediately after — it
// must not be relied on in any live path.
func (c *Coordinator) gateHold(ctx context.Context, b *dataplane.Batch) bool {
	c.gateMu.Lock()
	key, held := c.openKeyForTableLocked(b.Table)
	if !held {
		c.gateMu.Unlock()
		return false
	}
	full := len(c.gateBuf[key]) >= gateMaxEvents
	c.gateMu.Unlock()
	if full {
		select {
		case <-c.gateDrain:
		case <-ctx.Done():
			return false
		}
	}
	c.gateMu.Lock()
	// Re-check after the wait: the gate may have drained, closed, or the
	// table's window may have changed while the pump was asleep.
	key, held = c.openKeyForTableLocked(b.Table)
	if !held {
		c.gateMu.Unlock()
		return false
	}
	c.gateBuf[key] = append(c.gateBuf[key], b)
	c.gateMu.Unlock()
	return true
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
func (c *Coordinator) flushWindow(ctx context.Context, target string, partition int, chunkID uint32) error {
	key := gateKey(target, partition)
	c.gateMu.Lock()
	buf := c.gateBuf[key]
	c.gateBuf[key] = nil
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()

	meta := &pb.BatchMeta{
		Table:  target,
		Window: &pb.WindowTag{InWindow: true, ChunkId: chunkID},
	}
	for i, b := range buf {
		// A fresh meta per batch: enqueueBatch assigns the cycle id into
		// it, and a shared one would put every held batch in one cycle.
		if err := c.enqueueBatch(ctx, b, cloneBatchMeta(meta)); err != nil {
			for _, rest := range buf[i+1:] {
				rest.Release()
			}
			return err
		}
	}
	return nil
}

// closeWindow releases any remaining gated batches (post-last-chunk) for
// one partition's window and closes just that window — other partitions
// of the same table still snapshotting keep their own windows open. The
// trailing events are ordinary live changes: no window tag.
func (c *Coordinator) closeWindow(ctx context.Context, target string, partition int) error {
	key := gateKey(target, partition)
	c.gateMu.Lock()
	buf := c.gateBuf[key]
	delete(c.gateOn, key)
	delete(c.gateWin, key)
	delete(c.gateBuf, key)
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()

	meta := &pb.BatchMeta{Table: target}
	for i, b := range buf {
		// A fresh meta per batch, as in flushWindow.
		if err := c.enqueueBatch(ctx, b, cloneBatchMeta(meta)); err != nil {
			for _, rest := range buf[i+1:] {
				rest.Release()
			}
			return err
		}
	}
	return nil
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
		held = append(held, c.gateBuf[k]...)
		delete(c.gateOn, k)
		delete(c.gateWin, k)
		delete(c.gateBuf, k)
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
