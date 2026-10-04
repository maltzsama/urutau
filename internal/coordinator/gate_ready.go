package coordinator

import (
	"context"

	"github.com/maltzsama/urutau/dataplane"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// gateMaxBytes bounds one window's held live batches by size, as
// gateMaxEvents does by count: rows carry payloads, and 1024 batches of the
// full profile's events held GiBs and OOM-killed the coordinator (#438). A
// gate is full once it holds this much; the batch that crosses it is
// admitted, so one oversized batch never deadlocks the gate.
const gateMaxBytes = 64 << 20

// gateHold buffers a batch when a window is open for a partition its rows
// could belong to. A full gate never grows without bound: once the window's
// chunk is ready it drains InWindow-tagged (drainReadyWindow); before that
// it blocks the pump until ChunkReady. Ownership: when
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
	defer c.gateMu.Unlock()
	key, held := c.openKeyForTableLocked(b.Table)
	if !held {
		return false
	}
	for len(c.gateBuf[key]) >= gateMaxEvents || c.gateBytes[key] >= gateMaxBytes {
		_, ready := c.gateReady[key]
		drain := c.gateDrain
		c.gateMu.Unlock()
		if ready {
			// The chunk is in the worker's window: drain the held batches
			// InWindow-tagged now, so the pump keeps pulling the reader.
			if err := c.drainReadyWindow(ctx, key); err != nil {
				c.pumpFail(ctx, err)
				b.Release() // the run is terminating; the batch goes nowhere
				c.gateMu.Lock()
				return true
			}
		} else {
			// Before ChunkReady: wait for the worker's chunk SELECT, which
			// does not depend on the pump.
			select {
			case <-drain:
			case <-ctx.Done():
				c.gateMu.Lock()
				return false
			}
		}
		c.gateMu.Lock()
		// Re-check after the wait or the drain: the gate may have closed, or
		// the table's window changed, while the pump was away.
		if key, held = c.openKeyForTableLocked(b.Table); !held {
			return false
		}
	}
	c.gateAppendLocked(key, b)
	return true
}

// gateAppendLocked holds a batch in a window's gate. Caller holds gateMu.
func (c *Coordinator) gateAppendLocked(key string, b *dataplane.Batch) {
	if c.gateBytes == nil {
		c.gateBytes = map[string]int64{}
	}
	c.gateBuf[key] = append(c.gateBuf[key], b)
	c.gateBytes[key] += batchBytes(b)
}

// gateTakeLocked empties a window's gate and returns what it held, oldest
// first. Caller holds gateMu.
func (c *Coordinator) gateTakeLocked(key string) []*dataplane.Batch {
	buf := c.gateBuf[key]
	delete(c.gateBuf, key)
	delete(c.gateBytes, key)
	return buf
}

// markChunkReady records that chunkID's rows are in its worker's window (its
// ChunkReady arrived), and wakes a pump blocked on the full gate so it drains.
func (c *Coordinator) markChunkReady(target string, partition int, chunkID uint32) {
	key := gateKey(target, partition)
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	if !c.gateOn[key] {
		return
	}
	if c.gateReady == nil {
		c.gateReady = map[string]uint32{}
	}
	c.gateReady[key] = chunkID
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
}

// drainReadyWindow is the pump's early flush of a full gate whose chunk is
// ready: the held batches go out InWindow-tagged for that chunk, as
// flushWindow would at the end of the catch-up.
func (c *Coordinator) drainReadyWindow(ctx context.Context, key string) error {
	c.gateFlushMu.Lock()
	defer c.gateFlushMu.Unlock()
	c.gateMu.Lock()
	chunkID, ready := c.gateReady[key]
	win, open := c.gateWin[key]
	if !ready || !open {
		c.gateMu.Unlock()
		return nil
	}
	buf := c.gateTakeLocked(key)
	close(c.gateDrain)
	c.gateDrain = make(chan struct{})
	c.gateMu.Unlock()
	return c.enqueueWindowed(ctx, win.target, chunkID, buf)
}

// enqueueWindowed queues held batches InWindow-tagged for chunkID, in order.
// A fresh meta per batch: enqueueBatch assigns the cycle id into it, and a
// shared one would put every held batch in one cycle.
func (c *Coordinator) enqueueWindowed(ctx context.Context, target string, chunkID uint32, buf []*dataplane.Batch) error {
	return c.enqueueHeld(ctx, &pb.BatchMeta{
		Table:  target,
		Window: &pb.WindowTag{InWindow: true, WindowId: uint64(chunkID)},
	}, buf)
}

// enqueueHeld sends a gate's held batches, in order, with meta's window tag.
// They are consecutive batches of one table under one tag, so they go out
// concatenated, up to cycleMaxRows rows and cycleMaxBytes bytes per batch
// (coalesce.go): one held
// batch per cycle made a window's drain hundreds of one-row commits (#437).
// A fresh meta per sent batch: enqueueBatch assigns the cycle id into it.
func (c *Coordinator) enqueueHeld(ctx context.Context, meta *pb.BatchMeta, buf []*dataplane.Batch) error {
	for len(buf) > 0 {
		n, rows, bytes := 0, int64(0), int64(0)
		for n < len(buf) && c.fits(rows, bytes, buf[n].Record.NumRows(), batchBytes(buf[n])) {
			rows += buf[n].Record.NumRows()
			bytes += batchBytes(buf[n])
			n++
		}
		group := buf[:n]
		buf = buf[n:]
		merged, err := concatSourceBatches(group)
		for _, b := range group {
			b.Release()
		}
		if err == nil {
			err = c.enqueueBatch(ctx, merged, cloneBatchMeta(meta))
		}
		if err != nil {
			for _, rest := range buf {
				rest.Release()
			}
			return err
		}
	}
	return nil
}
