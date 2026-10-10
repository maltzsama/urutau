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

// enqueueWindowed queues held batches InWindow-tagged for windowID, in order.
// A fresh meta per batch: enqueueBatch assigns the cycle id into it, and a
// shared one would put every held batch in one cycle.
func (c *Coordinator) enqueueWindowed(ctx context.Context, target string, windowID uint64, buf []*dataplane.Batch) error {
	return c.enqueueHeld(ctx, &pb.BatchMeta{
		Table:  target,
		Window: &pb.WindowTag{InWindow: true, WindowId: windowID},
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
