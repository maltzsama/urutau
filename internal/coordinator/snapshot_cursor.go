package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// decodeWindowHighKey decodes a WindowOpen's high_key bytes into the PK tuple
// it carries, or nil when the window has no high key (the whole-chunk path).
func decodeWindowHighKey(b []byte) ([]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	rows, err := transport.DecodeBounds(b)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// resumeChunkAfterLoss computes where a partition's snapshot resumes after a
// mid-snapshot worker loss: the chunk index to redo from, and — for a chunk
// the earlier generation partially committed — the resume low key (the last
// committed window's high key, issue #646). It applies the cursor to the
// chunk's Low bound so the redo does not re-emit committed windows.
func (c *Coordinator) resumeChunkAfterLoss(chunks []source.Chunk, w *workerState, ref source.TableRef, partition int, windows map[uint64]int, i int) int {
	i, cursor := c.redoFrom(w, ref.Target, windows, partition, i)
	c.clearChunkReady(ref.Target, partition)
	if cursor != nil {
		chunks[i].Low = cursor
	}
	return i
}

// closeOneWindow proves the reader caught up to one window's position, then
// releases its gated live rows (InWindow) and its Closes marker. The worker
// captured pos AFTER the window's read — never before — so the caught-up proof
// gates any event the window's SELECT could have seen as pre-image (the
// interleave invariant).
func (c *Coordinator) closeOneWindow(ctx context.Context, rdr source.SourceReader, ref source.TableRef, partition int, w *workerState, cfg snapshot.SnapshotConfig, wo *pb.WindowOpen, lost <-chan struct{}, pending []uint32, chunkRef uint32, highKey []any) error {
	pos, err := c.src.ParsePosition(wo.Pos)
	if err != nil {
		return fmt.Errorf("coordinator: window %d pos %q: %w", wo.Seq, wo.Pos, err)
	}
	// A worker lost while the reader catches up takes its window with it, and
	// its full queue blocks the shared pump so the reader never reaches high;
	// waiting out the window timeout would end the run (issue #526) instead of
	// redoing the chunk, so the wait aborts on the loss.
	if err := c.waitCaughtUpOrLost(ctx, rdr, pos, cfg, wo.Seq, lost); err != nil {
		return err
	}
	c.markWindowReady(ref.Target, partition, wo.Seq)
	// Release the window's gated live events (InWindow-tagged) ahead of the
	// Closes marker — FIFO keeps them before it.
	if err := c.flushWindow(ctx, ref.Target, partition, wo.Seq); err != nil {
		return err
	}
	// The marker's position is the window's own captured position, past every
	// batch the window's read could have seen.
	if err := c.sendClosesPending(ctx, w, ref.Target, pos, wo.Seq, pending, chunkRef, highKey); err != nil {
		return err
	}
	// A worker lost before the marker was sent does not record this window
	// among its lost windows: the marker would reach a worker holding no such
	// window, and the chunk would pass for done. Report the loss so the
	// partition redoes it.
	select {
	case <-lost:
		return errWorkerLost
	default:
		return nil
	}
}
