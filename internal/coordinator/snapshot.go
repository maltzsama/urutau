package coordinator

import (
	"context"
	"errors"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
)

// waitChunkReady blocks until the worker reports the chunk SELECT done for
// THIS epoch. A ChunkReady from a superseded generation (same table+chunkID,
// different epoch) is ignored, so a stale reply cannot satisfy the wait
// against a dead window.
func (c *Coordinator) waitChunkReady(ctx context.Context, table string, chunkID uint32, epoch uint64) error {
	for {
		select {
		case cr := <-c.chunkReady:
			if cr.Table == table && cr.ChunkId == chunkID && cr.Epoch == epoch {
				return nil
			}
			c.log.Warn("coordinator: ignoring stale/unexpected ChunkReady",
				"table", cr.Table, "chunk", cr.ChunkId, "epoch", cr.Epoch, "want_epoch", epoch)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// snapshotTable runs the DBLog snapshot for one table with the chunk SELECT
// executed by the worker (design §3.1): for each chunk the coordinator pauses
// the pump (openWindow), sends ChunkRequest bounds, waits ChunkReady, proves
// caught-up, then releases the gated live events InWindow-tagged and the
// Closes marker. The worker holds the chunk rows in its window; the window
// is what InWindow events drain and the Closes marker flushes.
func (c *Coordinator) snapshotTable(ctx context.Context, rdr source.SourceReader, chunker source.ChunkSource, ref source.TableRef, cfg snapshot.SnapshotConfig) error {
	rt := c.loadRouting()
	owners, ok := rt.ownersOf(ref.Target)
	if !ok {
		return fmt.Errorf("coordinator: snapshot: no worker owns %s", ref.Target)
	}
	ranges := rt.rangesOf(ref.Target)
	if len(ranges) != len(owners) {
		return fmt.Errorf("coordinator: snapshot: table %s: %d partition ranges for %d owners", ref.Target, len(ranges), len(owners))
	}

	// One partition at a time: all partitions of a table share the same
	// replication reader (rdr) — there is one binlog/WAL connection per
	// pipeline, not per partition — so this is sequential I/O today, not
	// parallel. Each partition still gets its own correct, independent
	// window (design requirement; the parallelism this feature is FOR is
	// steady-state live-stream throughput, which the per-partition
	// worker processes already give — see enqueueBatch's PK-range split).
	// A future iteration could parallelize the chunk SELECTs themselves
	// (they run on the WORKER, not rdr) while keeping rdr's caught-up
	// proof sequential; not needed for this to be correct.
	//
	// A partition whose range has no rows gets no chunks and its owner never
	// commits, so it would never record a position. Collect those owners and
	// seed their baseline AFTER the snapshot — here "empty" is provable (no
	// chunks at snapshot time), unlike at boot, where an owner with no
	// position could equally be one interrupted mid-snapshot (whose partition
	// MUST re-snapshot, not be resumed past). WK-001 §2.6.
	bounds, err := chunker.Bounds(ctx)
	if err != nil {
		return err
	}
	allChunks := snapshot.Chunks(bounds)
	var emptyOwners []string
	for p, w := range owners {
		clipped, err := clipChunksToRange(allChunks, ranges[p])
		if err != nil {
			return fmt.Errorf("partition %d: %w", p, err)
		}
		if len(clipped) == 0 {
			emptyOwners = append(emptyOwners, w.name)
		}
		if err := c.snapshotPartition(ctx, rdr, allChunks, ref, ranges[p], p, w, cfg); err != nil {
			return fmt.Errorf("partition %d: %w", p, err)
		}
	}
	if len(emptyOwners) > 0 {
		if seeder, ok := c.snk.(sink.PositionSeeder); ok {
			if err := seeder.SeedPositions(ctx, core.TableRef{Target: ref.Target}, emptyOwners); err != nil {
				return fmt.Errorf("coordinator: seed %s: %w", ref.Target, err)
			}
		}
	}
	return nil
}

// snapshotPartition runs the DBLog snapshot for one partition of one
// table: only chunks that fall within partitionRange are sent to w, and
// the window/gate lifecycle (openWindow/flushWindow/closeWindow) is
// scoped to this partition alone, so a different partition's concurrent
// snapshot (if any) is never gated or released by this one's chunks.
// allChunks is computed once by snapshotTable (one Bounds query per table,
// not per partition — issue #216).
func (c *Coordinator) snapshotPartition(ctx context.Context, rdr source.SourceReader, allChunks []source.Chunk, ref source.TableRef, partitionRange source.Chunk, partition int, w *workerState, cfg snapshot.SnapshotConfig) error {
	chunks, err := clipChunksToRange(allChunks, partitionRange)
	if err != nil {
		return err
	}
	if len(chunks) == 0 {
		return nil // this partition's range contains no rows right now
	}
	// The epoch the ChunkRequests are sent under; the worker echoes it on
	// ChunkReady so a reply from a superseded generation is ignored.
	c.mu.Lock()
	epoch := w.epoch
	c.mu.Unlock()

	for i, ch := range chunks {
		chunkID := uint32(i)
		if err := ctx.Err(); err != nil {
			return err
		}
		if i == 0 {
			c.openWindow(ref.Target, partition)
		}
		// The snapshot watchdog: a worker that attaches but stops draining
		// would otherwise block the chunk round-trip (send, wait, flush)
		// forever — the supervisor does not run during the snapshot (issue
		// #206). The deadline must outlast WaitCaughtUp's own window timeout,
		// or a slow-but-healthy catch-up would look wedged (Config doc).
		timeout := c.cfg.SnapshotChunkTimeout
		if timeout <= 0 {
			timeout = defaultSnapshotChunkTimeout
		}
		chunkCtx, cancel := context.WithTimeout(ctx, timeout)
		err := c.snapshotChunk(chunkCtx, rdr, ref, partition, w, cfg, ch, chunkID, epoch)
		cancel()
		if err != nil {
			// A parent deadline/cancel (the run's own) surfaces here as
			// DeadlineExceeded too; report it as-is rather than blaming a
			// wedged worker. Only a chunk deadline with a live parent is the
			// watchdog firing.
			if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("coordinator: snapshot %s: chunk %d did not complete within %s (the worker may be wedged): %w",
					ref.Source, chunkID, timeout, err)
			}
			return err
		}
	}
	// Seal this partition's gate and release anything collected after its
	// last chunk. Other partitions' gates (if any) are untouched.
	return c.closeWindow(ctx, ref.Target, partition)
}

// snapshotChunk runs one chunk's round-trip: send the ChunkRequest, wait for
// the worker's ChunkReady, prove the reader caught up, then release the gated
// live rows and the Closes marker. ctx carries the watchdog deadline, so a
// worker that never acks the chunk fails the run instead of wedging it.
func (c *Coordinator) snapshotChunk(ctx context.Context, rdr source.SourceReader, ref source.TableRef, partition int, w *workerState, cfg snapshot.SnapshotConfig, ch source.Chunk, chunkID uint32, epoch uint64) error {
	boundsB, err := transport.EncodeBounds(ch.Low, ch.High)
	if err != nil {
		return fmt.Errorf("coordinator: chunk %d bounds: %w", chunkID, err)
	}
	req := &pb.ChunkRequest{Table: ref.Source, ChunkId: chunkID, Bounds: boundsB}

	select {
	case w.out <- &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Chunk{Chunk: req}}:
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := c.waitChunkReady(ctx, ref.Source, chunkID, epoch); err != nil {
		return err
	}
	c.log.Info("chunk ready", "table", ref.Source, "partition", partition, "chunk", chunkID)

	// The worker has the chunk rows in its window; prove the reader is
	// caught up before releasing anything that touches this window. The
	// high watermark is the source's FIXED position after the SELECT —
	// never a live master — so a busy source cannot keep the window open
	// forever.
	high, err := rdr.Master(ctx)
	if err != nil {
		return fmt.Errorf("dblog: chunk %d: master: %w", chunkID, err)
	}
	if err := snapshot.WaitCaughtUp(ctx, rdr, high, cfg); err != nil {
		return fmt.Errorf("dblog: chunk %d: %w", chunkID, err)
	}
	at := rdr.Synced()

	// Release this chunk's gated live events (InWindow-tagged) ahead of
	// the Closes marker — FIFO keeps them before it. The gate stays
	// open: the next chunk's backlog must not race ahead of these.
	if err := c.flushWindow(ctx, ref.Target, partition, chunkID); err != nil {
		return err
	}
	return c.sendCloses(ctx, w, ref.Target, at, chunkID)
}

// sendCloses queues a chunk's Closes marker for its partition's worker —
// not routed through enqueueBatch's table-wide lookup, since a marker carries
// no rows for enqueueBatch to route by key.
//
// On a staged table the worker delivers the window's rows as the marker's
// cycle (#416), so the marker takes a place in the table's send order here,
// expecting only this worker: the window commits after every live cycle
// released ahead of it. Committed on arrival instead, it could move the
// table's committed position past live cycles still open, and a crash would
// then take their replay for covered.
func (c *Coordinator) sendCloses(ctx context.Context, w *workerState, target string, at position.Position, chunkID uint32) error {
	meta := &pb.BatchMeta{
		Table:  target,
		LowPos: at.String(),
		Window: &pb.WindowTag{Closes: true, ChunkId: chunkID},
	}
	if c.stagesCycles() && c.isStagedTable(target) {
		meta.BatchId = c.batchSeq.Add(1)
		c.staged.expect(core.TableRef{Target: target}, meta.BatchId, []string{w.name})
	}
	if err := c.enqueueTo(ctx, w, nil, meta); err != nil {
		return err
	}
	c.noteWindow(target, w)
	return nil
}

// clipChunksToRange keeps only the chunks that intersect partitionRange,
// clamping each kept chunk's own Low/High to the range's bounds so a
// chunk straddling the partition boundary never sends rows outside it.
// An empty partitionRange (the unpartitioned {} zero value) matches
// everything unchanged.
func clipChunksToRange(chunks []source.Chunk, partitionRange source.Chunk) ([]source.Chunk, error) {
	if partitionRange.Low == nil && partitionRange.High == nil {
		return chunks, nil
	}
	var out []source.Chunk
	for _, ch := range chunks {
		if partitionRange.High != nil && ch.Low != nil {
			c, err := comparePK(ch.Low, partitionRange.High)
			if err != nil {
				return nil, err
			}
			if c >= 0 {
				continue // chunk starts at/after the range ends
			}
		}
		if partitionRange.Low != nil && ch.High != nil {
			c, err := comparePK(ch.High, partitionRange.Low)
			if err != nil {
				return nil, err
			}
			if c <= 0 {
				continue // chunk ends at/before the range starts — both are half-open [Low,High)
			}
		}
		clipped := ch
		if partitionRange.Low != nil {
			c := -1
			if ch.Low != nil {
				var err error
				if c, err = comparePK(ch.Low, partitionRange.Low); err != nil {
					return nil, err
				}
			}
			if c < 0 {
				clipped.Low = partitionRange.Low
			}
		}
		if partitionRange.High != nil {
			c := 1
			if ch.High != nil {
				var err error
				if c, err = comparePK(ch.High, partitionRange.High); err != nil {
					return nil, err
				}
			}
			if c > 0 {
				clipped.High = partitionRange.High
			}
		}
		out = append(out, clipped)
	}
	return out, nil
}
