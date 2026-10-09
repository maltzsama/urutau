package coordinator

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
)

// waitChunkReadyOr blocks until the worker reports the chunk SELECT done for
// THIS epoch, also returning errWorkerLost when lost closes: the worker's
// window died with its session, so no ChunkReady for it will come. A ChunkReady
// from a superseded generation (same table+chunkID, different epoch) is
// ignored, so a stale reply cannot satisfy the wait against a dead window.
func (c *Coordinator) waitChunkReadyOr(ctx context.Context, table string, chunkID uint32, epoch uint64, lost <-chan struct{}) (rows uint64, err error) {
	for {
		select {
		case cr := <-c.chunkReady:
			if cr.Table == table && cr.ChunkId == chunkID && cr.Epoch == epoch {
				return cr.Rows, nil
			}
			c.log.Warn("coordinator: ignoring stale/unexpected ChunkReady",
				"table", cr.Table, "chunk", cr.ChunkId, "epoch", cr.Epoch, "want_epoch", epoch)
		case <-lost:
			return 0, errWorkerLost
		case <-ctx.Done():
			return 0, ctx.Err()
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
	c.beginSnapshot(ref.Target)
	defer c.endSnapshot(ref.Target)

	rt := c.loadRouting()
	owners, ok := rt.ownersOf(ref.Target)
	if !ok {
		return fmt.Errorf("coordinator: snapshot: no worker owns %s", ref.Target)
	}
	// A partitioned table decouples the chunk's read range (collation) from
	// ownership: the coordinator reads and fans out (snapshot_hash.go). A
	// single owner needs no fan-out and keeps the worker-side path below.
	if len(owners) > 1 {
		return c.snapshotFanoutTable(ctx, rdr, chunker, ref, cfg)
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
	allChunks, todo, err := c.snapshotPlan(ctx, chunker, ref, ranges)
	if err != nil {
		return err
	}
	c.setSnapshotTodo(ref.Target, todo)
	defer c.setSnapshotTodo(ref.Target, nil)
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
	// A worker lost mid-snapshot takes its windows with it (issue #461): the
	// rows of every chunk whose Closes marker it had not committed lived only
	// in its memory. Once it is back, the partition redoes from the first
	// such chunk. Window ids are the worker's own (attempt, seq), announced in
	// WindowOpen, so a lost window's Closes marker, redelivered to the
	// reconnected worker, closes nothing it has open.
	opened := false
	windows := map[uint64]int{} // window seq → chunk index, this partition
	var epoch uint64
	haveEpoch := false
	todo := c.snapshotTodoFor(ref.Target)
	for i := 0; i < len(chunks); {
		if err := ctx.Err(); err != nil {
			return err
		}
		if todo != nil && !todo[chunkRef(partition, i)] {
			i++ // committed before a coordinator restart: resumed past
			continue
		}
		if err := c.awaitReattached(ctx, w); err != nil {
			return err
		}
		lost := c.lostSignal(w)
		c.mu.Lock()
		cur := w.epoch
		c.mu.Unlock()
		if haveEpoch && cur != epoch {
			// Lost (and back) since the previous round-trip.
			i = c.resumeChunkAfterLoss(chunks, w, ref, partition, windows, i)
			c.log.Warn("coordinator: snapshot worker was lost; redoing its uncommitted chunks",
				"table", ref.Source, "partition", partition, "worker", w.name, "from_chunk", i)
		}
		// The epoch the ChunkRequests are sent under; the worker echoes it
		// on ChunkReady and WindowOpen so a reply from a superseded
		// generation is ignored.
		epoch, haveEpoch = cur, true
		if !opened {
			if err := c.openWindowFlushed(ctx, ref.Target, partition); err != nil {
				return err
			}
			opened = true
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
		err := c.awaitChunkCommits(chunkCtx, w.name, maxUncommittedChunks, lost)
		if err == nil {
			err = c.snapshotChunk(chunkCtx, rdr, ref, partition, w, cfg, chunks[i], chunkRef(partition, i), i, epoch, lost, pendingAfter(todo, partition, i), windows)
		}
		if err == nil && i == len(chunks)-1 {
			// The partition's last chunk: its windows must all be committed
			// before the partition is done, or a loss right after would
			// leave rows only a lost window held.
			err = c.awaitChunkCommits(chunkCtx, w.name, 1, lost)
		}
		cancel()
		if errors.Is(err, errWorkerLost) {
			continue // the next pass finds the new epoch and redoes
		}
		if err != nil {
			// A parent deadline/cancel (the run's own) surfaces here as
			// DeadlineExceeded too; report it as-is rather than blaming a
			// wedged worker. Only a chunk deadline with a live parent is the
			// watchdog firing.
			if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("coordinator: snapshot %s: chunk %d did not complete within %s (the worker may be wedged): %w",
					ref.Source, i, timeout, err)
			}
			return err
		}
		i++
	}
	// Seal this partition's gate and release anything collected after its
	// last chunk. Other partitions' gates (if any) are untouched.
	return c.closeWindow(ctx, ref.Target, partition)
}

// snapshotChunk runs one chunk's round-trip: send the ChunkRequest, wait for
// the worker's WindowOpen announcements and ChunkReady, proving the reader
// caught up and releasing each window's gated live rows and Closes marker.
// ctx carries the watchdog deadline, so a worker that never acks the chunk
// fails the run instead of wedging it.
func (c *Coordinator) snapshotChunk(ctx context.Context, rdr source.SourceReader, ref source.TableRef, partition int, w *workerState, cfg snapshot.SnapshotConfig, ch source.Chunk, chunkID uint32, index int, epoch uint64, lost <-chan struct{}, pending []uint32, windows map[uint64]int) error {
	boundsB, err := transport.EncodeBounds(ch.Low, ch.High)
	if err != nil {
		return fmt.Errorf("coordinator: chunk %d bounds: %w", chunkID, err)
	}
	req := &pb.ChunkRequest{Table: ref.Source, ChunkId: chunkID, Bounds: boundsB}

	c.mu.Lock()
	out := w.out
	c.mu.Unlock()
	select {
	case out <- &pb.CoordinatorMessage{Msg: &pb.CoordinatorMessage_Chunk{Chunk: req}}:
	case <-lost:
		return errWorkerLost
	case <-ctx.Done():
		return ctx.Err()
	}

	// handleWindowOpen closes one announced window: prove caught up to its
	// position, release its gated live rows, send its Closes.
	handleWindowOpen := func(wo *pb.WindowOpen) error {
		if wo.Table != ref.Source || wo.ChunkId != chunkID || wo.Attempt != epoch {
			c.log.Warn("coordinator: ignoring stale/unexpected WindowOpen",
				"table", wo.Table, "chunk", wo.ChunkId, "attempt", wo.Attempt, "want_epoch", epoch)
			return nil
		}
		windows[wo.Seq] = index
		highKey, err := decodeWindowHighKey(wo.HighKey)
		if err != nil {
			return fmt.Errorf("coordinator: window %d high key: %w", wo.Seq, err)
		}
		return c.closeOneWindow(ctx, rdr, ref, partition, w, cfg, wo, lost, pending, chunkID, highKey)
	}

	// The worker streams WindowOpen as it reads the chunk (byte-cap), then
	// ChunkReady. Close each window as it opens, in order.
	for {
		select {
		case wo := <-c.windowOpen:
			if err := handleWindowOpen(wo); err != nil {
				return err
			}
		case cr := <-c.chunkReady:
			if cr.Table == ref.Source && cr.ChunkId == chunkID && cr.Epoch == epoch {
				c.log.Info("chunk ready", "table", ref.Source, "partition", partition, "chunk", chunkID, "rows", cr.Rows)
				// WindowOpen and ChunkReady arrive on SEPARATE channels, so
				// the select above can receive ChunkReady while earlier
				// WindowOpen messages are still buffered. The worker sends
				// every WindowOpen before ChunkReady, so drain and close the
				// buffered ones now — otherwise their gated live rows are
				// never released and their windows never commit.
				for {
					select {
					case wo := <-c.windowOpen:
						if err := handleWindowOpen(wo); err != nil {
							return err
						}
					default:
						return nil
					}
				}
			}
			c.log.Warn("coordinator: ignoring stale/unexpected ChunkReady",
				"table", cr.Table, "chunk", cr.ChunkId, "epoch", cr.Epoch, "want_epoch", epoch)
		case <-lost:
			return errWorkerLost
		case <-ctx.Done():
			return ctx.Err()
		}
	}
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
