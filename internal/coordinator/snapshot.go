package coordinator

import (
	"context"
	"fmt"
	"log/slog"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
)

// waitChunkReadyOr blocks until the worker reports the chunk SELECT done for
// THIS epoch, also returning snapshot.ErrWorkerLost when lost closes: the worker's
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
			return 0, snapshot.ErrWorkerLost
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
	c.snapshots.Begin(ref.Target)
	defer c.snapshots.End(ref.Target)

	rt := c.loadRouting()
	owners, ok := rt.OwnersOf(ref.Target)
	if !ok {
		return fmt.Errorf("coordinator: snapshot: no worker owns %s", ref.Target)
	}
	// A partitioned table decouples the chunk's read range (collation) from
	// ownership: the coordinator reads and fans out (snapshot_hash.go). A
	// single owner needs no fan-out and keeps the worker-side path below.
	if len(owners) > 1 {
		return c.snapshotFanoutTable(ctx, rdr, chunker, ref, cfg)
	}
	ranges := rt.RangesOf(ref.Target)
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
	c.snapshots.SetTodo(ref.Target, todo)
	defer c.snapshots.SetTodo(ref.Target, nil)
	var emptyOwners []string
	for p, w := range owners {
		clipped, err := snapshot.ClipChunksToRange(allChunks, ranges[p])
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

// snapshotPartition runs the DBLog snapshot for one partition of one table
// (snapshot.RunRangePartition): only chunks that fall within partitionRange
// are sent to w, and the window/gate lifecycle is scoped to this partition
// alone. allChunks is computed once by snapshotTable (one Bounds query per
// table, not per partition — issue #216).
func (c *Coordinator) snapshotPartition(ctx context.Context, rdr source.SourceReader, allChunks []source.Chunk, ref source.TableRef, partitionRange source.Chunk, partition int, w *workerState, cfg snapshot.SnapshotConfig) error {
	chunks, err := snapshot.ClipChunksToRange(allChunks, partitionRange)
	if err != nil {
		return err
	}
	h := rangeHost{c: c, rdr: rdr, ref: ref, partition: partition, w: w, cfg: cfg}
	return snapshot.RunRangePartition(ctx, h, h.partitionOf(c.snapshots.Todo(ref.Target)), chunks)
}

// snapshotChunk runs one chunk's round-trip (snapshot.RangeChunk).
func (c *Coordinator) snapshotChunk(ctx context.Context, rdr source.SourceReader, ref source.TableRef, partition int, w *workerState, cfg snapshot.SnapshotConfig, ch source.Chunk, chunkID uint32, index int, epoch uint64, lost <-chan struct{}, pending []uint32, windows map[uint64]int) error {
	h := rangeHost{c: c, rdr: rdr, ref: ref, partition: partition, w: w, cfg: cfg}
	return snapshot.RangeChunk(ctx, h, h.partitionOf(nil), ch, chunkID, index, epoch, lost, pending, windows)
}

// rangeHost is the coordinator as one partition's range snapshot sees it
// (snapshot.RangeHost): its worker, its gate, and the worker's replies.
type rangeHost struct {
	c         *Coordinator
	rdr       source.SourceReader
	ref       source.TableRef
	partition int
	w         *workerState
	cfg       snapshot.SnapshotConfig
}

func (h rangeHost) partitionOf(todo map[uint32]bool) snapshot.RangePartition {
	return snapshot.RangePartition{
		Source: h.ref.Source, Partition: h.partition, Worker: h.w.name,
		ChunkTimeout: h.c.cfg.SnapshotChunkTimeout, Todo: todo,
	}
}

func (h rangeHost) AwaitBack(ctx context.Context) error { return h.c.awaitReattached(ctx, h.w) }
func (h rangeHost) Lost() <-chan struct{}               { return h.c.lostSignal(h.w) }

func (h rangeHost) Epoch() uint64 {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	return h.w.epoch
}

func (h rangeHost) Redo(chunks []source.Chunk, windows map[uint64]int, next int) int {
	return h.c.resumeChunkAfterLoss(chunks, h.w, h.ref, h.partition, windows, next)
}

func (h rangeHost) OpenGate(ctx context.Context) error {
	return h.c.openWindowFlushed(ctx, h.ref.Target, h.partition)
}

func (h rangeHost) CloseGate(ctx context.Context) error {
	return h.c.closeWindow(ctx, h.ref.Target, h.partition)
}

func (h rangeHost) AwaitCommits(ctx context.Context, below int, lost <-chan struct{}) error {
	return h.c.awaitChunkCommits(ctx, h.w.name, below, lost)
}

func (h rangeHost) Out() chan<- *pb.CoordinatorMessage {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	return h.w.out
}

func (h rangeHost) WindowOpens() <-chan *pb.WindowOpen  { return h.c.windowOpen }
func (h rangeHost) ChunkReadies() <-chan *pb.ChunkReady { return h.c.chunkReady }

func (h rangeHost) CloseWindow(ctx context.Context, wo *pb.WindowOpen, lost <-chan struct{}, pending []uint32, chunkID uint32, highKey []any) error {
	return h.c.closeOneWindow(ctx, h.rdr, h.ref, h.partition, h.w, h.cfg, wo, lost, pending, chunkID, highKey)
}

func (h rangeHost) Log() *slog.Logger { return h.c.log }

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
	// The window's rows are in the worker's window: from here a full gate
	// drains tagged for it rather than block the pump. Marked ready only
	// after the catch-up, the gate blocked the pump during it, the reader
	// stopped and never reached pos (#821, the #435 stall again).
	c.markWindowReady(ref.Target, partition, wo.Seq)
	// A worker lost while the reader catches up takes its window with it, and
	// its full queue blocks the shared pump so the reader never reaches high;
	// waiting out the window timeout would end the run (issue #526) instead of
	// redoing the chunk, so the wait aborts on the loss.
	if err := snapshot.WaitCaughtUpOrLost(ctx, rdr, pos, cfg, wo.Seq, lost); err != nil {
		return err
	}
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
		return snapshot.ErrWorkerLost
	default:
		return nil
	}
}

// maxUncommittedChunks bounds a worker's snapshot chunks whose Closes marker
// is not yet committed (snapshot.MaxUncommittedChunks has the why).
const maxUncommittedChunks = snapshot.MaxUncommittedChunks

// awaitChunkCommits returns once fewer than below of worker's chunk markers
// are uncommitted, snapshot.ErrWorkerLost when lost closes first (its windows
// died), or when ctx ends (the chunk watchdog).
func (c *Coordinator) awaitChunkCommits(ctx context.Context, worker string, below int, lost <-chan struct{}) error {
	// The index is looked up on every check, as heldChunkMarkers does.
	holds := func(id uint64) bool {
		idx := c.index.Of(worker)
		return idx != nil && idx.Holds(id)
	}
	return c.markers.AwaitBelow(ctx, worker, below, holds, c.ackNotify, lost)
}

// notifyAck wakes awaitChunkCommits waiters without blocking the ack path
// (issue #587). Buffered 1: coalesced, a burst of acks is one wake.
func (c *Coordinator) notifyAck() {
	select {
	case c.ackNotify <- struct{}{}:
	default:
	}
}

// uncommittedChunks drops worker's committed markers and counts the rest.
func (c *Coordinator) uncommittedChunks(worker string) int {
	return len(c.heldChunkMarkers(worker))
}

// heldChunkMarkers drops worker's committed markers and returns the rest,
// oldest first.
func (c *Coordinator) heldChunkMarkers(worker string) []snapshot.Marker {
	idx := c.index.Of(worker)
	return c.markers.Held(worker, func(id uint64) bool { return idx != nil && idx.Holds(id) })
}

// onMarkerAck releases a Closes marker the worker acked by id: the window's
// rows are committed (#468). It advances no position — the window's rows
// carry none.
func (c *Coordinator) onMarkerAck(worker string, id uint64) {
	idx := c.index.Of(worker)
	if idx == nil {
		return
	}
	// Record the chunk's resume cursor BEFORE releasing the marker: a
	// concurrent heldChunkMarkers prunes acked markers, so this must read the
	// marker while it is still held (issue #646).
	c.markers.RecordCursor(worker, id)
	freed, freedOversized, popped := idx.ReleaseMarker(id)
	c.notifyAck()
	if freed > 0 {
		c.budget.Release(worker, freed)
	}
	if freedOversized {
		c.budget.ClearOversized(worker)
	}
	if w := c.workerFor(worker); w != nil {
		w.dropSent(popped)
	}
}
