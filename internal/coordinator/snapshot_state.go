package coordinator

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// Snapshot completion is durable state, not "the table has a position"
// (issue #428). The live stream commits to every table while the snapshot
// copies them one at a time, so a table can hold a cdc.position before its
// snapshot has run — or while its last windows are still uncommitted. A
// crash then must not take that position for a finished snapshot:
//
//   - at boot, every table about to be snapshotted is marked
//     cdc.snapshot.state=not_started before the stream starts;
//   - a table found not_started or in_progress at boot is snapshotted
//     again, position or not;
//   - complete is committed by the table's own writer, after every window,
//     by the snapshot-done marker (finishSnapshot).
//
// A table with a position and no state predates this marking and keeps the
// old reading: its snapshot is taken as done.

// snapshotInterrupted reports whether a table with a committed position was
// left mid-snapshot by an earlier run.
func (c *Coordinator) snapshotInterrupted(ctx context.Context, ref core.TableRef) (bool, error) {
	props, err := c.snk.Properties(ctx, ref)
	if err != nil {
		return false, fmt.Errorf("coordinator: %s: snapshot state: %w", ref.Target, err)
	}
	return snapshot.Unfinished(props), nil
}

// markSnapshotsPending records, before the stream can commit anything, that
// each table's snapshot has not finished. A table already marked unfinished
// keeps its state (a collapsed run's in_progress carries resumable bounds).
func (c *Coordinator) markSnapshotsPending(ctx context.Context, refs []source.TableRef) error {
	for _, ref := range refs {
		tref := core.TableRef{Source: ref.Source, Target: ref.Target}
		props, err := c.snk.Properties(ctx, tref)
		if err != nil {
			return fmt.Errorf("coordinator: %s: snapshot state: %w", ref.Target, err)
		}
		if snapshot.Unfinished(props) {
			continue
		}
		mark := map[string]string{snapshot.PropSnapshotState: string(snapshot.StateNotStarted)}
		if err := c.snk.SetProperties(ctx, tref, mark); err != nil {
			return fmt.Errorf("coordinator: %s: mark snapshot pending: %w", ref.Target, err)
		}
	}
	return nil
}

// noteSent records the latest position sent for a table (FIFO, so the last
// one sent is the highest).
func (c *Coordinator) noteSent(table, pos string) {
	if pos == "" {
		return
	}
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	if c.lastSent == nil {
		c.lastSent = map[string]string{}
	}
	c.lastSent[table] = pos
}

// noteWindow records the worker a table's latest window went to.
func (c *Coordinator) noteWindow(table string, w *workerState) {
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	if c.lastWindow == nil {
		c.lastWindow = map[string]*workerState{}
	}
	c.lastWindow[table] = w
}

func (c *Coordinator) sentState(table string) (pos string, w *workerState) {
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	return c.lastSent[table], c.lastWindow[table]
}

// finishSnapshot commits a table's snapshot completion once every window has
// been sent. The done marker goes to the worker that got the last window,
// behind everything sent to it, so it commits after that window; on a staged
// table it is its own cycle of the send order, so it commits after every
// window of every partition. Its cycle expects only that worker's delivery:
// what holds it back is the table's send order, which commits cycles
// head-first whatever worker owns them, not its owner set. A table that was never sent anything had no
// rows to copy and no window to wait for: its completion is written directly.
func (c *Coordinator) finishSnapshot(ctx context.Context, ref source.TableRef) error {
	pos, w := c.sentState(ref.Target)
	if pos == "" || w == nil {
		props := map[string]string{snapshot.PropSnapshotState: string(snapshot.StateComplete)}
		if err := c.snk.SetProperties(ctx, core.TableRef{Source: ref.Source, Target: ref.Target}, props); err != nil {
			return fmt.Errorf("coordinator: %s: mark snapshot complete: %w", ref.Target, err)
		}
		return nil
	}
	meta := &pb.BatchMeta{
		Table:  ref.Target,
		LowPos: pos,
		Window: &pb.WindowTag{SnapshotDone: true},
	}
	if c.stagesCycles() && c.isStagedTable(ref.Target) {
		meta.BatchId = c.batchSeq.Add(1)
		c.staged.expect(core.TableRef{Target: ref.Target}, meta.BatchId, []string{w.name})
	}
	return c.enqueueTo(ctx, w, nil, meta)
}

// propSnapshotPartitions records the partition ranges a table's in-progress
// snapshot was chunked for: its pending chunk ids index chunks clipped to
// those ranges, so progress recorded under other ranges cannot be resumed
// (issue #461).
const propSnapshotPartitions = "cdc.snapshot.partitions"

// chunkRef is a snapshot chunk's durable id: its partition in the high bits,
// its index among that partition's clipped chunks in the low 20.
func chunkRef(partition, index int) uint32 { return uint32(partition)<<20 | uint32(index) }

// partitionLayout renders partition ranges as recorded with the progress.
func partitionLayout(ranges []source.Chunk) string {
	return fmt.Sprintf("%d:%v", len(ranges), ranges)
}

// snapshotPlan returns the chunks a table's snapshot covers and which of them
// are still to do. An in-progress snapshot recorded under the same partition
// ranges resumes: its bounds are reused and only its pending chunks remain.
// Otherwise the source's bounds are read afresh, and the new snapshot's
// progress — bounds, ranges and every chunk pending — is recorded before its
// first chunk; each window then commits the chunks still to do after it.
func (c *Coordinator) snapshotPlan(ctx context.Context, chunker source.ChunkSource, ref source.TableRef, ranges []source.Chunk) ([]source.Chunk, map[uint32]bool, error) {
	tref := core.TableRef{Source: ref.Source, Target: ref.Target}
	layout := partitionLayout(ranges)
	if c.snk != nil {
		props, err := c.snk.Properties(ctx, tref)
		if err != nil {
			return nil, nil, fmt.Errorf("coordinator: %s: snapshot progress: %w", ref.Target, err)
		}
		sp, perr := snapshot.ReadSnapshotProgress(props)
		if perr == nil && sp.State == snapshot.StateInProgress && len(sp.Bounds) > 0 && props[propSnapshotPartitions] == layout {
			all := snapshot.Chunks(sp.Bounds)
			todo := map[uint32]bool{}
			for _, id := range sp.Pending {
				todo[id] = true
			}
			if len(todo) == 0 {
				todo = allChunkRefs(all, ranges)
			}
			c.log.Info("coordinator: resuming the snapshot from its recorded progress",
				"table", ref.Target, "chunks_pending", len(todo))
			return all, todo, nil
		}
	}
	bounds, err := chunker.Bounds(ctx)
	if err != nil {
		return nil, nil, err
	}
	all := snapshot.Chunks(bounds)
	todo := allChunkRefs(all, ranges)
	if c.snk != nil {
		props := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{
			State: snapshot.StateInProgress, Bounds: bounds, Pending: sortedRefs(todo),
			Started: time.Now().UTC().Format(time.RFC3339),
		})
		props[propSnapshotPartitions] = layout
		if err := c.snk.SetProperties(ctx, tref, props); err != nil {
			return nil, nil, fmt.Errorf("coordinator: %s: record snapshot progress: %w", ref.Target, err)
		}
	}
	return all, todo, nil
}

// allChunkRefs is every chunk of every partition.
func allChunkRefs(all []source.Chunk, ranges []source.Chunk) map[uint32]bool {
	todo := map[uint32]bool{}
	for p, r := range ranges {
		clipped, err := clipChunksToRange(all, r)
		if err != nil {
			continue
		}
		for i := range clipped {
			todo[chunkRef(p, i)] = true
		}
	}
	return todo
}

// sortedRefs lists chunk ids in order.
func sortedRefs(todo map[uint32]bool) []uint32 {
	out := make([]uint32, 0, len(todo))
	for id := range todo {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// pendingAfter is the chunks still to do once partition's chunk index is
// committed: later chunks of the same partition, and every chunk of the
// partitions after it, among todo.
func pendingAfter(todo map[uint32]bool, partition, index int) []uint32 {
	var out []uint32
	for _, id := range sortedRefs(todo) {
		p, i := int(id>>20), int(id&(1<<20-1))
		if p > partition || (p == partition && i > index) {
			out = append(out, id)
		}
	}
	return out
}

// setSnapshotTodo records the chunks still to do of the table being
// snapshotted; nil clears it.
func (c *Coordinator) setSnapshotTodo(target string, todo map[uint32]bool) {
	c.snapshotTodoMu.Lock()
	defer c.snapshotTodoMu.Unlock()
	if todo == nil {
		delete(c.snapshotTodo, target)
		return
	}
	if c.snapshotTodo == nil {
		c.snapshotTodo = map[string]map[uint32]bool{}
	}
	c.snapshotTodo[target] = todo
}

// snapshotTodoFor returns the table's chunks still to do, or nil (every chunk).
func (c *Coordinator) snapshotTodoFor(target string) map[uint32]bool {
	c.snapshotTodoMu.Lock()
	defer c.snapshotTodoMu.Unlock()
	return c.snapshotTodo[target]
}

// sendClosesPending queues a chunk's Closes marker for its partition's worker
// — not routed through enqueueBatch's table-wide lookup, since a marker carries
// no rows for enqueueBatch to route by key. pending names the table's snapshot
// chunks still to do after this window, which the worker commits with the
// window's rows so a restarted coordinator resumes there (issue #461).
//
// On a staged table the worker delivers the window's rows as the marker's
// cycle (#416), so the marker takes a place in the table's send order here,
// expecting only this worker: the window commits after every live cycle
// released ahead of it. Committed on arrival instead, it could move the
// table's committed position past live cycles still open, and a crash would
// then take their replay for covered.
func (c *Coordinator) sendClosesPending(ctx context.Context, w *workerState, target string, at position.Position, windowID uint64, pending []uint32) error {
	meta := &pb.BatchMeta{
		Table:  target,
		LowPos: at.String(),
		Window: &pb.WindowTag{Closes: true, WindowId: windowID, SnapshotPending: pending},
	}
	if c.stagesCycles() && c.isStagedTable(target) {
		meta.BatchId = c.batchSeq.Add(1)
		c.staged.expectWindow(core.TableRef{Target: target}, meta.BatchId, []string{w.name})
	}
	if err := c.enqueueTo(ctx, w, nil, meta); err != nil {
		return err
	}
	c.noteChunkMarker(w.name, meta.BatchId, target, windowID)
	c.noteWindow(target, w)
	return nil
}
