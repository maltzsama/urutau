package coordinator

import (
	"context"
	"errors"
	"fmt"
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

// noteWindow records the worker a table's latest window went to.
func (c *Coordinator) noteWindow(table string, w *workerState) {
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	if c.lastWindow == nil {
		c.lastWindow = map[string]*workerState{}
	}
	c.lastWindow[table] = w
}

// sentState returns the latest position sent for a table and the worker its
// latest window went to.
func (c *Coordinator) sentState(table string) (pos string, w *workerState) {
	c.sentMu.Lock()
	defer c.sentMu.Unlock()
	return c.sent.Of(table), c.lastWindow[table]
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
		if err := snapshot.MarkComplete(ctx, c.snk, ref); err != nil {
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
		c.staged.Expect(core.TableRef{Target: ref.Target}, meta.BatchId, []string{w.name})
	}
	return c.enqueueTo(ctx, w, nil, meta)
}

// snapshotPlan returns the chunks a table's snapshot covers and which of them
// are still to do: an in-progress snapshot recorded under the same partition
// ranges resumes, any other starts afresh and records its progress
// (snapshot.PlanRange).
func (c *Coordinator) snapshotPlan(ctx context.Context, chunker source.ChunkSource, ref source.TableRef, ranges []source.Chunk) ([]source.Chunk, map[uint32]bool, error) {
	// A nil sink must stay a nil store: a typed nil in the interface would
	// pass PlanRange's check.
	var store snapshot.ProgressStore
	if c.snk != nil {
		store = c.snk
	}
	plan, err := snapshot.PlanRange(ctx, store, chunker, core.TableRef{Source: ref.Source, Target: ref.Target}, ranges, time.Now())
	if err != nil {
		var pe *snapshot.PlanError
		if errors.As(err, &pe) {
			return nil, nil, fmt.Errorf("coordinator: %s: %s: %w", ref.Target, pe.Op, pe.Err)
		}
		return nil, nil, err
	}
	if plan.Resumed {
		c.log.Info("coordinator: resuming the snapshot from its recorded progress",
			"table", ref.Target, "chunks_pending", len(plan.Todo))
	}
	return plan.Chunks, plan.Todo, nil
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
func (c *Coordinator) sendClosesPending(ctx context.Context, w *workerState, target string, at position.Position, windowID uint64, pending []uint32, chunkRef uint32, highKey []any) error {
	meta := &pb.BatchMeta{
		Table:  target,
		LowPos: at.String(),
		Window: &pb.WindowTag{Closes: true, WindowId: windowID, SnapshotPending: pending},
	}
	if c.stagesCycles() && c.isStagedTable(target) {
		meta.BatchId = c.batchSeq.Add(1)
		c.staged.ExpectWindow(core.TableRef{Target: target}, meta.BatchId, []string{w.name})
	}
	if err := c.enqueueTo(ctx, w, nil, meta); err != nil {
		return err
	}
	c.markers.Note(w.name, snapshot.Marker{ID: meta.BatchId, Target: target, Window: windowID, ChunkRef: chunkRef, HighKey: highKey})
	c.noteWindow(target, w)
	return nil
}
