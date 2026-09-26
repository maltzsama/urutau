package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
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
// window of every partition. A table that was never sent anything had no
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
