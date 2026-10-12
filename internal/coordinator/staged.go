package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/columnar"
	"github.com/maltzsama/urutau/internal/gate"
	"github.com/maltzsama/urutau/internal/staged"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
)

// stagesCycles reports whether this run's sink commits staged cycles: only
// then do a partitioned table's sub-batches form cycles the coordinator
// groups and commits.
func (c *Coordinator) stagesCycles() bool {
	_, ok := c.snk.(sink.StagedCommitter)
	return ok
}

// isStagedTable reports whether target's commits are owned by the
// coordinator's staged cycle — a partitioned table on a staging sink. On such
// a table the worker's ack precedes the durable commit, so it must NOT
// advance the confirmed position; commitStagedCycle does, after the cycle is
// durable (WK-001 §2.2/F2).
func (c *Coordinator) isStagedTable(target string) bool {
	return c.stagesCycles() && c.ownerCount(target) > 1
}

// onStagedBatch handles one StagedBatch from a worker (WK-001 C5):
// staged.Cycles.OnDelivery records it and commits every cycle that is now
// complete and in turn, through stagedHost.
func (c *Coordinator) onStagedBatch(worker string, sb *pb.StagedBatch) {
	if sb == nil {
		return
	}
	c.staged.OnDelivery(stagedHost{c}, &c.stagedLocks, worker, staged.Delivery{
		Table: sb.Table, Seq: sb.Seq, Epoch: sb.Epoch, Descriptor: sb.Descriptor_, Position: sb.Position,
		SnapshotState: sb.SnapshotState, SnapshotPending: sb.SnapshotPending, WindowLost: sb.WindowLost,
	})
}

// commitStagedCycle commits one complete cycle, taking the table's staged
// lock. Kept for callers that do not already hold it (tests, one-off paths).
func (c *Coordinator) commitStagedCycle(cy *staged.Cycle) error {
	if err := c.staged.Commit(stagedHost{c}, &c.stagedLocks, cy); err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	return nil
}

// stagedHost is the coordinator as a staged commit sees it
// (staged.CommitHost).
type stagedHost struct{ c *Coordinator }

// Epoch reads the worker and its generation under c.mu: resetWorker writes
// w.epoch under c.mu from the supervisor and dashboard goroutines, so an
// unlocked read is a data race (issue #207).
func (h stagedHost) Epoch(worker string) (uint64, bool) {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	w := h.c.workers[worker]
	if w == nil {
		return 0, false
	}
	return w.epoch, true
}

func (h stagedHost) ParsePosition(s string) (position.Position, error) {
	return h.c.src.ParsePosition(s)
}

func (h stagedHost) DropWindow(target string, seq uint64, lost bool) bool {
	return h.c.fanout.Resolve(target, seq, lost)
}

func (h stagedHost) CommitStaged(ref core.TableRef, descriptors [][]byte, pos string) error {
	committer, ok := h.c.snk.(sink.StagedCommitter)
	if !ok {
		return errors.New("sink does not stage commits")
	}
	return committer.CommitStaged(h.c.runCtx, ref, descriptors, pos)
}

// Committed records the cycle's position for every owner it covered, so
// confirmedPosition (the min) reflects the whole cycle, and releases what the
// cycle made durable from every owner's in-flight index.
func (h stagedHost) Committed(cy *staged.Cycle, p position.Position) {
	for owner := range cy.Owners {
		h.c.recordConfirmed(owner, p)
	}
	h.c.releaseCommittedCycles(cy.Ref.Target, p)
}

func (h stagedHost) Fail(err error)    { h.c.fail(fmt.Errorf("coordinator: %w", err)) }
func (h stagedHost) Log() *slog.Logger { return h.c.log }

// releaseCommittedCycles takes out of every owner's in-flight index the
// table's batches a cycle committed at p covers. A table's cycles commit in
// order, so everything of it before p is durable for all its owners, whether
// or not they had rows in this cycle. An owner's ack of its own staged share
// carries that share's position, which does not release a partial batch
// (#823): the owner that had rows in a transaction's earlier batches and none
// in its last would hold them in flight until its next rows.
func (c *Coordinator) releaseCommittedCycles(table string, p position.Position) {
	owners, _ := c.loadRouting().OwnersOf(table)
	for _, w := range owners {
		idx := c.index.Of(w.name)
		if idx == nil {
			continue
		}
		freed, freedOversized, popped := idx.Truncate(table, p)
		if freed > 0 {
			c.budget.Release(w.name, freed)
		}
		if freedOversized {
			c.budget.ClearOversized(w.name)
		}
		w.dropSent(popped)
	}
	c.notifyAck()
}

// fail reports a terminal coordinator error without blocking if one is
// already pending.
func (c *Coordinator) fail(err error) {
	select {
	case c.terminate <- err:
	default:
	}
}

// Consecutive batches of a staged table are held in a per-table accumulator
// and sent as one cycle (staged.Coalescer has the why and the bounds). This
// file is the coordinator's side: which batches are taken, and the points
// where the accumulators are sent to keep the table's order — before a DBLog
// window opens to new batches (openWindowFlushed), before a re-slice pause
// holds its first batch (flushAccumBeforePause), and on the pump's tick.

// cycleCheckEvery is how often the pump checks the accumulators it did not
// flush on arrival.
const cycleCheckEvery = 20 * time.Millisecond

// cycleBounds are the row, byte and age bounds of one cycle.
func (c *Coordinator) cycleBounds() staged.Bounds {
	return staged.NewBounds(c.cfg.CycleMaxRows, c.cfg.CycleMaxBytes, c.cfg.CycleMaxAge)
}

// batchBytes is a batch's in-memory size, the byte bound's measure.
func batchBytes(b *dataplane.Batch) int64 { return columnar.BatchBytes(b) }

// coalesceHost is the coordinator as the accumulators see it.
type coalesceHost struct{ c *Coordinator }

func (h coalesceHost) Gate() *gate.Gate[*dataplane.Batch] { return h.c.gate }
func (h coalesceHost) Bounds() staged.Bounds              { return h.c.cycleBounds() }
func (h coalesceHost) Size(b *dataplane.Batch) int64      { return batchBytes(b) }
func (h coalesceHost) OpenCycles(table string) int {
	return h.c.staged.OpenFor(core.TableRef{Target: table})
}
func (h coalesceHost) Send(ctx context.Context, b *dataplane.Batch) error {
	return h.c.enqueueBatch(ctx, b, nil)
}

// accumulate takes a live batch of a staged table into its accumulator and
// reports whether it did. A batch of any other table is not taken: it is
// enqueued on its own, as before. A window that opened since gateHold looked
// takes the batch into its gate instead.
func (c *Coordinator) accumulate(ctx context.Context, b *dataplane.Batch) (bool, error) {
	if b.Record == nil || !c.isStagedTable(b.Table) {
		return false, nil
	}
	return true, c.accum.Take(ctx, coalesceHost{c}, b)
}

// flushDueAccums sends every accumulator that is due; the pump calls it on
// its tick.
func (c *Coordinator) flushDueAccums(ctx context.Context) error {
	return c.accum.FlushDue(ctx, coalesceHost{c})
}

// flushAccum sends a table's accumulated batches as one cycle.
func (c *Coordinator) flushAccum(ctx context.Context, table string) error {
	return c.accum.Flush(ctx, coalesceHost{c}, table)
}

// sendAccumLocked takes and sends a table's accumulator. Caller holds the
// gate's flush lock.
func (c *Coordinator) sendAccumLocked(ctx context.Context, table string) error {
	return c.accum.SendLocked(ctx, coalesceHost{c}, table)
}

// flushAccumBeforePause sends a table's accumulated batches when the table is
// paused for a re-slice, before its first paused batch is held: they are
// older than everything the pause holds.
func (c *Coordinator) flushAccumBeforePause(ctx context.Context, table string) error {
	if !c.pause.Paused(table) {
		return nil
	}
	return c.flushAccum(ctx, table)
}

// releaseAccums drops every accumulated batch when the pump ends: nothing
// sends them any more, and the stream replays them from the committed
// position.
func (c *Coordinator) releaseAccums() { c.accum.Release(coalesceHost{c}) }
