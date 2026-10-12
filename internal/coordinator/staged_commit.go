package coordinator

import (
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/faultinject"
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

// onStagedBatch handles one StagedBatch from a worker (WK-001 C5): it records
// the delivery and commits every cycle that is now complete and in turn. A
// delivery from a superseded generation is dropped — accepting it would open
// a fresh cycle for an already-committed seq and commit its data twice.
//
// The whole take-and-commit runs under the table's staged lock. The tracker
// guarantees send order only while its own lock is held; the commit runs after
// deliver returns, so a mutex around the commit alone does not preserve that
// order (mutexes do not enforce arrival order) — a later cycle could commit
// before an earlier one, or commit after an earlier cycle's commit failed,
// advancing the durable position past rows that were never written (#543).
// Holding the table lock across deliver and commit makes the pair atomic per
// table; a failed commit poisons the table so no later cycle commits.
func (c *Coordinator) onStagedBatch(worker string, sb *pb.StagedBatch) {
	if sb == nil {
		return
	}
	// Read the worker and its epoch under c.mu: resetWorker writes w.epoch
	// under c.mu from the supervisor/dashboard goroutines, so an unlocked read
	// here is a data race (issue #207).
	c.mu.Lock()
	w := c.workers[worker]
	if w == nil {
		c.mu.Unlock()
		c.log.Warn("coordinator: staged batch from unknown worker", "worker", worker)
		return
	}
	epoch := w.epoch
	c.mu.Unlock()
	if sb.Epoch != epoch {
		c.log.Warn("coordinator: stale staged epoch", "worker", worker, "have", epoch, "got", sb.Epoch)
		return
	}

	mu := c.stagedLocks.For(sb.Table)
	mu.Lock()
	defer mu.Unlock()
	if c.staged.IsPoisoned(sb.Table) {
		return
	}

	ref := core.TableRef{Target: sb.Table, Owner: worker}
	if sb.WindowLost {
		c.staged.MarkLost(ref, sb.Seq)
	}
	committable, known := c.staged.Deliver(ref, sb.Seq, sb.Descriptor_, sb.Position, sb.SnapshotState, sb.SnapshotPending)
	if sb.Seq != 0 && !known {
		// The seq named no cycle this tracker holds: a too-late delivery for
		// a discarded cycle — the telltale of a wedged cycle (issue #372). A
		// still-accumulating multi-owner cycle also returns no committable
		// cycles but IS known, so healthy partitioned traffic does not warn.
		c.log.Warn("coordinator: staged delivery not committable", "worker", worker, "table", sb.Table, "seq", sb.Seq)
	}
	for _, cy := range committable {
		// Cycles commit in send order; the first failure must stop every
		// later cycle of the table — committing a later cycle would advance
		// the durable position past the gap the failed cycle left, and its
		// data would never be replayed (WK-001 C5.4).
		if err := c.commitStagedCycleLocked(cy); err != nil {
			c.staged.Poison(sb.Table)
			c.fail(err)
			return
		}
	}
}

// commitStagedCycle commits one complete cycle, taking the table's staged
// lock. Kept for callers that do not already hold it (tests, one-off paths).
func (c *Coordinator) commitStagedCycle(cy *staged.Cycle) error {
	mu := c.stagedLocks.For(cy.Ref.Target)
	mu.Lock()
	defer mu.Unlock()
	if c.staged.IsPoisoned(cy.Ref.Target) {
		return nil
	}
	return c.commitStagedCycleLocked(cy)
}

// commitStagedCycleLocked commits one complete cycle through the sink's
// StagedCommitter, at the position staged.Cycle.CommitPosition gives it. The
// caller holds the table's staged lock, so cycles of one table commit
// strictly in send order.
func (c *Coordinator) commitStagedCycleLocked(cy *staged.Cycle) error {
	// A fan-out window an owner lost, and the windows queued behind it, do
	// not commit: their progress records the lost chunk as done (#829).
	if cy.Window && c.fanout.Resolve(cy.Ref.Target, cy.Seq, cy.Lost) {
		c.log.Warn("coordinator: snapshot window not committed; its chunk is read again",
			"table", cy.Ref.Target, "seq", cy.Seq, "lost", cy.Lost)
		return nil
	}
	pos, err := cy.CommitPosition(c.src.ParsePosition)
	if err != nil {
		return fmt.Errorf("coordinator: table %s: staged cycle %d position: %w", cy.Ref.Target, cy.Seq, err)
	}
	committer, ok := c.snk.(sink.StagedCommitter)
	if !ok {
		return fmt.Errorf("coordinator: table %s: sink does not stage commits", cy.Ref.Target)
	}
	faultinject.At(faultinject.CoordinatorCycleBeforeCommit,
		"table", cy.Ref.Target, "seq", cy.Seq, "position", pos, "deliveries", len(cy.Descriptors))
	if err := committer.CommitStaged(c.runCtx, cy.Ref, cy.Descriptors, pos); err != nil {
		return fmt.Errorf("coordinator: table %s: staged commit: %w", cy.Ref.Target, err)
	}
	faultinject.At(faultinject.CoordinatorCycleCommittedBeforeRecord,
		"table", cy.Ref.Target, "seq", cy.Seq, "position", pos, "deliveries", len(cy.Descriptors))
	c.log.Debug("coordinator: staged cycle committed", "table", cy.Ref.Target, "seq", cy.Seq, "pos", pos,
		"owners", len(cy.Owners), "descriptors", len(cy.Descriptors), "snapshot_state", cy.State, "pending", pendingHead(cy.Pending))
	// The cycle is durable: only now may source retention advance. Record the
	// cycle's position for every owner it covered, so confirmedPosition (the
	// min) reflects the whole cycle — the worker's per-batch ack does not
	// advance it on a staged table (WK-001 §2.2/F2).
	if pos != "" {
		p, perr := c.src.ParsePosition(pos)
		if perr != nil {
			c.log.Warn("coordinator: staged cycle position", "table", cy.Ref.Target, "pos", pos, "err", perr)
		} else {
			for owner := range cy.Owners {
				c.recordConfirmed(owner, p)
			}
			c.releaseCommittedCycles(cy.Ref.Target, p)
		}
	}
	return nil
}

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

// pendingHead renders a snapshot's pending chunks for a log line: how many,
// and the first, which is where a restarted coordinator would resume.
func pendingHead(pending []uint32) string {
	if pending == nil {
		return ""
	}
	if len(pending) == 0 {
		return "0"
	}
	return fmt.Sprintf("%d from %d", len(pending), pending[0])
}
