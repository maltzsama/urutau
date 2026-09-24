package coordinator

import (
	"fmt"
	"sync"

	"github.com/maltzsama/urutau/core"
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
	return c.stagesCycles() && len(c.loadRouting().owners[target]) > 1
}

// onStagedBatch handles one StagedBatch from a worker (WK-001 C5): it records
// the delivery and commits every cycle that is now complete and in turn. A
// delivery from a superseded generation is dropped — accepting it would open
// a fresh cycle for an already-committed seq and commit its data twice.
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
	ref := core.TableRef{Target: sb.Table, Owner: worker}
	committable := c.staged.deliver(ref, sb.Seq, sb.Descriptor_, sb.Position, sb.SnapshotState, sb.SnapshotPending)
	if sb.Seq != 0 && len(committable) == 0 {
		// A delivery that commits nothing is a dropped/incomplete cycle — the
		// telltale of a wedged cycle (issue #372).
		c.log.Warn("coordinator: staged delivery not committable", "worker", worker, "table", sb.Table, "seq", sb.Seq)
	}
	for _, cy := range committable {
		// Cycles commit in send order; the first failure must stop the run
		// here — committing a later cycle would advance the durable position
		// past the gap the failed cycle left, and its data would never be
		// replayed (WK-001 C5.4).
		if err := c.commitStagedCycle(cy); err != nil {
			c.fail(err)
			return
		}
	}
}

// commitStagedCycle commits one complete cycle through the sink's
// StagedCommitter, serialized per table. The commit position is the minimum
// safe position over the cycle's deliveries: every partition's watermark is
// durable in the same commit, so the lowest one is the new checkpoint floor.
func (c *Coordinator) commitStagedCycle(cy *stagedCycle) error {
	pos, err := c.minSafePositions(cy.positions)
	if err != nil {
		return fmt.Errorf("coordinator: table %s: staged cycle %d position: %w", cy.ref.Target, cy.seq, err)
	}
	committer, ok := c.snk.(sink.StagedCommitter)
	if !ok {
		return fmt.Errorf("coordinator: table %s: sink does not stage commits", cy.ref.Target)
	}
	mu := c.stagedLock(cy.ref.Target)
	mu.Lock()
	defer mu.Unlock()
	if err := committer.CommitStaged(c.runCtx, cy.ref, cy.descriptors, pos); err != nil {
		return fmt.Errorf("coordinator: table %s: staged commit: %w", cy.ref.Target, err)
	}
	// The cycle is durable: only now may source retention advance. Record the
	// cycle's position for every owner it covered, so confirmedPosition (the
	// min) reflects the whole cycle — the worker's per-batch ack does not
	// advance it on a staged table (WK-001 §2.2/F2).
	if pos != "" {
		p, perr := c.src.ParsePosition(pos)
		if perr != nil {
			c.log.Warn("coordinator: staged cycle position", "table", cy.ref.Target, "pos", pos, "err", perr)
		} else {
			for owner := range cy.owners {
				c.recordConfirmed(owner, p)
			}
		}
	}
	return nil
}

// stagedLock returns the mutex serializing staged commits for one table.
func (c *Coordinator) stagedLock(table string) *sync.Mutex {
	c.stagedMu.Lock()
	defer c.stagedMu.Unlock()
	mu := c.stagedLocks[table]
	if mu == nil {
		mu = &sync.Mutex{}
		c.stagedLocks[table] = mu
	}
	return mu
}

// minSafePositions parses each delivery's watermark and returns the lowest one
// as the cycle's commit position. An empty position set yields "".
func (c *Coordinator) minSafePositions(positions []string) (string, error) {
	vals := make([]position.Position, 0, len(positions))
	for _, s := range positions {
		if s == "" {
			continue
		}
		p, err := c.src.ParsePosition(s)
		if err != nil {
			return "", err
		}
		vals = append(vals, p)
	}
	if len(vals) == 0 {
		return "", nil
	}
	best, err := position.MinSafe(vals)
	if err != nil {
		return "", err
	}
	return best.String(), nil
}

// fail reports a terminal coordinator error without blocking if one is
// already pending.
func (c *Coordinator) fail(err error) {
	select {
	case c.terminate <- err:
	default:
	}
}
