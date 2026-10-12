package staged

import (
	"fmt"
	"log/slog"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/position"
)

// Delivery is one worker's staged share of a cycle (WK-001 C5).
type Delivery struct {
	Table           string
	Seq             uint64
	Epoch           uint64
	Descriptor      []byte
	Position        string
	SnapshotState   string
	SnapshotPending []uint32
	// WindowLost: the worker delivers for a snapshot window it never held.
	WindowLost bool
}

// CommitHost is the coordinator as a staged commit sees it.
type CommitHost interface {
	// Epoch returns worker's current generation, false for a worker the
	// coordinator does not know.
	Epoch(worker string) (uint64, bool)
	// ParsePosition parses a source position.
	ParsePosition(s string) (position.Position, error)
	// DropWindow takes a complete window cycle out of its fan-out pass and
	// reports whether it must be dropped instead of committed: an owner lost
	// it, or it follows one an owner lost (#829).
	DropWindow(target string, seq uint64, lost bool) bool
	// CommitStaged commits the cycle's descriptors at pos through the sink.
	CommitStaged(ref core.TableRef, descriptors [][]byte, pos string) error
	// Committed tells the host the cycle is durable through p: only now may
	// source retention advance for its owners, and the table's batches up to
	// p leave every owner's in-flight window.
	Committed(cy *Cycle, p position.Position)
	// Fail ends the run.
	Fail(err error)
	Log() *slog.Logger
}

// OnDelivery handles one staged delivery from a worker: it records it and
// commits every cycle that is now complete and in turn. A delivery from a
// superseded generation is dropped — accepting it would open a fresh cycle
// for an already-committed seq and commit its data twice.
//
// The whole take-and-commit runs under the table's lock. The tracker
// guarantees send order only while its own lock is held; the commit runs after
// Deliver returns, so a mutex around the commit alone does not preserve that
// order (mutexes do not enforce arrival order) — a later cycle could commit
// before an earlier one, or commit after an earlier cycle's commit failed,
// advancing the durable position past rows that were never written (#543).
// Holding the table lock across deliver and commit makes the pair atomic per
// table; a failed commit poisons the table so no later cycle commits.
func (s *Cycles) OnDelivery(h CommitHost, locks *TableLocks, worker string, d Delivery) {
	epoch, ok := h.Epoch(worker)
	if !ok {
		h.Log().Warn("coordinator: staged batch from unknown worker", "worker", worker)
		return
	}
	if d.Epoch != epoch {
		h.Log().Warn("coordinator: stale staged epoch", "worker", worker, "have", epoch, "got", d.Epoch)
		return
	}

	mu := locks.For(d.Table)
	mu.Lock()
	defer mu.Unlock()
	if s.IsPoisoned(d.Table) {
		return
	}

	ref := core.TableRef{Target: d.Table, Owner: worker}
	if d.WindowLost {
		s.MarkLost(ref, d.Seq)
	}
	committable, known := s.Deliver(ref, d.Seq, d.Descriptor, d.Position, d.SnapshotState, d.SnapshotPending)
	if d.Seq != 0 && !known {
		// The seq named no cycle this tracker holds: a too-late delivery for
		// a discarded cycle — the telltale of a wedged cycle (issue #372). A
		// still-accumulating multi-owner cycle also returns no committable
		// cycles but IS known, so healthy partitioned traffic does not warn.
		h.Log().Warn("coordinator: staged delivery not committable", "worker", worker, "table", d.Table, "seq", d.Seq)
	}
	for _, cy := range committable {
		// Cycles commit in send order; the first failure must stop every
		// later cycle of the table — committing a later cycle would advance
		// the durable position past the gap the failed cycle left, and its
		// data would never be replayed (WK-001 C5.4).
		if err := s.commitLocked(h, cy); err != nil {
			s.Poison(d.Table)
			h.Fail(err)
			return
		}
	}
}

// Commit commits one complete cycle, taking the table's lock. For callers
// that do not already hold it.
func (s *Cycles) Commit(h CommitHost, locks *TableLocks, cy *Cycle) error {
	mu := locks.For(cy.Ref.Target)
	mu.Lock()
	defer mu.Unlock()
	if s.IsPoisoned(cy.Ref.Target) {
		return nil
	}
	return s.commitLocked(h, cy)
}

// commitLocked commits one complete cycle through the host's sink, at the
// position CommitPosition gives it. The caller holds the table's lock, so
// cycles of one table commit strictly in send order.
func (s *Cycles) commitLocked(h CommitHost, cy *Cycle) error {
	// A fan-out window an owner lost, and the windows queued behind it, do
	// not commit: their progress records the lost chunk as done (#829).
	if cy.Window && h.DropWindow(cy.Ref.Target, cy.Seq, cy.Lost) {
		h.Log().Warn("coordinator: snapshot window not committed; its chunk is read again",
			"table", cy.Ref.Target, "seq", cy.Seq, "lost", cy.Lost)
		return nil
	}
	pos, err := cy.CommitPosition(h.ParsePosition)
	if err != nil {
		return fmt.Errorf("table %s: staged cycle %d position: %w", cy.Ref.Target, cy.Seq, err)
	}
	faultinject.At(faultinject.CoordinatorCycleBeforeCommit,
		"table", cy.Ref.Target, "seq", cy.Seq, "position", pos, "deliveries", len(cy.Descriptors))
	if err := h.CommitStaged(cy.Ref, cy.Descriptors, pos); err != nil {
		return fmt.Errorf("table %s: staged commit: %w", cy.Ref.Target, err)
	}
	faultinject.At(faultinject.CoordinatorCycleCommittedBeforeRecord,
		"table", cy.Ref.Target, "seq", cy.Seq, "position", pos, "deliveries", len(cy.Descriptors))
	h.Log().Debug("coordinator: staged cycle committed", "table", cy.Ref.Target, "seq", cy.Seq, "pos", pos,
		"owners", len(cy.Owners), "descriptors", len(cy.Descriptors), "snapshot_state", cy.State, "pending", pendingHead(cy.Pending))
	// The cycle is durable: only now may source retention advance. The
	// worker's per-batch ack does not advance it on a staged table (WK-001
	// §2.2/F2).
	if pos != "" {
		p, perr := h.ParsePosition(pos)
		if perr != nil {
			h.Log().Warn("coordinator: staged cycle position", "table", cy.Ref.Target, "pos", pos, "err", perr)
		} else {
			h.Committed(cy, p)
		}
	}
	return nil
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
