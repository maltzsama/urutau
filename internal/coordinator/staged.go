package coordinator

import (
	"sync"

	"github.com/maltzsama/urutau/core"
)

// stagedCycle accumulates the deliveries of one binlog batch (WK-001 C5) for
// a partitioned table: the coordinator sends N sub-batches (one per partition
// with rows) and waits for N staged deliveries, then commits the whole cycle
// as one unit. A cycle whose workers do not all deliver is discarded, never
// committed partially.
type stagedCycle struct {
	ref         core.TableRef
	seq         uint64
	expected    int
	owners      map[string]bool // workers whose delivery this cycle needs
	descriptors [][]byte
	positions   []string
	state       string
	pending     []uint32
}

// stagedCycles tracks open cycles keyed by (table, seq). Cycles of one table
// commit in send order (seq order), even when they complete out of order:
// committing a higher position first and a lower one after would regress the
// durable checkpoint and re-deliver rows. A Seq==0 delivery — a
// worker-generated snapshot/window batch, which never saw a BatchMeta — is
// committed on arrival (the snapshot phase precedes every data cycle).
type stagedCycles struct {
	mu    sync.Mutex
	open  map[cycleKey]*stagedCycle // still accumulating
	done  map[cycleKey]*stagedCycle // complete, waiting for its turn
	order map[string][]uint64       // per table, seqs in send order
}

type cycleKey struct {
	table string
	seq   uint64
}

func newStagedCycles() *stagedCycles {
	return &stagedCycles{
		open:  map[cycleKey]*stagedCycle{},
		done:  map[cycleKey]*stagedCycle{},
		order: map[string][]uint64{},
	}
}

// expect records that a delivery is expected from each of owners for
// (ref.Target, seq). Called once per binlog batch, with exactly the workers a
// sub-batch was sent to (a partition with no rows is not sent, so its worker
// is not expected).
func (s *stagedCycles) expect(ref core.TableRef, seq uint64, owners []string) {
	if s == nil || len(owners) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := cycleKey{ref.Target, seq}
	if _, ok := s.open[k]; ok {
		return
	}
	own := make(map[string]bool, len(owners))
	for _, o := range owners {
		own[o] = true
	}
	s.open[k] = &stagedCycle{ref: ref, seq: seq, expected: len(owners), owners: own}
	s.order[ref.Target] = append(s.order[ref.Target], seq)
}

// deliver records one staged delivery and returns the run of cycles of that
// table now committable, head-first. A Seq==0 delivery is its own cycle of
// one, returned immediately.
func (s *stagedCycles) deliver(ref core.TableRef, seq uint64, desc []byte, pos, state string, pending []uint32) []*stagedCycle {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := cycleKey{ref.Target, seq}
	cy := s.open[k]
	if cy == nil {
		// A worker-generated snapshot/window batch (seq 0) never saw a
		// BatchMeta: it is its own cycle of one. Any other unknown seq is
		// a too-late delivery for a discarded cycle — dropping it is the
		// only safe choice, since committing it would be a partial cycle.
		if seq != 0 {
			return nil
		}
		return []*stagedCycle{{
			ref: ref, seq: 0, expected: 1,
			descriptors: [][]byte{desc}, positions: []string{pos},
			state: state, pending: pending,
		}}
	}
	cy.descriptors = append(cy.descriptors, desc)
	cy.positions = append(cy.positions, pos)
	if state != "" {
		cy.state = state
	}
	if pending != nil {
		cy.pending = pending
	}
	if len(cy.descriptors) < cy.expected {
		return nil
	}
	delete(s.open, k)
	s.done[k] = cy
	return s.drainLocked(ref.Target)
}

// drainLocked pops the head of table's send order while it is complete,
// returning the committable run. Caller holds s.mu.
func (s *stagedCycles) drainLocked(table string) []*stagedCycle {
	var out []*stagedCycle
	q := s.order[table]
	for len(q) > 0 {
		k := cycleKey{table, q[0]}
		cy := s.done[k]
		if cy == nil {
			break
		}
		delete(s.done, k)
		q = q[1:]
		out = append(out, cy)
	}
	s.order[table] = q
	return out
}

// discardWorker drops every cycle that needed a delivery from worker — on
// session loss those cycles can never complete, and an incomplete cycle must
// never be committed — and then every remaining cycle of each affected table.
// A cycle still queued for that table sits BEHIND the discarded one in send
// order: committing it would advance the durable position over the discarded
// cycle's gap, and that gap's data would never be replayed. Returns the
// number discarded, for logging.
//
// A worker that owed nothing (the safe-reset case) leaves no open cycle
// here, so an affected table never arises and nothing is discarded.
func (s *stagedCycles) discardWorker(worker string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tables := map[string]bool{}
	discarded := 0
	for k, cy := range s.open {
		if cy.owners[worker] {
			tables[k.table] = true
			delete(s.open, k)
			discarded++
		}
	}
	for k, cy := range s.done {
		if cy.owners[worker] {
			tables[k.table] = true
			delete(s.done, k)
			discarded++
		}
	}
	for k := range s.open {
		if tables[k.table] {
			delete(s.open, k)
			discarded++
		}
	}
	for k := range s.done {
		if tables[k.table] {
			delete(s.done, k)
			discarded++
		}
	}
	for table := range tables {
		delete(s.order, table)
	}
	return discarded
}

// openFor counts the cycles of table that are not yet committed: still
// accumulating deliveries (open) or complete but waiting their turn in seq
// order (done). A re-slice waits for this to reach zero, so no cycle spans
// two partition layouts.
func (s *stagedCycles) openFor(ref core.TableRef) int {
	open, done := s.openForBreakdown(ref)
	return open + done
}

// openForBreakdown splits openFor's count: cycles still accumulating a
// delivery, and cycles complete but blocked behind an earlier open cycle in
// send order. The split names the stall — an open head cycle wedges every
// complete cycle behind it.
func (s *stagedCycles) openForBreakdown(ref core.TableRef) (open, done int) {
	if s == nil {
		return 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.open {
		if k.table == ref.Target {
			open++
		}
	}
	for k := range s.done {
		if k.table == ref.Target {
			done++
		}
	}
	return open, done
}
