// Package staged tracks the staged commit cycles of partitioned tables: which
// owners each cycle still waits for, and the order the complete ones commit
// in. It holds no sink and no worker: the coordinator opens cycles as it
// sends, feeds them the workers' deliveries, and commits what comes back.
package staged

import (
	"sync"

	"github.com/maltzsama/urutau/core"
)

// Cycle accumulates the deliveries of one binlog batch (WK-001 C5) for
// a partitioned table: the coordinator sends N sub-batches (one per partition
// with rows) and waits for N staged deliveries, then commits the whole cycle
// as one unit. A cycle whose workers do not all deliver is discarded, never
// committed partially.
type Cycle struct {
	Ref         core.TableRef
	Seq         uint64
	expected    int
	Owners      map[string]bool // workers whose delivery this cycle needs
	delivered   map[string]bool // owners whose delivery arrived
	Descriptors [][]byte
	Positions   []string
	// batchPos is the source batch's last position, when the cycle holds
	// every partition's share of one batch: the table is then durable
	// through it once the cycle commits (#459). Empty for a cycle opened
	// without it; the commit then falls back to the deliveries' minimum.
	BatchPos string
	// window marks a snapshot window's cycle: it commits the window's rows
	// and snapshot progress but no position (#468).
	Window bool
	// lost marks a window cycle one of its owners delivered for a window it
	// never held (#829).
	Lost    bool
	State   string
	Pending []uint32
}

// Cycles tracks open cycles keyed by (table, seq). Cycles of one table
// commit in send order (seq order), even when they complete out of order:
// committing a higher position first and a lower one after would regress the
// durable checkpoint and re-deliver rows. A Seq==0 delivery — a
// worker-generated batch that never saw a BatchMeta — is committed on
// arrival. A DBLog window's rows are not one of those: live cycles flow
// during a snapshot, so a window committed on arrival could move the table's
// position past live cycles still open. They are delivered as their Closes
// marker's cycle instead (sendClosesPending, #416).
type Cycles struct {
	mu    sync.Mutex
	open  map[cycleKey]*Cycle // still accumulating
	done  map[cycleKey]*Cycle // complete, waiting for its turn
	order map[string][]uint64 // per table, seqs in send order
	// poisoned marks a table whose cycle commit failed. Once set, no later
	// cycle of the table may commit: a later commit would advance the durable
	// position past the failed cycle's rows, which were never written (#543).
	poisoned map[string]bool
}

type cycleKey struct {
	table string
	seq   uint64
}

// New returns an empty set of cycles.
func New() *Cycles {
	return &Cycles{
		open:     map[cycleKey]*Cycle{},
		done:     map[cycleKey]*Cycle{},
		order:    map[string][]uint64{},
		poisoned: map[string]bool{},
	}
}

// Poison marks a table so no later cycle commits over a failed one (#543).
func (s *Cycles) Poison(table string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.poisoned[table] = true
	s.mu.Unlock()
}

// IsPoisoned reports whether the table's commit already failed (#543).
func (s *Cycles) IsPoisoned(table string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.poisoned[table]
}

// Expect records that a delivery is expected from each of owners for
// (ref.Target, seq). Called once per binlog batch, with exactly the workers a
// sub-batch was sent to (a partition with no rows is not sent, so its worker
// is not expected).
func (s *Cycles) Expect(ref core.TableRef, seq uint64, owners []string) {
	s.ExpectAt(ref, seq, owners, "")
}

// ExpectWindow is expect for a snapshot window's cycle (sendClosesPending).
func (s *Cycles) ExpectWindow(ref core.TableRef, seq uint64, owners []string) {
	s.Expect(ref, seq, owners)
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cy := s.open[cycleKey{ref.Target, seq}]; cy != nil {
		cy.Window = true
	}
}

// MarkLost records that owner's delivery of cycle seq, about to be made, is
// for a snapshot window it never held. A delivery the cycle does not expect
// or already has changes nothing: that owner's share is in the cycle.
func (s *Cycles) MarkLost(ref core.TableRef, seq uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cy := s.open[cycleKey{ref.Target, seq}]
	if cy == nil || !cy.Owners[ref.Owner] || cy.delivered[ref.Owner] {
		return
	}
	cy.Lost = true
}

// ExpectAt is expect for a cycle that carries every partition's share of one
// source batch, whose last position is batchPos.
func (s *Cycles) ExpectAt(ref core.TableRef, seq uint64, owners []string, batchPos string) {
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
	s.open[k] = &Cycle{Ref: ref, Seq: seq, expected: len(owners), Owners: own, BatchPos: batchPos}
	s.order[ref.Target] = append(s.order[ref.Target], seq)
}

// Deliver records one staged delivery and returns the run of cycles of that
// table now committable, head-first. The second return reports whether the
// delivery named a cycle this tracker knows: false means a too-late delivery
// for a discarded cycle (dropped), true covers both a still-accumulating cycle
// and a completed one. A Seq==0 delivery is its own cycle of one, returned
// immediately.
func (s *Cycles) Deliver(ref core.TableRef, seq uint64, desc []byte, pos, state string, pending []uint32) (committable []*Cycle, known bool) {
	if s == nil {
		return nil, false
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
			return nil, false
		}
		return []*Cycle{{
			Ref: ref, Seq: 0, expected: 1,
			Descriptors: [][]byte{desc}, Positions: []string{pos},
			State: state, Pending: pending,
		}}, true
	}
	// One delivery per expected owner: an owner's second delivery of the
	// cycle (a redelivery after a session reset), or one from a worker the
	// cycle was not sent to, must not stand in for another owner's and
	// commit the cycle without its rows.
	if ref.Owner != "" {
		if !cy.Owners[ref.Owner] || cy.delivered[ref.Owner] {
			return nil, true
		}
		if cy.delivered == nil {
			cy.delivered = make(map[string]bool, cy.expected)
		}
		cy.delivered[ref.Owner] = true
	}
	cy.Descriptors = append(cy.Descriptors, desc)
	cy.Positions = append(cy.Positions, pos)
	if state != "" {
		cy.State = state
	}
	if pending != nil {
		cy.Pending = pending
	}
	if len(cy.Descriptors) < cy.expected {
		return nil, true
	}
	delete(s.open, k)
	s.done[k] = cy
	return s.drainLocked(ref.Target), true
}

// drainLocked pops the head of table's send order while it is complete,
// returning the committable run. Caller holds s.mu.
func (s *Cycles) drainLocked(table string) []*Cycle {
	var out []*Cycle
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
	if len(q) == 0 {
		// Nothing left in send order: drop the entry, so the slice's backing
		// array (and a table that has gone quiet) does not stay forever.
		delete(s.order, table)
	} else {
		s.order[table] = q
	}
	return out
}

// OpenFor counts the cycles of table that are not yet committed: still
// accumulating deliveries (open) or complete but waiting their turn in seq
// order (done). A re-slice waits for this to reach zero, so no cycle spans
// two partition layouts.
func (s *Cycles) OpenFor(ref core.TableRef) int {
	if s == nil {
		return 0
	}
	// The table's send order holds exactly its cycles not committed yet: a
	// cycle enters it when expected and leaves it when drained. Its length is
	// the count, without walking every table's cycles — this is asked on the
	// coalescing path, once per batch.
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.order[ref.Target])
}

// OpenForBreakdown splits openFor's count: cycles still accumulating a
// delivery, and cycles complete but blocked behind an earlier open cycle in
// send order. The split names the stall — an open head cycle wedges every
// complete cycle behind it.
func (s *Cycles) OpenForBreakdown(ref core.TableRef) (open, done int) {
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

// OpenSeqs returns the seqs of a table's not-yet-committed cycles, for
// diagnostics: the head of send order names which cycle is blocking.
func (s *Cycles) OpenSeqs(ref core.TableRef) []uint64 {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.order[ref.Target]
	out := make([]uint64, 0, len(q))
	for _, seq := range q {
		k := cycleKey{ref.Target, seq}
		if _, open := s.open[k]; open {
			out = append(out, seq)
		} else if _, done := s.done[k]; done {
			out = append(out, seq)
		}
	}
	return out
}

// TotalOpen reports how many staged cycles are still accumulating or waiting
// to commit. Exposed for the urutau_coordinator_staged_cycles gauge: a growing
// count is a staged backlog (issue #602).
func (s *Cycles) TotalOpen() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.open) + len(s.done)
}
