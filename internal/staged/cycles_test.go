package staged

import (
	"math/rand/v2"
	"testing"

	"github.com/maltzsama/urutau/core"
)

func ref(t *testing.T, target string) core.TableRef {
	t.Helper()
	return core.TableRef{Target: target}
}

func TestStagedCycleWaitsForEveryDelivery(t *testing.T) {
	s := New()
	r := ref(t, "orders")
	s.Expect(r, 7, []string{"a", "b", "c"})

	if got, _ := s.Deliver(r, 7, []byte("a"), "100", "", nil); got != nil {
		t.Fatalf("cycle committed after 1/3 deliveries: %v", got)
	}
	if got, _ := s.Deliver(r, 7, []byte("b"), "101", "", nil); got != nil {
		t.Fatalf("cycle committed after 2/3 deliveries: %v", got)
	}
	got, _ := s.Deliver(r, 7, []byte("c"), "102", "", nil)
	if len(got) != 1 {
		t.Fatalf("cycle not committed after 3/3 deliveries: %v", got)
	}
	cy := got[0]
	if len(cy.Descriptors) != 3 {
		t.Fatalf("cycle has %d descriptors, want 3", len(cy.Descriptors))
	}
	if len(cy.Positions) != 3 {
		t.Fatalf("cycle has %d positions, want 3", len(cy.Positions))
	}
}

func TestStagedCyclesCommitInSendOrder(t *testing.T) {
	s := New()
	r := ref(t, "orders")
	s.Expect(r, 1, []string{"a", "b"})
	s.Expect(r, 2, []string{"a", "b"})

	// Cycle 2 completes first — it must wait for cycle 1, or committing it
	// would advance the checkpoint past cycle 1 and regress it later.
	if got, _ := s.Deliver(r, 2, []byte("2a"), "200", "", nil); got != nil {
		t.Fatalf("cycle 2 committed out of order: %v", got)
	}
	if got, _ := s.Deliver(r, 2, []byte("2b"), "201", "", nil); got != nil {
		t.Fatalf("cycle 2 committed before cycle 1: %v", got)
	}

	got, _ := s.Deliver(r, 1, []byte("1a"), "100", "", nil)
	if got != nil {
		t.Fatalf("cycle 1 committed before its second delivery: %v", got)
	}
	got, _ = s.Deliver(r, 1, []byte("1b"), "101", "", nil)
	if len(got) != 2 {
		t.Fatalf("expected cycle 1 then cycle 2 to commit, got %d", len(got))
	}
	if got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("commit order = [%d %d], want [1 2]", got[0].Seq, got[1].Seq)
	}
}

func TestStagedCycleSeqZeroCommitsOnArrival(t *testing.T) {
	s := New()
	r := ref(t, "orders")
	// A worker-generated snapshot batch never saw a BatchMeta: seq 0, its
	// own cycle of one.
	got, _ := s.Deliver(r, 0, []byte("snap"), "10", "state-1", []uint32{3, 4})
	if len(got) != 1 {
		t.Fatalf("seq-0 delivery did not commit on arrival: %v", got)
	}
	if got[0].State != "state-1" || len(got[0].Pending) != 2 {
		t.Fatalf("seq-0 cycle lost snapshot state: %+v", got[0])
	}
	if s.len() != 0 {
		t.Fatalf("seq-0 cycle leaked: %d tracked", s.len())
	}
}

// A partial delivery of a known cycle reports known=true (it is still
// accumulating, not dropped); a delivery for an unknown seq reports false.
// This is what lets onStagedBatch warn only on a real wedge instead of every
// multi-owner partial delivery (issue #372).
func TestDeliverReportsKnownCycle(t *testing.T) {
	s := New()
	r := ref(t, "orders")
	s.Expect(r, 7, []string{"a", "b"})

	if got, known := s.Deliver(r, 7, []byte("a"), "100", "", nil); got != nil || !known {
		t.Fatalf("partial delivery: got=%v known=%v, want nil,true", got, known)
	}
	if got, known := s.Deliver(r, 99, []byte("x"), "900", "", nil); got != nil || known {
		t.Fatalf("unknown seq: got=%v known=%v, want nil,false", got, known)
	}
}

func TestStagedCyclesDiscardTable(t *testing.T) {
	s := New()
	s.Expect(ref(t, "orders"), 1, []string{"a", "b"})
	s.Expect(ref(t, "orders"), 2, []string{"a", "b"})
	s.Expect(ref(t, "items"), 3, []string{"a"})

	if n := s.discardTable("orders"); n != 2 {
		t.Fatalf("discarded %d cycles, want 2", n)
	}
	if s.len() != 1 {
		t.Fatalf("%d cycles tracked after discard, want 1", s.len())
	}
	// A discarded cycle's late delivery must be dropped, never committed as
	// a cycle of one — that would commit a partial cycle.
	if got, _ := s.Deliver(ref(t, "orders"), 1, []byte("late"), "100", "", nil); got != nil {
		t.Fatalf("late delivery for a discarded cycle committed: %v", got)
	}
}

// discardTable drops every cycle of a table. Test-only: it moved here from
// staged.go, whose production build has no caller for the per-table variant.
func (s *Cycles) discardTable(table string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k := range s.open {
		if k.table == table {
			delete(s.open, k)
			n++
		}
	}
	for k := range s.done {
		if k.table == table {
			delete(s.done, k)
			n++
		}
	}
	delete(s.order, table)
	return n
}

// len reports the number of tracked cycles. Test-only: it moved here from
// staged.go, whose production build has no caller for it.
func (s *Cycles) len() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.open) + len(s.done)
}

// A cycle completes once every expected owner delivered, not after as many
// deliveries: an owner delivering the same cycle twice (a redelivery after a
// session reset) must not commit it without the other owner's rows.
func TestStagedCycleCountsOwnersNotDeliveries(t *testing.T) {
	s := New()
	s.Expect(core.TableRef{Target: "t"}, 1, []string{"a", "b"})
	if c, _ := s.Deliver(core.TableRef{Target: "t", Owner: "a"}, 1, []byte{1}, "0/10", "", nil); len(c) != 0 {
		t.Fatal("the cycle completed with one of two owners")
	}
	if c, known := s.Deliver(core.TableRef{Target: "t", Owner: "a"}, 1, []byte{1}, "0/10", "", nil); len(c) != 0 || !known {
		t.Fatalf("a's second delivery: committable=%d known=%v; want the cycle still waiting for b", len(c), known)
	}
	if c, _ := s.Deliver(core.TableRef{Target: "t", Owner: "b"}, 1, []byte{2}, "0/20", "", nil); len(c) != 1 || len(c[0].Descriptors) != 2 {
		t.Fatalf("b's delivery: committable=%v; want the cycle with a's and b's descriptors", c)
	}
}

// Only the owners a cycle was sent to count toward it: a delivery from any
// other worker must not stand in for an expected owner's.
func TestStagedCycleIgnoresUnexpectedOwners(t *testing.T) {
	s := New()
	s.Expect(core.TableRef{Target: "t"}, 1, []string{"a", "b"})
	if c, _ := s.Deliver(core.TableRef{Target: "t", Owner: "a"}, 1, []byte{1}, "0/10", "", nil); len(c) != 0 {
		t.Fatal("the cycle completed with one of two owners")
	}
	if c, known := s.Deliver(core.TableRef{Target: "t", Owner: "x"}, 1, []byte{9}, "0/90", "", nil); len(c) != 0 || !known {
		t.Fatalf("x's delivery: committable=%d known=%v; want the cycle still waiting for b", len(c), known)
	}
	if c, _ := s.Deliver(core.TableRef{Target: "t", Owner: "b"}, 1, []byte{2}, "0/20", "", nil); len(c) != 1 || len(c[0].Descriptors) != 2 {
		t.Fatalf("b's delivery: committable=%v; want the cycle with a's and b's descriptors only", c)
	}
}

// openFor answers from the table's send order instead of walking every
// table's cycles. The two must agree after every step of any mix of cycles
// expected, delivered out of order and committed.
func TestOpenForMatchesTheCycleMaps(t *testing.T) {
	s := New()
	tables := []string{"raw.a", "raw.b", "raw.c"}
	rng := rand.New(rand.NewPCG(1, 2))
	type share struct {
		seq   uint64
		owner string
	}
	owed := map[string][]share{} // table → deliveries not made yet
	var seq uint64
	check := func(step string) {
		t.Helper()
		for _, tb := range tables {
			ref := core.TableRef{Target: tb}
			open, done := s.OpenForBreakdown(ref)
			if got := s.OpenFor(ref); got != open+done {
				t.Fatalf("%s: openFor(%s) = %d, the maps hold %d open + %d done", step, tb, got, open, done)
			}
		}
	}
	for range 3000 {
		tb := tables[rng.IntN(len(tables))]
		if len(owed[tb]) == 0 || rng.IntN(3) == 0 {
			seq++
			s.Expect(core.TableRef{Target: tb}, seq, []string{"w0", "w1"})
			owed[tb] = append(owed[tb], share{seq, "w0"}, share{seq, "w1"})
			check("expect")
			continue
		}
		// Any owed share, in any order: cycles complete out of send order.
		i := rng.IntN(len(owed[tb]))
		sh := owed[tb][i]
		owed[tb] = append(owed[tb][:i], owed[tb][i+1:]...)
		s.Deliver(core.TableRef{Target: tb, Owner: sh.owner}, sh.seq, []byte{1}, "", "", nil)
		check("deliver")
	}
	// Everything owed is delivered: nothing may be left open, in the maps or
	// in the send order.
	for tb, shares := range owed {
		for _, sh := range shares {
			s.Deliver(core.TableRef{Target: tb, Owner: sh.owner}, sh.seq, []byte{1}, "", "", nil)
		}
	}
	check("drain")
	for _, tb := range tables {
		if n := s.OpenFor(core.TableRef{Target: tb}); n != 0 {
			t.Fatalf("%s: %d cycles left after every delivery", tb, n)
		}
	}
}

// #602: totalOpen counts staged cycles still accumulating or waiting to
// commit.
func TestStagedCyclesTotalOpen(t *testing.T) {
	s := New()
	if s.TotalOpen() != 0 {
		t.Fatalf("empty stagedCycles totalOpen = %d, want 0", s.TotalOpen())
	}
	s.Expect(core.TableRef{Source: "shop.a", Target: "raw.a"}, 1, []string{"w0"})
	if s.TotalOpen() != 1 {
		t.Fatalf("totalOpen after one open cycle = %d, want 1", s.TotalOpen())
	}
}
