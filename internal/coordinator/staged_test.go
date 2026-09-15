package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/core"
)

func ref(t *testing.T, target string) core.TableRef {
	t.Helper()
	return core.TableRef{Target: target}
}

func TestStagedCycleWaitsForEveryDelivery(t *testing.T) {
	s := newStagedCycles()
	r := ref(t, "orders")
	s.expect(r, 7, []string{"a", "b", "c"})

	if got := s.deliver(r, 7, []byte("a"), "100", "", nil); got != nil {
		t.Fatalf("cycle committed after 1/3 deliveries: %v", got)
	}
	if got := s.deliver(r, 7, []byte("b"), "101", "", nil); got != nil {
		t.Fatalf("cycle committed after 2/3 deliveries: %v", got)
	}
	got := s.deliver(r, 7, []byte("c"), "102", "", nil)
	if len(got) != 1 {
		t.Fatalf("cycle not committed after 3/3 deliveries: %v", got)
	}
	cy := got[0]
	if len(cy.descriptors) != 3 {
		t.Fatalf("cycle has %d descriptors, want 3", len(cy.descriptors))
	}
	if len(cy.positions) != 3 {
		t.Fatalf("cycle has %d positions, want 3", len(cy.positions))
	}
}

func TestStagedCyclesCommitInSendOrder(t *testing.T) {
	s := newStagedCycles()
	r := ref(t, "orders")
	s.expect(r, 1, []string{"a", "b"})
	s.expect(r, 2, []string{"a", "b"})

	// Cycle 2 completes first — it must wait for cycle 1, or committing it
	// would advance the checkpoint past cycle 1 and regress it later.
	if got := s.deliver(r, 2, []byte("2a"), "200", "", nil); got != nil {
		t.Fatalf("cycle 2 committed out of order: %v", got)
	}
	if got := s.deliver(r, 2, []byte("2b"), "201", "", nil); got != nil {
		t.Fatalf("cycle 2 committed before cycle 1: %v", got)
	}

	got := s.deliver(r, 1, []byte("1a"), "100", "", nil)
	if got != nil {
		t.Fatalf("cycle 1 committed before its second delivery: %v", got)
	}
	got = s.deliver(r, 1, []byte("1b"), "101", "", nil)
	if len(got) != 2 {
		t.Fatalf("expected cycle 1 then cycle 2 to commit, got %d", len(got))
	}
	if got[0].seq != 1 || got[1].seq != 2 {
		t.Fatalf("commit order = [%d %d], want [1 2]", got[0].seq, got[1].seq)
	}
}

func TestStagedCycleSeqZeroCommitsOnArrival(t *testing.T) {
	s := newStagedCycles()
	r := ref(t, "orders")
	// A worker-generated snapshot batch never saw a BatchMeta: seq 0, its
	// own cycle of one.
	got := s.deliver(r, 0, []byte("snap"), "10", "state-1", []uint32{3, 4})
	if len(got) != 1 {
		t.Fatalf("seq-0 delivery did not commit on arrival: %v", got)
	}
	if got[0].state != "state-1" || len(got[0].pending) != 2 {
		t.Fatalf("seq-0 cycle lost snapshot state: %+v", got[0])
	}
	if s.len() != 0 {
		t.Fatalf("seq-0 cycle leaked: %d tracked", s.len())
	}
}

func TestStagedCyclesDiscardTable(t *testing.T) {
	s := newStagedCycles()
	s.expect(ref(t, "orders"), 1, []string{"a", "b"})
	s.expect(ref(t, "orders"), 2, []string{"a", "b"})
	s.expect(ref(t, "items"), 3, []string{"a"})

	if n := s.discardTable("orders"); n != 2 {
		t.Fatalf("discarded %d cycles, want 2", n)
	}
	if s.len() != 1 {
		t.Fatalf("%d cycles tracked after discard, want 1", s.len())
	}
	// A discarded cycle's late delivery must be dropped, never committed as
	// a cycle of one — that would commit a partial cycle.
	if got := s.deliver(ref(t, "orders"), 1, []byte("late"), "100", "", nil); got != nil {
		t.Fatalf("late delivery for a discarded cycle committed: %v", got)
	}
}

func TestStagedCyclesDiscardWorkerUnblocksQueue(t *testing.T) {
	s := newStagedCycles()
	r := ref(t, "orders")
	// Cycle 1 needs worker "a"; cycle 2 needs "b". Cycle 2 arrives first and
	// waits; worker "a" dies, so cycle 1 is discarded and cycle 2 — which did
	// not involve "a" — must be unblocked and returned for commit.
	s.expect(r, 1, []string{"a"})
	s.expect(r, 2, []string{"b"})

	if got := s.deliver(r, 2, []byte("2"), "200", "", nil); got != nil {
		t.Fatalf("cycle 2 committed before cycle 1: %v", got)
	}
	ready, n := s.discardWorker("a")
	if n != 1 {
		t.Fatalf("discarded %d cycles, want 1", n)
	}
	if len(ready) != 1 || ready[0].seq != 2 {
		t.Fatalf("worker loss did not unblock cycle 2: %v", ready)
	}
}
