package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
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

func TestStagedCyclesDiscardWorkerDropsAffectedTable(t *testing.T) {
	s := newStagedCycles()
	r := ref(t, "orders")
	// Cycle 1 needs worker "a"; cycle 2 needs "b". Cycle 2 arrives first and
	// waits. Worker "a" dies owing cycle 1: cycle 1 is discarded, and cycle 2
	// — though complete and owned only by "b" — sits BEHIND cycle 1 in send
	// order, so committing it would advance the position over cycle 1's gap
	// and cycle 1's data would never be replayed. Both are discarded.
	s.expect(r, 1, []string{"a"})
	s.expect(r, 2, []string{"b"})

	if got := s.deliver(r, 2, []byte("2"), "200", "", nil); got != nil {
		t.Fatalf("cycle 2 committed before cycle 1: %v", got)
	}
	if n := s.discardWorker("a"); n != 2 {
		t.Fatalf("discarded %d cycles, want 2 (the owed cycle and the one behind it)", n)
	}
	if s.len() != 0 {
		t.Fatalf("%d cycles tracked after the discard, want 0", s.len())
	}
}

// fakeStagedSink satisfies sink.StagedCommitter (and sink.Sink via the
// embedded interface) so isStagedTable's capability probe is exercised.
type fakeStagedSink struct{ sink.Sink }

func (fakeStagedSink) CommitStaged(context.Context, core.TableRef, [][]byte, string) error {
	return nil
}

// WK-001 §2.2/F2: only a partitioned table on a staging sink is "staged" —
// its commits are owned by the coordinator's cycle, so its worker's ack must
// not advance the confirmed position.
func TestIsStagedTable(t *testing.T) {
	c := &Coordinator{
		snk: fakeStagedSink{},
		route: map[string][]*workerState{
			"orders": {{name: "w0"}, {name: "w1"}},
			"items":  {{name: "w2"}},
		},
	}
	if !c.isStagedTable("orders") {
		t.Fatal("partitioned table on a staging sink must be staged")
	}
	if c.isStagedTable("items") {
		t.Fatal("single-owner table must not be staged")
	}
	c.snk = nil
	if c.isStagedTable("orders") {
		t.Fatal("a non-staging sink must not stage")
	}
}

// fakeSource satisfies source.Source via the embedded interface, overriding
// only ParsePosition (the sole method commitStagedCycle reaches).
type fakeSource struct{ source.Source }

func (fakeSource) ParsePosition(s string) (position.Position, error) {
	return position.MustLSN(s), nil
}

// recordingStagedSink records that CommitStaged ran.
type recordingStagedSink struct {
	sink.Sink
	committed *bool
}

func (s *recordingStagedSink) CommitStaged(context.Context, core.TableRef, [][]byte, string) error {
	*s.committed = true
	return nil
}

// WK-001 §2.2/F2: on a staged table the coordinator's commitStagedCycle — not
// the worker's ack — advances the confirmed position, and it does so for
// EVERY owner of the cycle with the cycle's MinSafe.
func TestCommitStagedCycleAdvancesConfirmedForOwners(t *testing.T) {
	committed := false
	c := &Coordinator{
		src:         fakeSource{},
		snk:         &recordingStagedSink{committed: &committed},
		confirmed:   map[string]position.Position{},
		stagedLocks: map[string]*sync.Mutex{},
		log:         slog.Default(),
	}
	cy := &stagedCycle{
		ref:         core.TableRef{Target: "t"},
		seq:         5,
		owners:      map[string]bool{"w0": true, "w1": true},
		descriptors: [][]byte{{1}},
		positions:   []string{"0/10", "0/20"},
	}
	if err := c.commitStagedCycle(cy); err != nil {
		t.Fatalf("commitStagedCycle: %v", err)
	}
	if !committed {
		t.Fatal("CommitStaged was not called")
	}
	if len(c.confirmed) != 2 {
		t.Fatalf("confirmed owners = %d, want 2", len(c.confirmed))
	}
	if got := c.confirmedPosition(); got == nil || got.String() != "0/10" {
		t.Fatalf("confirmed = %v, want 0/10 (the cycle MinSafe)", got)
	}
}
