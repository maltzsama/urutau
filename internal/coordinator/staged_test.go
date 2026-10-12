package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/staged"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
)

// fakeStagedSink satisfies sink.StagedCommitter (and sink.Sink via the
// embedded interface) so isStagedTable's capability probe is exercised.
type fakeStagedSink struct{ sink.Sink }

func (fakeStagedSink) CommitStaged(context.Context, core.TableRef, [][]byte, string) error {
	return nil
}

// SupportsConcurrentWriters lets the re-slice capability check pass; these
// tests exercise routing and cycle bookkeeping, not the sink.
func (fakeStagedSink) SupportsConcurrentWriters() bool { return true }

// fakeNonConcurrentSink is a sink.Sink that declares it cannot order
// concurrent writers, for the re-slice capability check.
type fakeNonConcurrentSink struct{ sink.Sink }

func (fakeNonConcurrentSink) SupportsConcurrentWriters() bool { return false }

// WK-001 §2.2/F2: only a partitioned table on a staging sink is "staged" —
// its commits are owned by the coordinator's cycle, so its worker's ack must
// not advance the confirmed position.
func TestIsStagedTable(t *testing.T) {
	c := &Coordinator{snk: fakeStagedSink{}}
	c.publishRouting(newRouting(map[string][]*workerState{
		"orders": {{name: "w0"}, {name: "w1"}},
		"items":  {{name: "w2"}},
	}, map[string][]source.Chunk{}))
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
	cy := &staged.Cycle{
		Ref:         core.TableRef{Target: "t"},
		Seq:         5,
		Owners:      map[string]bool{"w0": true, "w1": true},
		Descriptors: [][]byte{{1}},
		Positions:   []string{"0/10", "0/20"},
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
