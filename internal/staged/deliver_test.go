package staged

import (
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/position"
)

// fakeCommitHost records the commits a delivery led to.
type fakeCommitHost struct {
	epoch     map[string]uint64
	commits   []string // "seq@pos"
	committed []string // positions told durable
	dropped   map[uint64]bool
	failSeq   string
	failed    error
}

func (h *fakeCommitHost) Epoch(worker string) (uint64, bool) {
	e, ok := h.epoch[worker]
	return e, ok
}
func (h *fakeCommitHost) ParsePosition(s string) (position.Position, error) {
	return position.MustLSN(s), nil
}
func (h *fakeCommitHost) DropWindow(_ string, seq uint64, lost bool) bool {
	return lost || h.dropped[seq]
}
func (h *fakeCommitHost) CommitStaged(_ core.TableRef, descriptors [][]byte, pos string) error {
	tag := string(descriptors[0]) + "@" + pos
	if string(descriptors[0]) == h.failSeq {
		return errors.New("catalog down")
	}
	h.commits = append(h.commits, tag)
	return nil
}
func (h *fakeCommitHost) Committed(_ *Cycle, p position.Position) {
	h.committed = append(h.committed, p.String())
}
func (h *fakeCommitHost) Fail(err error)    { h.failed = err }
func (h *fakeCommitHost) Log() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var ordersRef = core.TableRef{Target: "raw.orders"}

// Cycles commit in send order even when the later one completes first, each
// at its batch's position, and the host is told each is durable.
func TestOnDeliveryCommitsInSendOrder(t *testing.T) {
	h := &fakeCommitHost{epoch: map[string]uint64{"w0": 1}}
	s, locks := New(), &TableLocks{}
	s.ExpectAt(ordersRef, 1, []string{"w0"}, "0/10")
	s.ExpectAt(ordersRef, 2, []string{"w0"}, "0/20")

	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 2, Epoch: 1, Descriptor: []byte("c2"), Position: "0/20"})
	if len(h.commits) != 0 {
		t.Fatalf("cycle 2 committed before cycle 1: %v", h.commits)
	}
	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 1, Epoch: 1, Descriptor: []byte("c1"), Position: "0/10"})
	if !reflect.DeepEqual(h.commits, []string{"c1@0/10", "c2@0/20"}) {
		t.Fatalf("commits = %v, want cycle 1 then cycle 2", h.commits)
	}
	if !reflect.DeepEqual(h.committed, []string{"0/10", "0/20"}) {
		t.Fatalf("told durable = %v", h.committed)
	}
}

// A delivery from a superseded generation, or from a worker the coordinator
// does not know, is dropped: the cycle still owes that owner's share.
func TestOnDeliveryDropsAStaleGeneration(t *testing.T) {
	h := &fakeCommitHost{epoch: map[string]uint64{"w0": 2}}
	s, locks := New(), &TableLocks{}
	s.ExpectAt(ordersRef, 1, []string{"w0"}, "0/10")

	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 1, Epoch: 1, Descriptor: []byte("old")})
	s.OnDelivery(h, locks, "ghost", Delivery{Table: "raw.orders", Seq: 1, Descriptor: []byte("ghost")})
	if len(h.commits) != 0 || s.OpenFor(ordersRef) != 1 {
		t.Fatalf("commits = %v, open = %d; want none committed and the cycle still open", h.commits, s.OpenFor(ordersRef))
	}
	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 1, Epoch: 2, Descriptor: []byte("new")})
	if !reflect.DeepEqual(h.commits, []string{"new@0/10"}) {
		t.Fatalf("commits = %v, want the current generation's", h.commits)
	}
}

// A failed commit ends the run and poisons the table: no later cycle of it
// commits past the gap.
func TestOnDeliveryPoisonsTheTableOnAFailedCommit(t *testing.T) {
	h := &fakeCommitHost{epoch: map[string]uint64{"w0": 0}, failSeq: "c1"}
	s, locks := New(), &TableLocks{}
	s.ExpectAt(ordersRef, 1, []string{"w0"}, "0/10")
	s.ExpectAt(ordersRef, 2, []string{"w0"}, "0/20")

	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 1, Descriptor: []byte("c1")})
	if h.failed == nil || !s.IsPoisoned("raw.orders") {
		t.Fatalf("failed = %v, poisoned = %v; want the run ended and the table poisoned", h.failed, s.IsPoisoned("raw.orders"))
	}
	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 2, Descriptor: []byte("c2")})
	if len(h.commits) != 0 || len(h.committed) != 0 {
		t.Fatalf("commits = %v after the failure: a later cycle committed past the gap", h.commits)
	}
}

// A window cycle an owner lost is dropped, not committed, and commits no
// position when it is kept.
func TestOnDeliveryWindowCycles(t *testing.T) {
	h := &fakeCommitHost{epoch: map[string]uint64{"w0": 0}}
	s, locks := New(), &TableLocks{}
	s.ExpectWindow(ordersRef, 1, []string{"w0"})
	s.ExpectWindow(ordersRef, 2, []string{"w0"})

	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 1, WindowLost: true})
	if len(h.commits) != 0 {
		t.Fatalf("a lost window committed: %v", h.commits)
	}
	s.OnDelivery(h, locks, "w0", Delivery{Table: "raw.orders", Seq: 2, Descriptor: []byte("w2"), Position: "0/99"})
	if !reflect.DeepEqual(h.commits, []string{"w2@"}) || len(h.committed) != 0 {
		t.Fatalf("commits = %v, durable = %v; want the window committed with no position", h.commits, h.committed)
	}
}
