package coordinator

// CD-AUDIT v1 guardian tests.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
)

// CD-T1: a stall with unacked (in-flight) batches must TERMINATE, not reset.
// A reset cancels the session but the worker reconnects and drains only the
// queue — the in-flight batches are neither queued nor in the one-slot
// resend, so a reset would drop them silently. Terminating restarts the run
// and replays from the committed position.
func TestSupervisorResetWithInFlightTerminates(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	c.index["w1"].add(inflightBatch{id: 1, table: "t"})

	// Worker stopped acking (2 minutes ago).
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	err := s.tick(time.Now(), SupervisorConfig{
		AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute,
	})
	if err == nil || !strings.Contains(err.Error(), "in-flight") {
		t.Fatalf("err = %v, want a terminate citing in-flight batches", err)
	}
}

// The control: a stale worker with NOTHING owed resets normally (no data to
// lose), so the supervisor's recovery path still works.
func TestSupervisorResetWithoutInFlightRecovers(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	w := workers["w1"]
	cancelled := false
	w.cancel = func() { cancelled = true }
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	if err := s.tick(time.Now(), SupervisorConfig{
		AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute,
	}); err != nil {
		t.Fatalf("tick: %v (a reset with nothing owed must not terminate)", err)
	}
	if !cancelled {
		t.Fatal("a safe reset must still cancel the session")
	}
}

// CD-T4: a marker batch for a table with no canonical schema (not in refs)
// must error in the coordinator, not encode a zero-column record that the
// worker rejects with a misleading column error.
func TestEnqueueBatchMarkerUnknownTableErrors(t *testing.T) {
	c := &Coordinator{
		route:     map[string]*workerState{"t": {name: "w1", queue: make(chan queuedBatch, 1)}},
		refs:      nil,
		canonical: map[string]core.Schema{},
	}
	err := c.enqueueBatch(context.Background(), nil, &pb.BatchMeta{Table: "t"})
	if err == nil || !strings.Contains(err.Error(), "canonical schema") {
		t.Fatalf("err = %v, want a marker-schema error", err)
	}
}

// CD-T4 negative: a marker with an empty table is rejected up front.
func TestEnqueueBatchMarkerEmptyTableErrors(t *testing.T) {
	c := &Coordinator{route: map[string]*workerState{}, canonical: map[string]core.Schema{}}
	err := c.enqueueBatch(context.Background(), nil, &pb.BatchMeta{})
	if err == nil || !strings.Contains(err.Error(), "requires a table") {
		t.Fatalf("err = %v, want a marker-table error", err)
	}
}

// V7 / CD-T2: a session ending during an active snapshot must fail the run
// fast — a reset mid-snapshot is NOT a recoverable non-failure here (the
// worker's window died with it), and letting the snapshot loop continue
// would either wait out AckTimeout/MaxResets or let a stale ChunkReady
// satisfy the wait against an empty window.
func TestSignalSessionEndDuringSnapshot(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	c.snapshotActive.Store(true)

	// A reset (errSessionReset) mid-snapshot must still surface as a run
	// error, not be swallowed by the reset-is-not-a-failure rule.
	c.signalSessionEnd("w1", errSessionReset)

	select {
	case err := <-c.sessionErrs:
		if err == nil || !strings.Contains(err.Error(), "during snapshot") {
			t.Fatalf("err = %v, want a snapshot-session error", err)
		}
	default:
		t.Fatal("a reset mid-snapshot must fail the run")
	}
}

// The control: the SAME reset without a snapshot active is not a run error
// (the supervisor recovers).
func TestSignalSessionEndResetWithoutSnapshotIsSilent(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)
	c.snapshotActive.Store(false)

	c.signalSessionEnd("w1", errSessionReset)

	select {
	case err := <-c.sessionErrs:
		t.Fatalf("a reset outside a snapshot must not fail the run, got %v", err)
	default:
	}
}

// A genuine worker death always surfaces, snapshot or not.
func TestSignalSessionEndDeathSurfaces(t *testing.T) {
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 4)

	c.signalSessionEnd("w1", errWorkerDead)

	select {
	case err := <-c.sessionErrs:
		if err != errWorkerDead {
			t.Fatalf("err = %v, want the death error", err)
		}
	default:
		t.Fatal("a worker death must surface")
	}
}

var errWorkerDead = errors.New("stream died")

// opaquePos is an identity-only position (plugin offset cookie): different
// values are Incomparable.
type opaquePos string

func (o opaquePos) String() string { return string(o) }
func (o opaquePos) Compare(other position.Position) int {
	p, ok := other.(opaquePos)
	if ok && o == p {
		return 0
	}
	return position.Incomparable
}
func (o opaquePos) Contains(other position.Position) bool {
	p, ok := other.(opaquePos)
	return ok && o == p
}

// P2: an incomparable ack must NOT truncate the in-flight index — the safe
// direction is "don't truncate" (the head batch stays queued until coverage
// is certain). Truncating on an undefined order could free budget for
// batches that were never committed.
func TestPositionIndexIncomparableAckDoesNotTruncate(t *testing.T) {
	// A different opaque cookie cannot prove coverage: the batch stays
	// queued.
	p := newPositionIndex("run-p2a")
	p.add(inflightBatch{table: "t", high: opaquePos("cookie-1"), bytes: 5})
	if freed := p.truncate("t", opaquePos("cookie-2")); freed != 0 {
		t.Fatalf("incomparable ack freed %d, want 0 (must not truncate)", freed)
	}
	if p.InFlight() != 1 {
		t.Fatalf("in-flight = %d, want 1", p.InFlight())
	}

	// Identity DOES cover it (same cookie).
	p2 := newPositionIndex("run-p2b")
	p2.add(inflightBatch{table: "t", high: opaquePos("cookie-1"), bytes: 5})
	if freed := p2.truncate("t", opaquePos("cookie-1")); freed != 5 {
		t.Fatalf("identical ack freed %d, want 5", freed)
	}
}
