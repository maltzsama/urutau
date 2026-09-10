package coordinator

// CD-AUDIT v1 guardian tests.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
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
