package coordinator

import (
	"log/slog"
	"testing"
	"time"
)

// supervisorHarness builds a Coordinator with the minimal surface the
// supervisor touches: a workers map and a logger. Metrics and eventlog are
// left nil (both are nil-guarded).
func supervisorHarness() (*supervisor, map[string]*workerState) {
	workers := map[string]*workerState{
		"w1": {name: "w1", attached: true},
	}
	c := &Coordinator{workers: workers, log: slog.New(slog.DiscardHandler)}
	s := newSupervisor(c)
	c.supervisor = s // resetWorker reaches it via c.supervisor
	s.noteAck("w1", time.Now())
	return s, workers
}

// A worker that stops acking past the timeout is reset: epoch bumps and the
// session cancel fires.
func TestSupervisorTickResetsStaleWorker(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]

	cfg := SupervisorConfig{AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute}
	// Last ack was 2 minutes ago.
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	cancelled := false
	w.cancel = func() { cancelled = true }

	if err := s.tick(time.Now(), cfg); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if w.epoch != 1 {
		t.Fatalf("epoch = %d, want 1 after reset", w.epoch)
	}
	if !cancelled {
		t.Fatal("session cancel was not invoked on reset")
	}
	if !s.isPending("w1") {
		t.Fatal("reset worker should be marked pending until it reattaches")
	}
}

// A freshly-attached worker that has not acked yet is stale too.
func TestSupervisorTickFreshAttachedNoAckIsStale(t *testing.T) {
	s, workers := supervisorHarness()
	workers["w2"] = &workerState{name: "w2", attached: true} // never acked
	w := workers["w2"]
	cancelled := false
	w.cancel = func() { cancelled = true }

	if err := s.tick(time.Now(), SupervisorConfig{MaxResets: 5}); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if !cancelled {
		t.Fatal("attached-but-silent worker should be reset")
	}
}

// Resets beyond MaxResets within the window terminate the job.
func TestSupervisorTickTerminatesOnCrashloop(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]
	w.cancel = func() {}

	cfg := SupervisorConfig{AckTimeout: 30 * time.Second, MaxResets: 2, ResetWindow: 15 * time.Minute}
	now := time.Now()
	s.noteAck("w1", now.Add(-time.Minute)) // stale

	// First stale tick: one reset in the window (below MaxResets).
	if err := s.tick(now, cfg); err != nil {
		t.Fatalf("first tick: %v", err)
	}
	// Second stale tick (worker still pending): crosses MaxResets → terminal.
	err := s.tick(now.Add(time.Minute), cfg)
	if err == nil || err.Error() != "coordinator: crashloop: worker w1: 2 resets in 15m0s" {
		t.Fatalf("second tick err = %v, want crashloop termination", err)
	}
}

// An actively-acking worker is never reset.
func TestSupervisorTickHealthyWorkerUntouched(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]
	s.noteAck("w1", time.Now())

	if err := s.tick(time.Now(), SupervisorConfig{AckTimeout: 30 * time.Second}); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if w.epoch != 0 || s.isPending("w1") {
		t.Fatalf("healthy worker reset: epoch=%d pending=%v", w.epoch, s.isPending("w1"))
	}
}

// noteAttach clears the pending flag; a reattached worker is not reset.
func TestSupervisorReattachClearsPending(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]
	w.cancel = func() {}

	// Reset it.
	s.noteAck("w1", time.Now().Add(-time.Minute))
	if err := s.tick(time.Now(), SupervisorConfig{AckTimeout: 30 * time.Second}); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if !s.isPending("w1") {
		t.Fatal("expected pending after reset")
	}
	// Reattach: clears pending, updates last ack.
	s.noteAttach("w1")
	if s.isPending("w1") {
		t.Fatal("pending not cleared on reattach")
	}
}

// recordReset slides the window: resets older than the window expire.
func TestSupervisorRecordResetWindowSlides(t *testing.T) {
	s, _ := supervisorHarness()
	window := 10 * time.Minute
	now := time.Now()

	s.recordReset("w1", now.Add(-20*time.Minute), window) // expired
	s.recordReset("w1", now.Add(-5*time.Minute), window)  // within
	s.recordReset("w1", now, window)

	if got := len(s.resets["w1"]); got != 2 {
		t.Fatalf("resets in window = %d, want 2 (oldest expired)", got)
	}
}
