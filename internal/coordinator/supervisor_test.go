package coordinator

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

// supervisorHarness builds a Coordinator with the minimal surface the
// supervisor touches: a workers map and a logger. Metrics and eventlog are
// left nil (both are nil-guarded).
func supervisorHarness() (*supervisor, map[string]*workerState) {
	// The worker holds one queued batch: a stale-ack reset only applies to a
	// worker that OWES work. An attached worker with nothing pending is idle,
	// not stuck, and tick must leave it alone (issue #312).
	workers := map[string]*workerState{
		"w1": {name: "w1", attached: true, queue: make(chan queuedBatch, 4)},
	}
	workers["w1"].queue <- queuedBatch{id: 1}
	c := &Coordinator{workers: workers, log: slog.New(slog.DiscardHandler)}
	s := newSupervisor(c)
	c.supervisor = s // resetWorker reaches it via c.supervisor
	s.noteAck("w1", time.Now())
	return s, workers
}

// An attached worker that owes nothing is idle, not stale: a quiet table (or
// one that just went through a re-slice) must not be reset, because the reset
// discards its open staged cycles and loses the rows they carry.
func TestSupervisorLeavesIdleWorkerAlone(t *testing.T) {
	workers := map[string]*workerState{
		"w1": {name: "w1", attached: true, queue: make(chan queuedBatch, 4)},
	}
	c := &Coordinator{workers: workers, log: slog.New(slog.DiscardHandler)}
	s := newSupervisor(c)
	c.supervisor = s
	workers["w1"].cancel = func() {}
	// Last ack long past the timeout, but nothing queued and nothing in flight.
	s.noteAck("w1", time.Now().Add(-time.Minute))

	if err := s.tick(time.Now(), SupervisorConfig{AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute}); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if workers["w1"].epoch != 0 || s.isPending("w1") {
		t.Fatalf("idle worker was reset: epoch=%d pending=%v", workers["w1"].epoch, s.isPending("w1"))
	}
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
	// Never acked AND owes a batch: that is stale. (Owing nothing would make
	// it merely idle — see TestSupervisorLeavesIdleWorkerAlone.)
	workers["w2"] = &workerState{name: "w2", attached: true, queue: make(chan queuedBatch, 4)}
	workers["w2"].queue <- queuedBatch{id: 2}
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

// A reset worker that has not reattached is awaited, not reset again: its
// reconnect is on the way (issue #461). Resetting it on every tick counted
// toward MaxResets and ended the run while the Pod was still restarting.
func TestSupervisorAwaitsAPendingWorker(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]
	w.cancel = func() {}

	cfg := SupervisorConfig{AckTimeout: 30 * time.Second, AbsenceTimeout: 5 * time.Minute}
	now := time.Now()
	s.noteAck("w1", now.Add(-time.Minute)) // stale

	if err := s.tick(now, cfg); err != nil {
		t.Fatalf("first tick: %v", err)
	}
	if w.epoch != 1 {
		t.Fatalf("epoch = %d after the first stale tick, want 1", w.epoch)
	}
	w.attached = false // its session ended; the Pod is restarting
	for i := 1; i <= 4; i++ {
		if err := s.tick(now.Add(time.Duration(i)*time.Minute), cfg); err != nil {
			t.Fatalf("tick %d while the worker restarts: %v", i, err)
		}
	}
	if w.epoch != 1 {
		t.Fatalf("epoch = %d, want 1: a pending worker must not be reset again", w.epoch)
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

// A lost worker that owes work is awaited for the absence timeout: its Pod
// comes back under the same name and is redelivered what it owed (issue
// #461). Ending the run at once was the earlier "clean replay".
func TestSupervisorAwaitsALostWorkerOwingWork(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	w := workers["w1"]
	w.attached, w.hadSession = false, true
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	c.index["w1"].add(inflightBatch{id: 1, table: "t"})
	now := time.Now()
	s.pendingSetAt("w1", now.Add(-2*time.Minute))
	s.noteAck("w1", now.Add(-2*time.Minute))

	if err := s.tick(now, SupervisorConfig{AckTimeout: 30 * time.Second, AbsenceTimeout: 5 * time.Minute}); err != nil {
		t.Fatalf("tick: %v; a worker gone for 2m of a 5m absence timeout must be awaited", err)
	}
}

// A lost worker gone past the absence timeout ends the run: a Pod that never
// comes back (unschedulable, a volume that does not mount) is not a hiccup.
func TestSupervisorEndsTheRunForAWorkerGonePastTheAbsenceTimeout(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]
	w.attached, w.hadSession = false, true
	now := time.Now()
	s.pendingSetAt("w1", now.Add(-6*time.Minute))
	s.noteAck("w1", now.Add(-6*time.Minute))

	err := s.tick(now, SupervisorConfig{AckTimeout: 30 * time.Second, AbsenceTimeout: 5 * time.Minute})
	if err == nil || !strings.Contains(err.Error(), "not reconnected") {
		t.Fatalf("err = %v, want the run ended for a worker gone past the absence timeout", err)
	}
}

// A worker that never attached is not flagged even if it owes work: its
// session was never lost, so flagging it on boot would be a false terminate.
func TestSupervisorTickNeverAttachedNotFlagged(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]
	w.attached = false
	w.hadSession = false
	w.cancel = func() {}
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	if err := s.tick(time.Now(), SupervisorConfig{
		AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute,
	}); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if w.epoch != 0 || s.isPending("w1") {
		t.Fatalf("never-attached worker was reset: epoch=%d pending=%v", w.epoch, s.isPending("w1"))
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

// #208: recordReset returns the post-insert count read under the same lock, so
// the crashloop check that follows cannot race a concurrent reset.
func TestRecordResetReturnsWindowedCount(t *testing.T) {
	s := newSupervisor(&Coordinator{})
	now := time.Now()
	window := time.Minute

	if n := s.recordReset("w", now, window); n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}
	if n := s.recordReset("w", now.Add(time.Second), window); n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}
	// An entry older than the window is expired before counting.
	if n := s.recordReset("w", now.Add(2*window), window); n != 1 {
		t.Fatalf("count = %d, want 1 after expiry", n)
	}
}

// progressHarness is a worker that owes one in-flight batch and last acked
// ago: past the ack timeout.
func progressHarness(ago time.Duration) (*supervisor, *workerState, time.Time) {
	s, workers := supervisorHarness()
	s.c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	s.c.index["w1"].add(inflightBatch{id: 1, table: "t"})
	now := time.Now()
	s.noteAck("w1", now.Add(-ago))
	return s, workers["w1"], now
}

var progressCfg = SupervisorConfig{AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute}

// Slow storage: a worker writing a large file to a slow S3 acks nothing for
// longer than the ack timeout while its uploads advance. Terminating it
// replays the same load against the same storage, and the matrix run
// restarted 12 times without converging (#422). A worker whose network
// output grows is busy, not stalled.
func TestSupervisorSparesAWorkerMakingStorageProgress(t *testing.T) {
	s, w, now := progressHarness(2 * time.Minute)
	w.cancel = func() {}
	s.noteProgress("w1", 100<<20, now.Add(-40*time.Second))
	s.noteProgress("w1", 180<<20, now.Add(-5*time.Second))
	if err := s.tick(now, progressCfg); err != nil {
		t.Fatalf("tick terminated a worker whose uploads advance: %v", err)
	}
	if w.epoch != 0 || s.isPending("w1") {
		t.Fatalf("a worker making progress was reset: epoch=%d", w.epoch)
	}
}

// requireReset fails unless the stalled worker was reset (issue #461: a
// stalled upsert worker is reset and redelivered, not a reason to end the
// run).
func requireReset(t *testing.T, s *supervisor, w *workerState, err error, why string) {
	t.Helper()
	if err != nil {
		t.Fatalf("tick: %v; %s must be reset, not end the run", err, why)
	}
	if w.epoch != 1 || !s.isPending("w1") {
		t.Fatalf("epoch=%d pending=%v; %s must be reset", w.epoch, s.isPending("w1"), why)
	}
}

// A worker whose output stands still is stalled, as before, and is reset
// promptly.
func TestSupervisorResetsAWorkerWithoutProgress(t *testing.T) {
	s, w, now := progressHarness(2 * time.Minute)
	w.cancel = func() {}
	s.noteProgress("w1", 100<<20, now.Add(-90*time.Second))
	s.noteProgress("w1", 100<<20+4096, now.Add(-5*time.Second)) // heartbeats only
	requireReset(t, s, w, s.tick(now, progressCfg), "a worker without progress")
}

// Progress without an ack for long is not trusted forever: a worker that
// keeps sending (retrying one failing upload, say) but never acks is ended
// at progressAckCap × the ack timeout.
func TestSupervisorCapsProgressWithoutAcks(t *testing.T) {
	s, w, now := progressHarness(progressAckCap*30*time.Second + time.Second)
	w.cancel = func() {}
	s.noteProgress("w1", 100<<20, now.Add(-40*time.Second))
	s.noteProgress("w1", 900<<20, now.Add(-5*time.Second))
	requireReset(t, s, w, s.tick(now, progressCfg), "a worker past the progress cap")
}

// Growth is measured over the last ack timeout, not accumulated from an old
// baseline: trickles of control traffic that add up to 1 MiB over minutes
// are not progress (review of #450).
func TestSupervisorProgressIsGrowthWithinTheTimeout(t *testing.T) {
	s, w, now := progressHarness(2 * time.Minute)
	w.cancel = func() {}
	s.noteProgress("w1", 100<<20, now.Add(-100*time.Second))
	s.noteProgress("w1", 100<<20+600<<10, now.Add(-45*time.Second))
	s.noteProgress("w1", 100<<20+1200<<10, now.Add(-5*time.Second)) // 600 KiB in the last 30 s
	requireReset(t, s, w, s.tick(now, progressCfg), "a worker that grew under 1 MiB in the last timeout")
}
