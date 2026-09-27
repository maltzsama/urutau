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

// A worker whose Pod was deleted (a re-slice scale-in) was attached and is
// now detached with work owed: it can never drain its queue, so tick must
// terminate for a clean replay rather than reset (issue #372).
func TestSupervisorTickDetachedOwingTerminates(t *testing.T) {
	s, workers := supervisorHarness()
	c := s.c
	w := workers["w1"]
	w.attached = false
	w.hadSession = true
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	c.index["w1"].add(inflightBatch{id: 1, table: "t"})
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	err := s.tick(time.Now(), SupervisorConfig{
		AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute,
	})
	if err == nil || !strings.Contains(err.Error(), "detached") {
		t.Fatalf("err = %v, want a terminate for the detached, owing worker", err)
	}
}

// A detached worker that owes only QUEUED work (no in-flight batch) must also
// terminate: no session will ever drain its queue, so a reset strands it
// (issue #372).
func TestSupervisorTickDetachedQueuedTerminates(t *testing.T) {
	s, workers := supervisorHarness()
	w := workers["w1"]
	w.attached = false
	w.hadSession = true
	// supervisorHarness seeds one queued batch; the index stays empty, so the
	// worker owes via the queue alone, not an in-flight batch.
	s.noteAck("w1", time.Now().Add(-2*time.Minute))

	err := s.tick(time.Now(), SupervisorConfig{
		AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute,
	})
	if err == nil || !strings.Contains(err.Error(), "detached") {
		t.Fatalf("err = %v, want a terminate for the detached, queued-owing worker", err)
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

// A worker whose output stands still is stalled, as before: a wedged worker
// must still end the run promptly.
func TestSupervisorTerminatesAWorkerWithoutProgress(t *testing.T) {
	s, _, now := progressHarness(2 * time.Minute)
	s.noteProgress("w1", 100<<20, now.Add(-90*time.Second))
	s.noteProgress("w1", 100<<20+4096, now.Add(-5*time.Second)) // heartbeats only
	if err := s.tick(now, progressCfg); err == nil || !strings.Contains(err.Error(), "stalled") {
		t.Fatalf("err = %v, want the stall termination", err)
	}
}

// Progress without an ack for long is not trusted forever: a worker that
// keeps sending (retrying one failing upload, say) but never acks is ended
// at progressAckCap × the ack timeout.
func TestSupervisorCapsProgressWithoutAcks(t *testing.T) {
	s, _, now := progressHarness(progressAckCap*30*time.Second + time.Second)
	s.noteProgress("w1", 100<<20, now.Add(-40*time.Second))
	s.noteProgress("w1", 900<<20, now.Add(-5*time.Second))
	if err := s.tick(now, progressCfg); err == nil || !strings.Contains(err.Error(), "stalled") {
		t.Fatalf("err = %v, want the stall termination past the cap", err)
	}
}

// Growth is measured over the last ack timeout, not accumulated from an old
// baseline: trickles of control traffic that add up to 1 MiB over minutes
// are not progress (review of #450).
func TestSupervisorProgressIsGrowthWithinTheTimeout(t *testing.T) {
	s, _, now := progressHarness(2 * time.Minute)
	s.noteProgress("w1", 100<<20, now.Add(-100*time.Second))
	s.noteProgress("w1", 100<<20+600<<10, now.Add(-45*time.Second))
	s.noteProgress("w1", 100<<20+1200<<10, now.Add(-5*time.Second)) // 600 KiB in the last 30 s
	if err := s.tick(now, progressCfg); err == nil || !strings.Contains(err.Error(), "stalled") {
		t.Fatalf("err = %v, want the stall termination: under 1 MiB grew in the last timeout", err)
	}
}
