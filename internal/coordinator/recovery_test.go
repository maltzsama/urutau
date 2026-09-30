package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// scriptedTerminations stands in for the Kubernetes Pod status: each worker's
// last container termination, as the coordinator reads it when the worker
// comes back.
type scriptedTerminations map[string]podTermination

func (s scriptedTerminations) of(worker string) (podTermination, bool) {
	t, ok := s[worker]
	return t, ok
}

// crashAgain records one more restart of worker's container, ended as t.
func (s scriptedTerminations) crashAgain(worker string, t podTermination) {
	prev := s[worker]
	t.podUID = "uid-1"
	t.restarts = prev.restarts + 1
	s[worker] = t
}

// recoveryHarness is one worker, w1, that owes one batch (in the in-flight
// index) and is lost and comes back as the test scripts it.
func recoveryHarness(t *testing.T) (*Coordinator, *supervisor, scriptedTerminations) {
	t.Helper()
	s, _ := supervisorHarness()
	c := s.c
	c.sessionErrs = make(chan error, 8)
	c.index = map[string]*positionIndex{"w1": newPositionIndex("run-1")}
	c.index["w1"].add(inflightBatch{id: 1, table: "t"})
	terms := scriptedTerminations{"w1": {podUID: "uid-1"}}
	c.podTermination = terms.of
	return c, s, terms
}

// loseAndReturn is one loss of w1 followed by its reconnect.
func loseAndReturn(c *Coordinator, s *supervisor) error {
	c.signalSessionEnd("w1", context.Canceled)
	s.noteAttach("w1")
	return c.workerBack("w1")
}

// Issue #461: a crash loop is the same worker crashing again and again
// without delivering what it owed — a batch that OOM-kills it every time.
// The third such crash in a row ends the run, with the Pod's reason.
func TestConsecutiveCrashesEndTheRun(t *testing.T) {
	c, s, terms := recoveryHarness(t)
	for i := 1; i <= defaultMaxConsecutiveCrashes; i++ {
		terms.crashAgain("w1", podTermination{reason: "OOMKilled", exitCode: 137})
		err := loseAndReturn(c, s)
		if i < defaultMaxConsecutiveCrashes && err != nil {
			t.Fatalf("crash %d ended the run: %v", i, err)
		}
		if i == defaultMaxConsecutiveCrashes {
			if err == nil || !strings.Contains(err.Error(), "OOMKilled") {
				t.Fatalf("crash %d: err = %v, want the run ended citing OOMKilled", i, err)
			}
		}
	}
}

// A worker that loses its stream to the network is not crashing: a partition
// lasting several reconnects must never end the run.
func TestNetworkLossesAreNotCrashes(t *testing.T) {
	c, s, terms := recoveryHarness(t)
	for i := 0; i < 3*defaultMaxConsecutiveCrashes; i++ {
		terms.crashAgain("w1", podTermination{exitCode: 1, message: "network: worker: channel lost: connection timed out"})
		if err := loseAndReturn(c, s); err != nil {
			t.Fatalf("network loss %d ended the run: %v", i+1, err)
		}
	}
}

// A Pod deleted from outside (a chaos pod-kill, a node drain) comes back as a
// new Pod with no termination of its own: not a crash either.
func TestAReplacedPodIsNotACrash(t *testing.T) {
	c, s, terms := recoveryHarness(t)
	for i := 0; i < 3*defaultMaxConsecutiveCrashes; i++ {
		terms["w1"] = podTermination{podUID: "uid-new-" + string(rune('a'+i))}
		if err := loseAndReturn(c, s); err != nil {
			t.Fatalf("replaced Pod %d ended the run: %v", i+1, err)
		}
	}
}

// Crashes spaced out by healthy stretches are not a crash loop: once the
// worker has delivered everything it owed when it came back, and stayed up
// an ack timeout, the count starts over. A table that sees a crash every few
// hours must never be ended by them.
func TestDeliveringWhatItOwedStartsTheCountOver(t *testing.T) {
	c, s, terms := recoveryHarness(t)
	cfg := SupervisorConfig{AckTimeout: 30 * time.Second}
	for i := 0; i < 3*defaultMaxConsecutiveCrashes; i++ {
		terms.crashAgain("w1", podTermination{reason: "OOMKilled", exitCode: 137})
		if err := loseAndReturn(c, s); err != nil {
			t.Fatalf("spaced crash %d ended the run: %v", i+1, err)
		}
		// It delivers what it owed, then runs well for a while.
		c.index["w1"].truncateAll()
		c.index["w1"].add(inflightBatch{id: uint64(100 + i), table: "t"})
		s.noteAck("w1", time.Now())
		if err := s.tick(time.Now().Add(time.Minute), cfg); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
}

// A worker that owes nothing still crash-loops if it dies again before it has
// stayed up for an ack timeout: coming back is not enough.
func TestAnIdleWorkerCrashLoopEndsTheRun(t *testing.T) {
	c, s, terms := recoveryHarness(t)
	c.index["w1"].truncateAll()
	var err error
	for i := 0; i < defaultMaxConsecutiveCrashes; i++ {
		terms.crashAgain("w1", podTermination{exitCode: 2, message: "panic: nil map"})
		err = loseAndReturn(c, s)
	}
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("err = %v, want an idle worker crashing on every start to end the run", err)
	}
}

// A worker that delivers nothing while it owes work ends the run past the
// absence timeout, whether it is detached or reconnects and drops again every
// few seconds: each reconnect must not restart that clock.
func TestAWorkerDeliveringNothingPastTheDeliveryTimeoutEndsTheRun(t *testing.T) {
	c, s, _ := recoveryHarness(t)
	cfg := SupervisorConfig{AckTimeout: 30 * time.Second, DeliveryTimeout: 5 * time.Minute}
	now := time.Now()
	s.noteDeliveredAt("w1", now.Add(-6*time.Minute))
	c.signalSessionEnd("w1", context.Canceled)
	s.noteAttach("w1") // a flapping partition lets it reconnect for a moment
	err := s.tick(now, cfg)
	if err == nil || !strings.Contains(err.Error(), "delivered nothing") {
		t.Fatalf("err = %v, want the run ended for a worker delivering nothing for 6m", err)
	}
}

// A worker that owes nothing is never absent, however long it is gone: a
// quiet table's Pod stuck Pending hurts nothing until work arrives for it.
func TestAWorkerOwingNothingIsNeverAbsent(t *testing.T) {
	c, s, _ := recoveryHarness(t)
	c.index["w1"].truncateAll()
	<-c.workers["w1"].queue // the harness seeds one queued batch
	now := time.Now()
	s.noteDeliveredAt("w1", now.Add(-time.Hour))
	c.signalSessionEnd("w1", context.Canceled)
	if err := s.tick(now, SupervisorConfig{AckTimeout: 30 * time.Second, DeliveryTimeout: 5 * time.Minute}); err != nil {
		t.Fatalf("tick: %v; a worker owing nothing must not be absent", err)
	}
}

// The termination read from the Pod decides whether a loss was a crash.
func TestPodTerminationClassification(t *testing.T) {
	cases := []struct {
		name  string
		term  podTermination
		crash bool
	}{
		{"oom", podTermination{reason: "OOMKilled", exitCode: 137}, true},
		{"panic", podTermination{exitCode: 2, message: "panic: runtime error"}, true},
		{"error", podTermination{exitCode: 1, message: "worker: table t: commit: boom"}, true},
		{"network", podTermination{exitCode: 1, message: "network: worker: channel lost"}, false},
		{"clean exit", podTermination{exitCode: 0}, false},
	}
	for _, tc := range cases {
		if got, _ := tc.term.crash(); got != tc.crash {
			t.Errorf("%s: crash = %v, want %v", tc.name, got, tc.crash)
		}
	}
}

// A worker holds two streams, Session and Control, and both end when it is
// lost: that is one loss, and one crash when it comes back, not two.
func TestALossSeenOnBothStreamsCountsOnce(t *testing.T) {
	c, s, terms := recoveryHarness(t)
	for i := 1; i < defaultMaxConsecutiveCrashes; i++ {
		terms.crashAgain("w1", podTermination{reason: "OOMKilled", exitCode: 137})
		c.signalSessionEnd("w1", context.Canceled) // Session stream
		c.signalSessionEnd("w1", context.Canceled) // Control stream, same loss
		s.noteAttach("w1")
		if err := c.workerBack("w1"); err != nil {
			t.Fatalf("crash %d ended the run: %v", i, err)
		}
	}
}

// truncateAll acks every batch the index holds.
func (p *positionIndex) truncateAll() {
	p.mu.Lock()
	p.head = nil
	p.mu.Unlock()
}

// The supervisor starts after the snapshot, so its delivery-timeout rule does
// not cover a worker lost mid-snapshot: the snapshot waited for it without a
// deadline, and a worker that never came back held the run forever (PR #462
// review). The wait is bounded by the same --worker-delivery-timeout.
func TestASnapshotWaitsForALostWorkerOnlyUpToTheDeliveryTimeout(t *testing.T) {
	c, _, _ := recoveryHarness(t)
	c.cfg.WorkerDeliveryTimeout = 50 * time.Millisecond
	w := c.workers["w1"]
	c.signalSessionEnd("w1", context.Canceled)
	c.mu.Lock()
	w.attached = false
	c.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- c.awaitReattached(context.Background(), w) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "did not come back") {
			t.Fatalf("err = %v, want the run ended for a worker gone past the delivery timeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the snapshot still waits for a worker gone past the delivery timeout")
	}
}

// The supervisor ran only after the snapshot, so a table's only worker could
// come back from each kill and still deliver nothing for over the delivery
// timeout without ending the job (chaos-1M-c428fc5: pr_events, 5m22s). During
// the snapshot the delivery rule applies; the ack-timeout reset does not, as
// a reset mid-window would drop the window's rows.
func TestTheDeliveryTimeoutEndsTheRunDuringTheSnapshot(t *testing.T) {
	c, s, _ := recoveryHarness(t)
	c.snapshotActive.Store(true)
	cfg := SupervisorConfig{AckTimeout: 30 * time.Second, DeliveryTimeout: 5 * time.Minute}
	now := time.Now()
	s.noteDeliveredAt("w1", now.Add(-6*time.Minute))
	err := s.tick(now, cfg)
	if err == nil || !strings.Contains(err.Error(), "delivered nothing") {
		t.Fatalf("err = %v, want the run ended for a worker delivering nothing for 6m mid-snapshot", err)
	}
	_ = c
}

// Mid-snapshot a worker late on acks is not reset: only the delivery rule
// applies until the snapshot is done.
func TestNoAckTimeoutResetDuringTheSnapshot(t *testing.T) {
	c, s, _ := recoveryHarness(t)
	c.snapshotActive.Store(true)
	cfg := SupervisorConfig{AckTimeout: 30 * time.Second, DeliveryTimeout: 5 * time.Minute}
	now := time.Now()
	s.noteAck("w1", now.Add(-2*time.Minute))
	s.noteDeliveredAt("w1", now.Add(-2*time.Minute))
	c.mu.Lock()
	epoch := c.workers["w1"].epoch
	c.mu.Unlock()
	if err := s.tick(now, cfg); err != nil {
		t.Fatalf("tick: %v", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.workers["w1"].epoch != epoch {
		t.Fatal("a worker late on acks was reset mid-snapshot")
	}
}

// The snapshot can reach awaitReattached well after the loss (it was busy
// with another chunk). The delivery timeout counts from the loss, not from
// the start of the wait.
func TestTheSnapshotWaitForALostWorkerCountsFromTheLoss(t *testing.T) {
	c, s, _ := recoveryHarness(t)
	c.cfg.WorkerDeliveryTimeout = time.Minute
	w := c.workers["w1"]
	c.signalSessionEnd("w1", context.Canceled)
	c.mu.Lock()
	w.attached = false
	c.mu.Unlock()
	s.mu.Lock()
	s.healthOf("w1").lostAt = time.Now().Add(-2 * time.Minute)
	s.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- c.awaitReattached(context.Background(), w) }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "did not come back") {
			t.Fatalf("err = %v, want the run ended for a worker lost 2m ago with a 1m timeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the wait restarted the delivery timeout instead of counting from the loss")
	}
}
