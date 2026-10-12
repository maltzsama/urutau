package supervisor

import (
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/coordk8s"
)

func oom(uid string, restarts int32) coordk8s.Termination {
	return coordk8s.Termination{PodUID: uid, Restarts: restarts, Reason: "OOMKilled", ExitCode: 137}
}

// Both of a worker's streams report its loss: the second report of the same
// epoch is not a new loss, and the loss keeps the time it was first seen.
func TestHealthLossIsSeenOncePerEpoch(t *testing.T) {
	var h Health
	now := time.Now()
	if got := h.LossTime("w1", now); !got.Equal(now) {
		t.Fatalf("loss time of a worker never lost = %v, want now", got)
	}
	if !h.Loss("w1", 2, now.Add(-time.Minute)) {
		t.Fatal("the first loss was not recorded")
	}
	if h.Loss("w1", 2, now) {
		t.Fatal("the same loss, seen on the other stream, was recorded again")
	}
	if got := h.LossTime("w1", now); !got.Equal(now.Add(-time.Minute)) {
		t.Fatalf("loss time = %v, want when it was first seen", got)
	}
	if !h.Loss("w1", 3, now) {
		t.Fatal("a loss at a later epoch was not recorded")
	}
}

// A loss is a crash when the same Pod restarted since it was last seen, or
// when a Pod seen for the first time already counts a restart.
func TestHealthBackCountsFreshCrashes(t *testing.T) {
	var h Health
	now := time.Now()

	h.Loss("w1", 1, now)
	if n, crashed, _ := h.Back("w1", oom("pod-a", 1), true, 0, now); !crashed || n != 1 {
		t.Fatalf("first sighting with a restart: crashes=%d crashed=%v, want 1 true", n, crashed)
	}
	// Back again with nothing new on the Pod: a reconnect, not a crash.
	h.Loss("w1", 2, now)
	if n, crashed, _ := h.Back("w1", oom("pod-a", 1), true, 0, now); crashed || n != 1 {
		t.Fatalf("no new restart: crashes=%d crashed=%v, want 1 false", n, crashed)
	}
	h.Loss("w1", 3, now)
	if n, crashed, desc := h.Back("w1", oom("pod-a", 2), true, 0, now); !crashed || n != 2 || desc == "" {
		t.Fatalf("a new restart: crashes=%d crashed=%v desc=%q, want 2 true and a description", n, crashed, desc)
	}
	// A Pod replaced from outside has another UID: its restarts are not
	// compared with the old Pod's.
	h.Loss("w1", 4, now)
	if n, crashed, _ := h.Back("w1", oom("pod-b", 5), true, 0, now); crashed || n != 2 {
		t.Fatalf("a replaced Pod: crashes=%d crashed=%v, want 2 false", n, crashed)
	}
	// No loss pending: a restart read on an attach that followed no loss is
	// not counted.
	if n, crashed, _ := h.Back("w1", oom("pod-b", 6), true, 0, now); crashed || n != 2 {
		t.Fatalf("no loss pending: crashes=%d crashed=%v, want 2 false", n, crashed)
	}
	// Outside Kubernetes there is no Pod to read: no loss is a crash.
	h.Loss("w1", 5, now)
	if n, crashed, _ := h.Back("w1", coordk8s.Termination{}, false, 0, now); crashed || n != 2 {
		t.Fatalf("no termination known: crashes=%d crashed=%v, want 2 false", n, crashed)
	}
}

// A worker that exited because it lost the coordinator did not crash.
func TestHealthBackDoesNotCountANetworkExit(t *testing.T) {
	var h Health
	now := time.Now()
	h.Loss("w1", 1, now)
	term := coordk8s.Termination{PodUID: "pod-a", Restarts: 1, Reason: "Error", ExitCode: 1, Message: coordk8s.NetworkExitPrefix + "coordinator lost"}
	if n, crashed, _ := h.Back("w1", term, true, 0, now); crashed || n != 0 {
		t.Fatalf("network exit: crashes=%d crashed=%v, want 0 false", n, crashed)
	}
}

// The crash count starts over only once the worker delivered every batch it
// owed when it came back and stayed up for the ack timeout.
func TestHealthDeliveringStartsTheCountOver(t *testing.T) {
	const ack = 30 * time.Second
	var h Health
	back := time.Now()
	h.Loss("w1", 1, back)
	h.Back("w1", oom("pod-a", 1), true, 40, back)
	if got := h.Recovering(); !got["w1"] {
		t.Fatalf("recovering = %v, want w1", got)
	}

	h.Delivering("w1", 40, back.Add(time.Minute), ack) // still holds a batch it owed
	if h.Crashes("w1") != 1 {
		t.Fatal("the count started over while the worker still owed a batch from before the crash")
	}
	h.Delivering("w1", 41, back.Add(ack/2), ack) // delivered, but not up for long enough
	if h.Crashes("w1") != 1 {
		t.Fatal("the count started over before the worker stayed up for the ack timeout")
	}
	h.Loss("w1", 2, back)
	h.Delivering("w1", 0, back.Add(time.Minute), ack) // lost again: not classified yet
	if h.Crashes("w1") != 1 {
		t.Fatal("the count started over with a loss pending")
	}
	if got := h.Recovering(); got != nil {
		t.Fatalf("recovering = %v, want none while the loss is pending", got)
	}
	h.Back("w1", oom("pod-a", 1), true, 40, back)
	h.Delivering("w1", 41, back.Add(time.Minute), ack)
	if h.Crashes("w1") != 0 {
		t.Fatal("the count did not start over once the worker delivered what it owed")
	}
	if got := h.Recovering(); got != nil {
		t.Fatalf("recovering = %v, want none", got)
	}
}

func TestHealthForget(t *testing.T) {
	var h Health
	now := time.Now()
	h.Loss("w1", 1, now)
	h.Back("w1", oom("pod-a", 1), true, 0, now)
	if !h.Tracks("w1") {
		t.Fatal("no record of a worker that was lost")
	}
	h.Forget("w1")
	if h.Tracks("w1") || h.Crashes("w1") != 0 {
		t.Fatal("the record outlived Forget: a reused name would inherit its crashes")
	}
}
