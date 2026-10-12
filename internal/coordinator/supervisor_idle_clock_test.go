package coordinator

import (
	"log/slog"
	"testing"
	"time"
)

// A worker that connects and then waits for work has acked nothing, because
// it was sent nothing. After a coordinator restart the workers reconnect up
// to a minute before the reader starts; the first batch queued for one of
// them must not find its ack clock already past the timeout. The clock that
// decides a stall starts when the worker starts owing work.
func TestSupervisorDoesNotResetAWorkerForTheTimeItWaitedForWork(t *testing.T) {
	workers := map[string]*workerState{
		"w1": {name: "w1", attached: true, queue: make(chan queuedBatch, 4)},
	}
	c := &Coordinator{workers: workers, log: slog.New(slog.DiscardHandler)}
	s := newSupervisor(c)
	c.supervisor = s
	w := workers["w1"]
	reset := false
	w.cancel = func() { reset = true }
	cfg := SupervisorConfig{AckTimeout: 30 * time.Second, MaxResets: 5, ResetWindow: 15 * time.Minute}

	start := time.Now()
	s.NoteAck("w1", start) // its Hello, as NoteAttach records it

	// A minute with nothing to do.
	for d := time.Second; d <= time.Minute; d += time.Second {
		if err := s.tick(start.Add(d), cfg); err != nil {
			t.Fatalf("tick while idle: %v", err)
		}
	}
	if reset {
		t.Fatal("an idle worker was reset")
	}

	// The reader starts and the first batch is queued.
	w.queue <- queuedBatch{id: 1}
	owing := start.Add(time.Minute + time.Second)
	if err := s.tick(owing, cfg); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if reset || w.epoch != 0 {
		t.Fatalf("a worker was reset the moment it was given work: epoch=%d", w.epoch)
	}

	// Still inside the timeout, counted from when it started owing.
	if err := s.tick(owing.Add(29*time.Second), cfg); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if reset {
		t.Fatal("a worker was reset before owing work for the ack timeout")
	}

	// Owing the same batch for longer than the timeout is a stall.
	if err := s.tick(owing.Add(31*time.Second), cfg); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if !reset || w.epoch != 1 {
		t.Fatalf("a worker owing work past the ack timeout was not reset: epoch=%d", w.epoch)
	}
}
