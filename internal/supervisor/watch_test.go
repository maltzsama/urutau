package supervisor

import (
	"reflect"
	"testing"
	"time"
)

const (
	ackTimeout      = 30 * time.Second
	deliveryTimeout = 5 * time.Minute
)

// An attached worker that owes work and has acked nothing for the ack
// timeout is stalled; one that owes nothing is idle, however long ago it
// acked.
func TestWatchJudgeStalledNeedsOwedWork(t *testing.T) {
	now := time.Now()
	var w Watch
	w.NoteAck("busy", now.Add(-time.Minute))
	w.NoteAck("idle", now.Add(-time.Hour))
	w.NoteAck("fresh", now.Add(-time.Second))
	probes := []Probe{
		{Name: "busy", Attached: true, HadSession: true, Owes: true},
		{Name: "idle", Attached: true, HadSession: true},
		{Name: "fresh", Attached: true, HadSession: true, Owes: true},
	}
	stalled, gone := w.Judge(now, ackTimeout, deliveryTimeout, probes, false)
	if !reflect.DeepEqual(stalled, []string{"busy"}) || gone != nil {
		t.Fatalf("stalled=%v gone=%v, want only busy stalled", stalled, gone)
	}
}

// The ack clock starts at the last tick that saw the worker owing nothing:
// the wait for work is not a stall.
func TestWatchJudgeCountsFromTheLastIdleTick(t *testing.T) {
	start := time.Now()
	var w Watch
	w.NoteAck("w1", start)
	idle := []Probe{{Name: "w1", Attached: true, HadSession: true}}
	owing := []Probe{{Name: "w1", Attached: true, HadSession: true, Owes: true}}

	w.Judge(start.Add(time.Minute), ackTimeout, deliveryTimeout, idle, false)
	if stalled, _ := w.Judge(start.Add(time.Minute+ackTimeout/2), ackTimeout, deliveryTimeout, owing, false); stalled != nil {
		t.Fatalf("stalled=%v half an ack timeout after its first batch", stalled)
	}
	if stalled, _ := w.Judge(start.Add(time.Minute+2*ackTimeout), ackTimeout, deliveryTimeout, owing, false); len(stalled) != 1 {
		t.Fatalf("stalled=%v two ack timeouts after its first batch, want w1", stalled)
	}
}

// A worker awaited after a loss or a reset is not reset again, and none is
// mid-snapshot; the delivery timeout still ends the run for it.
func TestWatchJudgeAwaitsAPendingWorker(t *testing.T) {
	now := time.Now()
	var w Watch
	w.NoteAck("w1", now.Add(-time.Minute))
	owing := []Probe{{Name: "w1", Attached: true, HadSession: true, Owes: true}}

	if stalled, _ := w.Judge(now, ackTimeout, deliveryTimeout, owing, true); stalled != nil {
		t.Fatalf("stalled=%v mid-snapshot", stalled)
	}
	if w.MarkPending("w1") {
		t.Fatal("pending before it was marked")
	}
	if !w.MarkPending("w1") || !w.IsPending("w1") {
		t.Fatal("the second mark did not see the first")
	}
	if stalled, gone := w.Judge(now, ackTimeout, deliveryTimeout, owing, false); stalled != nil || gone != nil {
		t.Fatalf("stalled=%v gone=%v for a worker awaited after a reset", stalled, gone)
	}
	_, gone := w.Judge(now.Add(deliveryTimeout), ackTimeout, deliveryTimeout, owing, false)
	if !reflect.DeepEqual(gone, []string{"w1"}) {
		t.Fatalf("gone=%v past the delivery timeout, want w1", gone)
	}

	w.NoteAttach("w1", now.Add(deliveryTimeout))
	if w.IsPending("w1") {
		t.Fatal("still pending after its Hello")
	}
}

// A detached worker that has had a session is awaited, not reset; one whose
// output keeps growing is delivering to slow storage.
func TestWatchJudgeSparesDetachedAndBusyWorkers(t *testing.T) {
	now := time.Now()
	var w Watch
	w.NoteAck("lost", now.Add(-time.Minute))
	w.NoteAck("uploading", now.Add(-time.Minute))
	w.Progress.Note("uploading", 100<<20, now.Add(-40*time.Second))
	w.Progress.Note("uploading", 180<<20, now.Add(-5*time.Second))
	probes := []Probe{
		{Name: "lost", HadSession: true, Owes: true},
		{Name: "uploading", Attached: true, HadSession: true, Owes: true},
	}
	if stalled, gone := w.Judge(now, ackTimeout, deliveryTimeout, probes, false); stalled != nil || gone != nil {
		t.Fatalf("stalled=%v gone=%v, want none", stalled, gone)
	}
}

// A worker registered by a scale-out has its clocks seeded: attached with no
// ack on record, it would be reset on the first tick.
func TestWatchNoteRegisteredSeedsTheClocks(t *testing.T) {
	now := time.Now()
	owing := []Probe{{Name: "w1", Attached: true, Owes: true}}

	var unseeded Watch
	if stalled, _ := unseeded.Judge(now, ackTimeout, deliveryTimeout, owing, false); len(stalled) != 1 {
		t.Fatalf("stalled=%v for a worker with no ack on record, want w1", stalled)
	}
	var w Watch
	w.NoteRegistered("w1", now)
	w.NoteRegistered("w1", now.Add(-time.Hour)) // already seeded: kept
	if stalled, _ := w.Judge(now.Add(time.Second), ackTimeout, deliveryTimeout, owing, false); stalled != nil {
		t.Fatalf("stalled=%v right after registration", stalled)
	}
}

func TestWatchResetWindowAndForget(t *testing.T) {
	now := time.Now()
	const window = 10 * time.Minute
	var w Watch
	w.RecordReset("w1", now.Add(-20*time.Minute), window)
	w.RecordReset("w1", now.Add(-5*time.Minute), window)
	if n := w.RecordReset("w1", now, window); n != 2 {
		t.Fatalf("resets in the window = %d, want 2: the oldest expired", n)
	}
	w.NoteAck("w1", now)
	w.NoteDeliveredAt("w1", now)
	w.SetPending("w1")
	w.Progress.Note("w1", 1<<20, now)
	w.Health.Loss("w1", 1, now)
	if !w.Tracks("w1") {
		t.Fatal("no trace of a worker with state")
	}
	w.Forget("w1")
	if w.Tracks("w1") {
		t.Fatal("a trace outlived Forget: a reused name would inherit it")
	}
}
