package supervisor

import (
	"testing"
	"time"
)

const ack = 30 * time.Second

// Output that grew by ProgressMinBytes over the last ack timeout marks the
// worker busy; heartbeats alone do not.
func TestProgressBusyNeedsGrowthWithinTheAckTimeout(t *testing.T) {
	now := time.Now()
	lastAck := now.Add(-time.Minute)

	var p Progress
	if p.Busy("w1", now, lastAck, ack) {
		t.Fatal("busy with no sample")
	}
	p.Note("w1", 100<<20, now.Add(-40*time.Second))
	if p.Busy("w1", now, lastAck, ack) {
		t.Fatal("busy with one sample")
	}
	p.Note("w1", 180<<20, now.Add(-5*time.Second))
	if !p.Busy("w1", now, lastAck, ack) {
		t.Fatal("not busy after 80 MiB of output in the last ack timeout")
	}

	var idle Progress
	idle.Note("w1", 100<<20, now.Add(-40*time.Second))
	idle.Note("w1", 100<<20+4096, now.Add(-5*time.Second))
	if idle.Busy("w1", now, lastAck, ack) {
		t.Fatal("busy on heartbeats alone")
	}
}

// Growth is measured over the last ack timeout, not accumulated from an old
// sample.
func TestProgressBusyIgnoresGrowthOlderThanTheAckTimeout(t *testing.T) {
	now := time.Now()
	var p Progress
	p.Note("w1", 100<<20, now.Add(-100*time.Second))
	p.Note("w1", 100<<20+600<<10, now.Add(-45*time.Second))
	p.Note("w1", 100<<20+1200<<10, now.Add(-5*time.Second)) // 600 KiB in the last 30 s
	if p.Busy("w1", now, now.Add(-time.Minute), ack) {
		t.Fatal("busy on growth that mostly predates the last ack timeout")
	}
}

// Progress stands in for acks only up to ProgressAckCap timeouts, and not at
// all once the reports stop.
func TestProgressBusyIsCapped(t *testing.T) {
	now := time.Now()
	var p Progress
	p.Note("w1", 100<<20, now.Add(-40*time.Second))
	p.Note("w1", 900<<20, now.Add(-5*time.Second))
	if p.Busy("w1", now, now.Add(-ProgressAckCap*ack-time.Second), ack) {
		t.Fatal("busy past the cap on ack timeouts")
	}
	if p.Busy("w1", now, time.Time{}, ack) {
		t.Fatal("busy with no ack on record")
	}
	if p.Busy("w1", now.Add(ack+10*time.Second), now, ack) {
		t.Fatal("busy although the last report is older than the ack timeout")
	}
}

// A lower counter is a new process: its samples start over. Forget drops
// them.
func TestProgressRestartsOnALowerCounter(t *testing.T) {
	now := time.Now()
	var p Progress
	p.Note("w1", 0, now) // no output reported: not a sample
	if p.Tracks("w1") {
		t.Fatal("a zero counter was recorded")
	}
	p.Note("w1", 500<<20, now.Add(-20*time.Second))
	p.Note("w1", 2<<20, now.Add(-10*time.Second)) // restarted
	p.Note("w1", 3<<20, now.Add(-5*time.Second))
	if !p.Busy("w1", now, now.Add(-time.Minute), ack) {
		t.Fatal("not busy: the new process sent 1 MiB")
	}
	p.Forget("w1")
	if p.Tracks("w1") || p.Busy("w1", now, now.Add(-time.Minute), ack) {
		t.Fatal("samples kept after Forget")
	}
}
