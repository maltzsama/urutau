package routing

import (
	"reflect"
	"testing"
)

func TestHoldBuffersOnlyThePausedTable(t *testing.T) {
	var p Pause[int]
	if p.Hold("raw.a", 1) {
		t.Fatal("a table never paused held a batch")
	}
	p.Pause("raw.a")
	if !p.Paused("raw.a") || p.Paused("raw.b") {
		t.Fatalf("paused: a=%v b=%v", p.Paused("raw.a"), p.Paused("raw.b"))
	}
	if !p.Hold("raw.a", 1) {
		t.Fatal("the paused table did not hold its batch")
	}
	if p.Hold("raw.b", 9) {
		t.Fatal("another table's batch was held")
	}
}

// A batch for the resumed table that arrives before the router takes the
// held ones must queue behind them, or it would be routed ahead of older
// batches.
func TestABatchArrivingAfterResumeQueuesBehindTheHeldOnes(t *testing.T) {
	var p Pause[int]
	p.Pause("raw.a")
	p.Hold("raw.a", 1)
	p.Resume("raw.a")
	if !p.Hold("raw.a", 2) {
		t.Fatal("a batch arriving before the held ones were taken was not held")
	}
	if got := p.TakeResumed(); !reflect.DeepEqual(got, map[string][]int{"raw.a": {1, 2}}) {
		t.Fatalf("TakeResumed = %v, want raw.a:[1 2]", got)
	}
	if p.Hold("raw.a", 3) {
		t.Fatal("a batch was held after the table's held batches were taken")
	}
	if p.Held("raw.a") != 0 || len(p.HeldTables()) != 0 {
		t.Fatalf("held after take: %d, tables %v", p.Held("raw.a"), p.HeldTables())
	}
}

func TestTakeResumedLeavesAStillPausedTable(t *testing.T) {
	var p Pause[int]
	p.Pause("raw.a")
	p.Pause("raw.b")
	p.Hold("raw.a", 1)
	p.Hold("raw.b", 2)
	p.Resume("raw.b")
	if got := p.TakeResumed(); !reflect.DeepEqual(got, map[string][]int{"raw.b": {2}}) {
		t.Fatalf("TakeResumed = %v, want raw.b:[2]", got)
	}
	if got := p.HeldTables(); !reflect.DeepEqual(got, []string{"raw.a"}) {
		t.Fatalf("HeldTables = %v, want [raw.a]", got)
	}
}

// Several resumes before the router wakes leave one wake pending, and it
// must not block the resumer.
func TestResumeWakesWithoutBlocking(t *testing.T) {
	var p Pause[int]
	wake := p.Wake()
	p.Pause("raw.a")
	p.Pause("raw.b")
	p.Resume("raw.a")
	p.Resume("raw.b")
	select {
	case <-wake:
	default:
		t.Fatal("no wake after a resume")
	}
	select {
	case <-wake:
		t.Fatal("a second wake was queued")
	default:
	}
}
