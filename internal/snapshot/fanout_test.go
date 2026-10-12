package snapshot

import (
	"reflect"
	"testing"
)

// The first window cycle an owner lost is dropped, and so is every later
// cycle of the pass: their progress was written before the loss and omits
// the lost chunk too. The pass then reports the chunk to read again from.
func TestACycleAfterALostOneIsDropped(t *testing.T) {
	var f FanoutPasses
	f.Begin("raw.a")
	f.Note("raw.a", 10, 3)
	f.Note("raw.a", 11, 4)
	f.Note("raw.a", 12, 5)

	if f.Resolve("raw.a", 10, false) {
		t.Fatal("a cycle every owner held was dropped")
	}
	if done, _, _ := f.Drained("raw.a"); done {
		t.Fatal("drained with two cycles unresolved")
	}
	if !f.Resolve("raw.a", 11, true) {
		t.Fatal("the cycle an owner lost was not dropped")
	}
	if !f.Resolve("raw.a", 12, false) {
		t.Fatal("a cycle after the lost one was not dropped")
	}
	done, from, lost := f.Drained("raw.a")
	if !done || !lost || from != 4 {
		t.Fatalf("Drained = %v, %d, %v; want done, from chunk 4, lost", done, from, lost)
	}
	// The next pass starts clean.
	if done, _, lost := f.Drained("raw.a"); !done || lost {
		t.Fatalf("second Drained = %v, lost=%v; want a clean pass", done, lost)
	}
}

// A cycle of no fan-out pass — a range snapshot's window, or one whose pass
// ended — commits whatever it says.
func TestACycleOfNoPassCommits(t *testing.T) {
	var f FanoutPasses
	if f.Resolve("raw.a", 1, true) {
		t.Fatal("a cycle of an untracked table was dropped")
	}
	f.Begin("raw.a")
	if f.Resolve("raw.a", 99, true) {
		t.Fatal("a cycle the pass never noted was dropped")
	}
	f.Note("raw.a", 1, 0)
	f.End("raw.a")
	if f.Resolve("raw.a", 1, true) {
		t.Fatal("a cycle was dropped after its pass ended")
	}
	if done, _, lost := f.Drained("raw.a"); !done || lost {
		t.Fatalf("Drained of an untracked table = %v, lost=%v", done, lost)
	}
}

func TestPendingFrom(t *testing.T) {
	if got := PendingFrom([]uint32{2, 3, 5, 8}, 5); !reflect.DeepEqual(got, []uint32{5, 8}) {
		t.Fatalf("PendingFrom = %v, want [5 8]", got)
	}
	if got := PendingFrom([]uint32{2, 3}, 9); got != nil {
		t.Fatalf("PendingFrom of a chunk outside the pass = %v, want nil", got)
	}
}

func TestPendingAfterKeepsLaterChunksAndLaterPartitions(t *testing.T) {
	todo := map[uint32]bool{
		ChunkRef(0, 0): true, ChunkRef(0, 1): true, ChunkRef(0, 2): true,
		ChunkRef(1, 0): true,
	}
	want := []uint32{ChunkRef(0, 2), ChunkRef(1, 0)}
	if got := PendingAfter(todo, 0, 1); !reflect.DeepEqual(got, want) {
		t.Fatalf("PendingAfter = %v, want %v", got, want)
	}
	if got := PendingAfter(todo, 1, 0); got != nil {
		t.Fatalf("PendingAfter the last chunk = %v, want nil", got)
	}
}
