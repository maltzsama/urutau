package inflight

import "testing"

func TestIndexesZeroValue(t *testing.T) {
	var x Indexes
	if x.Of("w0") != nil || x.InFlight("w0") != 0 || len(x.Snapshot()) != 0 {
		t.Fatal("an empty registry reports an index")
	}
	x.Delete("w0") // no panic on an empty registry

	idx := NewPositionIndex("run-1")
	idx.Add(Batch{ID: 1, Table: "raw.a"})
	x.Set("w0", idx)
	if x.Of("w0") != idx || x.InFlight("w0") != 1 {
		t.Fatalf("index of w0 = %p with %d in flight, want the one set with 1", x.Of("w0"), x.InFlight("w0"))
	}

	// The snapshot is a copy: a later write does not reach it.
	snap := x.Snapshot()
	x.Set("w1", NewPositionIndex("run-1"))
	x.Delete("w0")
	if len(snap) != 1 || snap["w0"] != idx {
		t.Fatalf("snapshot = %v, want only w0", snap)
	}
	if x.Of("w0") != nil || x.Of("w1") == nil {
		t.Fatal("the registry did not follow the delete and the set")
	}
}
