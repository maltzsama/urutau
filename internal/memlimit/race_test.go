//go:build race

package memlimit

import "testing"

// The race detector keeps its shadow memory outside the Go heap, where the
// runtime's limit does not count it. A race worker given 0.9 of a 5 GiB
// cgroup let its heap reach 4.5 GiB with ~1.5 GiB of shadow memory on top and
// was OOM-killed in the middle of a snapshot (#647, #822).
func TestRaceBuildLeavesRoomForTheDetector(t *testing.T) {
	const gib = int64(1) << 30
	got := runtimeShare(5 * gib)
	if room := 5*gib - got; room < 2*gib {
		t.Fatalf("runtimeShare(5 GiB) = %d MiB, leaving %d MiB outside the heap; the detector alone takes ~1536 MiB",
			got>>20, room>>20)
	}
	if got < 2*gib {
		t.Fatalf("runtimeShare(5 GiB) = %d MiB: the heap is left too little to work", got>>20)
	}
	// A small cgroup still gets a usable heap.
	if small := runtimeShare(gib); small < gib/2 {
		t.Fatalf("runtimeShare(1 GiB) = %d MiB, want at least half of it", small>>20)
	}
}
