//go:build race

package memlimit

import "testing"

// The race detector keeps its shadow memory outside the Go heap, where the
// runtime's limit does not count it, and that memory grows with the heap:
// measured on the full-100k events worker, about twice what the heap had
// mapped (2.8 GiB for 1.4 GiB), and it stays after the heap shrinks. A fixed
// 1.5 GiB allowance left a 5 GiB worker a 3 GiB heap limit, and the worker
// was OOM-killed in its snapshot (#822). The heap's share must leave twice
// itself for the detector.
func TestRaceBuildLeavesRoomForTheDetector(t *testing.T) {
	const gib = int64(1) << 30
	for _, cgroup := range []int64{gib, 5 * gib, 16 * gib} {
		got := runtimeShare(cgroup)
		budget := int64(float64(cgroup) * Fraction)
		if 3*got > budget {
			t.Errorf("runtimeShare(%d MiB) = %d MiB: the heap and twice itself of shadow memory is %d MiB, over the %d MiB the cgroup leaves",
				cgroup>>20, got>>20, 3*got>>20, budget>>20)
		}
		// The heap still gets all of its third: no more is held back than the
		// detector takes.
		if got < budget/3-1 {
			t.Errorf("runtimeShare(%d MiB) = %d MiB, want a third of %d MiB", cgroup>>20, got>>20, budget>>20)
		}
	}
}
