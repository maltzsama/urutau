package worker

import (
	"runtime/debug"
	"testing"
)

// The snapshot window size and in-flight count derive from the worker's
// runtime memory limit — not operator flags — so a bigger Pod gets bigger
// windows (fewer round-trips) and the retention stays a fixed fraction of
// memory (issue #622).
func TestSnapshotWindowBytes(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })
	for _, c := range []struct {
		mem  int64
		want int
	}{
		{512 << 20, 64 << 20}, // clamped low
		{1 << 30, 64 << 20},   // 1 GiB / 16
		{4 << 30, 256 << 20},  // 4 GiB / 16
		{8 << 30, 512 << 20},  // 8 GiB / 16
		{64 << 30, 512 << 20}, // clamped high
	} {
		debug.SetMemoryLimit(c.mem)
		if got := snapshotWindowBytes(); got != c.want {
			t.Errorf("mem=%d: windowBytes=%d, want %d", c.mem, got, c.want)
		}
	}
}

func TestSnapshotWindowInFlight(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })
	for _, c := range []struct {
		mem  int64
		want int
	}{
		{4 << 30, 2},  // window 256 MiB, 4 GiB/8 = 512 MiB → 2
		{16 << 30, 4}, // window 512 MiB (clamped), 16 GiB/8 = 2 GiB → 4
	} {
		debug.SetMemoryLimit(c.mem)
		if got := snapshotWindowInFlight(); got != c.want {
			t.Errorf("mem=%d: inFlight=%d, want %d", c.mem, got, c.want)
		}
	}
}

func TestRuntimeMemoryLimitFallsBackWhenUnset(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	debug.SetMemoryLimit(0)
	if got := runtimeMemoryLimit(); got != 4<<30 {
		t.Errorf("unset: runtimeMemoryLimit=%d, want 4 GiB", got)
	}
	debug.SetMemoryLimit(prev)
}
