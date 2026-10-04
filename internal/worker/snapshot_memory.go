package worker

import "runtime/debug"

// snapshotWindowBytes derives one DBLog snapshot window's byte cap from the
// worker's runtime memory limit. It is NOT an operator knob: a bigger Pod gets
// bigger windows (fewer round-trips), a smaller one smaller windows (tighter
// memory) — always bounded so the snapshot never holds a whole chunk of large
// payloads and OOMs the worker (issue #622).
func snapshotWindowBytes() int {
	mem := runtimeMemoryLimit()
	w := mem / 16
	if w < 64<<20 {
		w = 64 << 20
	}
	if w > 512<<20 {
		w = 512 << 20
	}
	return int(w)
}

// snapshotWindowInFlight derives how many byte-capped windows the chunk reader
// holds open at once, keeping ≈ 1/8 of the worker's memory in windows, at
// least two. Together with snapshotWindowBytes it bounds the snapshot's
// retention to in-flight × byte-cap.
func snapshotWindowInFlight() int {
	n := int(runtimeMemoryLimit() / 8 / int64(snapshotWindowBytes()))
	if n < 2 {
		n = 2
	}
	return n
}

// runtimeMemoryLimit is the Go runtime's soft memory limit (set by memlimit
// from the cgroup), or 4 GiB when none is set (tests, no cgroup).
func runtimeMemoryLimit() int64 {
	if mem := debug.SetMemoryLimit(-1); mem > 0 && mem < 1<<50 {
		return mem
	}
	return 4 << 30
}
