//go:build race

package memlimit

// raceShadow is what the race detector holds outside the Go heap, measured on
// a worker under the full production-readiness load (#647).
const raceShadow = 1536 << 20

// outsideHeap is the memory a race build uses that the runtime's limit does
// not count: the detector's shadow memory, capped at a third of a small
// cgroup so the heap keeps the larger share.
func outsideHeap(cgroup int64) int64 {
	return min(raceShadow, cgroup/3)
}
