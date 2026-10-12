//go:build race

package memlimit

// raceShadowPerHeap is the race detector's memory outside the Go heap per
// byte of heap the runtime has mapped. Measured on the events worker under
// the full-100k production-readiness load: 2.8 GiB for a heap of 1.4 GiB, and
// it is not given back when the heap shrinks (#822).
const raceShadowPerHeap = 2

// outsideHeap is the memory a race build uses that the runtime's limit does
// not count: the detector's shadow memory, which grows with the heap. The
// heap gets the share of the cgroup's budget that leaves raceShadowPerHeap
// times itself for the detector.
func outsideHeap(cgroup int64) int64 {
	budget := int64(float64(cgroup) * Fraction)
	return budget - budget/(1+raceShadowPerHeap)
}
