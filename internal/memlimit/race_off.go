//go:build !race

package memlimit

// outsideHeap is the memory the binary uses that the runtime's limit does
// not count beyond what Fraction already leaves: none without the race
// detector.
func outsideHeap(int64) int64 { return 0 }
