package coordinator

import (
	"sync"
	"testing"
)

// The worker registry is mutated at runtime by live re-slicing (issue #312),
// so every reader must take c.mu. Before issue #479 onAck, onMarkerAck,
// gracefulShutdown and the dashboard summary read the map unlocked and raced
// registerOwner/retireOwner — a "concurrent map read and map write" that the
// race-instrumented pod e2e caught on the ack hot path.
func TestWorkerRegistryReadsAreRaceFree(t *testing.T) {
	c, w := coordHarness()

	const iterations = 2000
	var wg sync.WaitGroup

	// A writer mutating the registry the way registerOwner/retireOwner do.
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := range iterations {
			c.mu.Lock()
			if i%2 == 0 {
				c.workers["w0"] = w
			} else {
				delete(c.workers, "w0")
			}
			c.mu.Unlock()
		}
	}()

	readers := map[string]func(){
		"workerFor":        func() { _ = c.workerFor("w0") },
		"workersSnapshot":  func() { _ = c.workersSnapshot() },
		"gracefulShutdown": func() { c.gracefulShutdown() },
		"onMarkerAck":      func() { c.onMarkerAck("w0", 0) },
		"summary":          func() { _ = dashState{c}.Summary() },
		"anyWorkerDown":    func() { _ = c.anyWorkerDown() },
	}

	for name, fn := range readers {
		wg.Add(1)

		go func(name string, fn func()) {
			defer wg.Done()

			for range iterations {
				fn()
			}
		}(name, fn)
	}

	wg.Wait()
}
