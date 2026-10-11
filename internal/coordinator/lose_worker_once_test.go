package coordinator

import (
	"errors"
	"sync"
	"testing"
)

// A worker's Session and Control streams both end when it is lost, and each
// handler reports the loss from its own goroutine. That is one loss: the
// worker's epoch moves once. Bumped twice, the worker reconnects into an
// epoch the coordinator has already left, and what it sends is dropped as
// stale.
func TestALossSeenOnBothStreamsBumpsTheEpochOnce(t *testing.T) {
	for i := range 300 {
		c, w := coordHarness()
		var wg sync.WaitGroup
		start := make(chan struct{})
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_ = c.loseWorker(w.name, errors.New("stream closed"))
			}()
		}
		close(start)
		wg.Wait()
		c.mu.Lock()
		epoch := w.epoch
		c.mu.Unlock()
		if epoch != 1 {
			t.Fatalf("iteration %d: epoch = %d after one loss, want 1", i, epoch)
		}
	}
}
