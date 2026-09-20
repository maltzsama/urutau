package coordinator

import (
	"testing"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// #207: onStagedBatch must read w.epoch under c.mu — resetWorker writes it
// under c.mu from another goroutine. Run with -race; the unlocked read was a
// data race.
func TestOnStagedBatchEpochRace(t *testing.T) {
	c, w := coordHarness()
	// A stale-epoch batch: onStagedBatch reads the epoch, then returns.
	sb := &pb.StagedBatch{Epoch: w.epoch + 1, Table: "raw.orders"}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 2000 {
			c.resetWorker(w)
		}
	}()
	for range 2000 {
		c.onStagedBatch(w.name, sb)
	}
	<-done
}
