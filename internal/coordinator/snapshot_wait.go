package coordinator

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// waitCaughtUpOrLost runs the DBLog caught-up proof but aborts the moment the
// chunk's worker is lost, returning errWorkerLost so the partition redoes the
// chunk. A loss mid-catch-up leaves the worker's queue full, which blocks the
// shared source pump and stalls the reader, so the reader can never reach high.
// Without this the wait burns the whole window timeout and ends the run
// (issue #526) instead of replaying, the way #461 recovers a lost worker.
func (c *Coordinator) waitCaughtUpOrLost(ctx context.Context, rdr source.SourceReader, high position.Position, cfg snapshot.SnapshotConfig, lost <-chan struct{}) error {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-lost:
			cancel()
		case <-done:
		}
	}()

	err := snapshot.WaitCaughtUp(waitCtx, rdr, high, cfg)
	if err != nil {
		select {
		case <-lost:
			return errWorkerLost
		default:
		}
		return fmt.Errorf("dblog: %w", err)
	}
	return nil
}
