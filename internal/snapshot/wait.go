package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// ErrWorkerLost is a chunk round-trip cut short by its worker's loss.
var ErrWorkerLost = errors.New("worker lost")

// WaitCaughtUpOrLost runs the DBLog caught-up proof but aborts the moment the
// chunk's worker is lost, returning ErrWorkerLost so the partition redoes the
// chunk. A loss mid-catch-up leaves the worker's queue full, which blocks the
// shared source pump and stalls the reader, so the reader can never reach high.
// Without this the wait burns the whole window timeout and ends the run
// (issue #526) instead of replaying, the way #461 recovers a lost worker.
func WaitCaughtUpOrLost(ctx context.Context, rdr source.SourceReader, high position.Position, cfg SnapshotConfig, windowID uint64, lost <-chan struct{}) error {
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

	err := WaitCaughtUp(waitCtx, rdr, high, cfg)
	if err != nil {
		select {
		case <-lost:
			return ErrWorkerLost
		default:
		}
		return fmt.Errorf("dblog: window %d: %w", windowID, err)
	}
	return nil
}

// DecodeHighKey decodes a WindowOpen's high_key bytes into the PK tuple it
// carries, or nil when the window has no high key (the whole-chunk path).
func DecodeHighKey(b []byte) ([]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	rows, err := transport.DecodeBounds(b)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows[0], nil
}

// fanoutDrainPoll is how often Await looks at a pass.
const fanoutDrainPoll = 20 * time.Millisecond

// Await returns once every window cycle of target's pass is committed or
// dropped, with the first chunk an owner lost, if any. A cycle waits for
// every owner's delivery; an owner that never comes back ends the run through
// the supervisor, which cancels ctx.
func (f *FanoutPasses) Await(ctx context.Context, target string) (from uint32, lost bool, err error) {
	poll := time.NewTicker(fanoutDrainPoll)
	defer poll.Stop()
	for {
		if done, from, lost := f.Drained(target); done {
			return from, lost, nil
		}
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		case <-poll.C:
		}
	}
}
