package coordinator

import (
	"context"
	"time"
)

const fanoutDrainPoll = 20 * time.Millisecond

// awaitFanoutPass returns once every window cycle of target's pass is
// committed or dropped, with the first chunk an owner lost, if any. A cycle
// waits for every owner's delivery; an owner that never comes back ends the
// run through the supervisor, which cancels ctx.
func (c *Coordinator) awaitFanoutPass(ctx context.Context, target string) (from uint32, lost bool, err error) {
	poll := time.NewTicker(fanoutDrainPoll)
	defer poll.Stop()
	for {
		if done, from, lost := c.fanout.Drained(target); done {
			return from, lost, nil
		}
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		case <-poll.C:
		}
	}
}
