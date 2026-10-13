package supervisor

import (
	"context"
	"errors"
	"time"

	"github.com/maltzsama/urutau/internal/coordk8s"
)

const (
	// DefaultMaxConsecutiveCrashes is how many times in a row one worker may
	// crash without delivering what it owed before the run ends.
	DefaultMaxConsecutiveCrashes = 3
	// DefaultDeliveryTimeout is how long a lost worker may take to reconnect,
	// or a worker owing work may deliver none of it, before the run ends.
	DefaultDeliveryTimeout = 5 * time.Minute
	// DefaultAckTimeout is how long an attached worker owing work may go
	// without an ack before it is reset.
	DefaultAckTimeout = 30 * time.Second
	// reattachPoll is how often AwaitBack re-checks a lost worker's return.
	reattachPoll = 50 * time.Millisecond
)

// ErrCrashLoop marks a run ended by a worker crashing again and again.
var ErrCrashLoop = errors.New("crash loop")

// ErrNotBack is a lost worker that did not come back in time.
var ErrNotBack = errors.New("worker did not come back")

// MaxCrashes is the configured crash limit, or its default.
func MaxCrashes(configured int) int {
	if configured > 0 {
		return configured
	}
	return DefaultMaxConsecutiveCrashes
}

// DeliveryTimeout is the configured delivery timeout, or its default.
func DeliveryTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return DefaultDeliveryTimeout
}

// AckTimeout is the configured ack timeout, or its default.
func AckTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return DefaultAckTimeout
}

// Return is what a worker coming back means.
type Return struct {
	// Crashed: the loss it comes back from was a crash, described by Desc.
	Crashed bool
	Desc    string
	// Crashes is its crashes in a row without delivering what it owed.
	Crashes int
	// Loop: this crash reached the limit; the run ends.
	Loop bool
}

// Back classifies the loss a reconnected worker comes back from (see
// Health.Back) and says whether it is a crash loop: limit crashes in a row
// without the worker delivering everything it owed when it came back before
// crashing again (Judge starts the count over).
func (w *Watch) Back(worker string, term coordk8s.Termination, known bool, owedMark uint64, now time.Time, limit int) Return {
	n, crashed, desc := w.Health.Back(worker, term, known, owedMark, now)
	return Return{Crashed: crashed, Desc: desc, Crashes: n, Loop: crashed && n >= limit}
}

// AwaitBack returns once worker is attached and no longer awaited after a
// loss or a reset, or ErrNotBack when it has not come back within timeout of
// its loss. The supervision's tick starts after the snapshot, so its delivery
// timeout does not cover a wait made during it: this one does.
func (w *Watch) AwaitBack(ctx context.Context, worker string, timeout time.Duration, attached func() bool) error {
	deadline := w.Health.LossTime(worker, time.Now()).Add(timeout)
	poll := time.NewTicker(reattachPoll)
	defer poll.Stop()
	for {
		if attached() && !w.IsPending(worker) {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrNotBack
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		}
	}
}

// Run calls tick every poll until ctx is done or a tick returns an error,
// which it hands to terminate: the run goes terminal.
func Run(ctx context.Context, poll time.Duration, tick func(now time.Time) error, terminate chan<- error) {
	if poll <= 0 {
		poll = time.Second
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := tick(time.Now()); err != nil {
				select {
				case terminate <- err:
				case <-ctx.Done():
				}
				return
			}
		}
	}
}
