package remote

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

const (
	// reconnectMin and reconnectMax bound the wait before the worker attaches
	// again. The cap stays well under the time a restarted coordinator waits
	// for its workers.
	reconnectMin = 500 * time.Millisecond
	reconnectMax = 10 * time.Second
	// reconnectSettled is how long a session must have lasted for the next
	// loss to start from reconnectMin again.
	reconnectSettled = time.Minute
)

// errPipelineRunning marks a coordinator loss after which the worker's
// pipeline did not stop within its grace period.
var errPipelineRunning = errors.New("worker: pipeline still running")

// pipelineRunning is err marked errPipelineRunning.
func pipelineRunning(err error) error { return fmt.Errorf("%w: %w", err, errPipelineRunning) }

// reconnectable reports whether the worker may attach again in this process
// after a session ended with err: the coordinator closed the session (nil)
// or was lost, and the session's pipeline is known to have stopped. A
// pipeline that did not stop could still land a commit next to the new
// session's — only the end of the process rules that out. Any other error
// is the worker's own and must surface.
func reconnectable(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, ErrCoordinatorLost) && !errors.Is(err, errPipelineRunning)
}

// Run runs the worker until ctx ends: it attaches to the coordinator, serves
// the session (RunRemote) and, when the coordinator goes away, attaches
// again.
//
// Leaving the recovery to the kubelet — exit, be restarted — makes every
// coordinator restart and network fault a container restart, and the
// kubelet's restart back-off grows with each one up to five minutes. A
// restarted coordinator waits for its workers before it reads the source, so
// one Pod in CrashLoopBackOff stopped every table of the pipeline (#841).
// A session builds everything it uses from scratch, so a new one in the same
// process starts from the same state a restarted process would.
func Run(ctx context.Context, cfg RemoteConfig) error {
	normalizeRemoteConfig(&cfg)
	wait := reconnectMin
	for {
		started := time.Now()
		err := RunRemote(ctx, cfg)
		if ctx.Err() != nil || !reconnectable(err) {
			return err
		}
		if time.Since(started) >= reconnectSettled {
			wait = reconnectMin
		}
		d := wait + time.Duration(rand.Int64N(int64(wait)/2+1))
		cfg.Logger.Warn("worker: coordinator gone; attaching again", "in", d, "cause", err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(d):
		}
		wait = min(wait*2, reconnectMax)
	}
}
