package remote

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// sessionSender serializes Session sends: grpc client streams are not
// concurrent-safe, and commits ack from per-table goroutines.
type sessionSender struct {
	mu sync.Mutex
	s  pb.UrutauControl_SessionClient
}

func (s *sessionSender) send(msg *pb.WorkerMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Send(msg)
}

// workerHandshake opens the Session, sends the Hello and waits for the
// Assignment, retrying the whole sequence with backoff. A worker whose name
// the coordinator does not know yet is rejected as unknown — and under KEDA
// a scale-out starts the pod before the coordinator's reconcile loop has
// registered the new owner, so that rejection is expected, not fatal.
// Retrying lets the worker connect once the owner exists instead of
// crash-looping. sessionWithRetry still handles a coordinator that is only
// starting its listener.
func workerHandshake(ctx context.Context, conn *grpc.ClientConn, name string, log *slog.Logger) (pb.UrutauControl_SessionClient, *pb.Assignment, error) {
	const maxTries = 20
	const base = 250 * time.Millisecond
	const cap = 10 * time.Second
	var last error
	for attempt := 0; attempt < maxTries; attempt++ {
		if attempt > 0 {
			d := base << (attempt - 1)
			if d > cap {
				d = cap
			}
			d += time.Duration(rand.Int64N(int64(d)/2 + 1))
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(d):
			}
		}
		// Each attempt owns its Session: a failed attempt closes it, or the
		// coordinator keeps the worker attached to it and refuses every
		// retry as "already connected" (chaos-1M-521691a).
		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		session, err := sessionWithRetry(attemptCtx, conn, log)
		if err != nil {
			cancelAttempt()
			last = err
			continue
		}
		sender := &sessionSender{s: session}
		err = sender.send(&pb.WorkerMessage{Msg: &pb.WorkerMessage_Hello{Hello: &pb.Hello{
			WorkerName: name,
			Phase:      pb.WorkerPhase_WORKER_PHASE_STARTING,
			Epoch:      1,
		}}})
		if err == nil {
			var msg *pb.CoordinatorMessage
			if msg, err = session.Recv(); err == nil {
				if assign := msg.GetAssign(); assign != nil {
					context.AfterFunc(ctx, cancelAttempt)
					return session, assign, nil
				}
				err = errors.New("worker: expected Assignment, got none")
			}
		}
		cancelAttempt()
		last = err
		log.Warn("worker: handshake retry", "attempt", attempt+1, "err", err)
	}
	return nil, nil, CoordinatorLost(fmt.Errorf("worker: handshake: %w", last))
}
