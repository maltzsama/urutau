package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// ── gRPC control plane ───────────────────────────────────────────────

type controlServer struct {
	pb.UnimplementedUrutauControlServer
	c *Coordinator
}

// Session accepts one worker: the Hello names the group it claims (unknown
// names and second connects are rejected; epoch validation arrives with
// supervision). The server then streams assignments and collects acks until
// the worker goes.
func (s *controlServer) Session(stream pb.UrutauControl_SessionServer) (retErr error) {
	c := s.c
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := msg.GetHello()
	if hello == nil {
		return errors.New("coordinator: first worker message must be Hello")
	}
	// An ephemeral maintenance worker is a worker, but has no data plane:
	// route it to the maintenance scheduler instead of the assignment/ack
	// path below.
	if hello.Maintenance {
		return c.maintenanceSession(stream, hello)
	}

	c.mu.Lock()
	w, known := c.workers[hello.WorkerName]
	if known && w.attached {
		c.mu.Unlock()
		return fmt.Errorf("coordinator: worker %q already connected", hello.WorkerName)
	}
	// The supervisor cancels this ctx to force a reset; the worker sees the
	// stream die and suicides.
	sessCtx, sessCancel := context.WithCancel(stream.Context())
	sess := &workerSession{
		out:  make(chan *pb.CoordinatorMessage, 16),
		done: make(chan error, 1),
	}
	// Publish the session BEFORE signaling ready: the ready send
	// happens-before run's receive, so run may use w.out the moment it
	// wakes — attaching after the signal is a data race.
	if known {
		w.out, w.attached, w.hadSession = sess.out, true, true
		w.cancel = sessCancel
	}
	c.mu.Unlock()
	if known {
		// noteAttach takes the supervisor lock; calling it under c.mu would
		// invert the order supervisor.tick uses (supervisor.mu → c.mu) and
		// deadlock the two (audit #3).
		c.supervisor.noteAttach(hello.WorkerName)
		c.pushDashState() // the worker attached
	}
	if !known {
		sessCancel()
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}

	defer func() {
		c.mu.Lock()
		w.attached, w.out, w.cancel = false, nil, nil
		c.mu.Unlock()
		sessCancel()
		c.signalSessionEnd(hello.WorkerName, retErr)
	}()

	select {
	case c.ready <- struct{}{}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	c.log.Info("worker session", "worker", hello.WorkerName)

	// Assignment on every attach (including after a reset): the worker
	// waits for it before opening Flight, and a resurrected worker needs a
	// fresh one.
	if msg, err := c.assignmentFor(w); err != nil {
		return err
	} else {
		select {
		case sess.out <- msg:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}

	// Recv loop: acks and worker errors.
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				sess.done <- err
				return
			}
			switch m := msg.Msg.(type) {
			case *pb.WorkerMessage_Ack:
				c.onAck(hello.WorkerName, m.Ack)
			case *pb.WorkerMessage_Staged:
				c.onStagedBatch(hello.WorkerName, m.Staged)
			case *pb.WorkerMessage_Hello:
				c.onHello(hello.WorkerName, m.Hello)
			case *pb.WorkerMessage_WorkerMetrics:
				c.onWorkerMetrics(m.WorkerMetrics)
			case *pb.WorkerMessage_ChunkReady:
				// A full chunkReady buffer with no draining snapshot loop
				// (stale replies after a reset) must not wedge this recv
				// goroutine; it aborts on session cancellation instead.
				select {
				case c.chunkReady <- m.ChunkReady:
				case <-sessCtx.Done():
					sess.done <- context.Canceled
					return
				}
			case *pb.WorkerMessage_Error:
				sess.done <- errors.New("coordinator: worker error: " + m.Error.Detail)
				return
			case *pb.WorkerMessage_SchemaDrift:
				c.onSchemaDrift(hello.WorkerName, m.SchemaDrift)
			}
		}
	}()

	// Send loop: assignments and future control messages.
	for {
		select {
		case m := <-sess.out:
			if err := stream.Send(m); err != nil {
				return err
			}
		case err := <-sess.done:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-sessCtx.Done():
			return errSessionReset
		}
	}
}

// errSessionReset marks a session cancelled by the supervisor; not a worker
// failure, so it must not kill the run via sessionErrs.
var errSessionReset = errors.New("session reset")

// signalSessionEnd reports a session's terminal state to the run loop.
//
// A supervisor reset is not a worker failure, and neither is the death of a
// worker mid-reset (it suicides on channel loss); the supervisor owns the
// outcome. The one exception is a snapshot in progress: the worker's
// in-memory window died with the session, so the protocol cannot continue —
// either it waits out AckTimeout/MaxResets, or a stale ChunkReady from the
// old generation satisfies waitChunkReady against an empty window (a
// silently incomplete snapshot). Fail the run instead, so it restarts and
// re-snapshots cleanly (CD-5).
func (c *Coordinator) signalSessionEnd(worker string, retErr error) {
	// A supervisor reset (pending) reconnects and redelivers its batches, so
	// its open staged cycles must NOT be discarded: the reconnecting session's
	// nonzero sequences are unknown to stagedCycles.deliver and would be
	// dropped, losing the staged rows. Only a genuinely lost worker's cycles
	// are discarded below.
	pending := c.supervisor.isPending(worker)
	discarded := 0
	if !pending {
		// A lost worker leaves the staged cycles it owed permanently
		// incomplete: discard them (never commit a partial cycle). The run
		// terminates below and replays every partition from the committed
		// position, so no later cycle may be committed over the gap.
		var open []string
		for _, ref := range c.workerRefs(worker) {
			open = append(open, c.staged.debugOpen(ref.Target)...)
		}
		discarded = c.staged.discardWorker(worker)
		if discarded > 0 {
			c.log.Warn("coordinator: discarded staged cycles of lost worker", "worker", worker, "cycles", discarded, "open-before", open)
		}
	}
	c.pushDashState() // the worker is no longer attached
	// A worker whose open staged cycles were just discarded (discarded > 0)
	// lost rows — they were delivered to a cycle that can never complete — and
	// discardWorker leaves a hole in the table's send order that drainLocked
	// cannot pass, so a re-slice's drain waits until it times out (issue #372).
	// A session that ends with context.Canceled — the Pod was deleted, or the
	// client's stream closed — while the worker still owes batches strands them
	// the same way: the pump keeps routing to the detached owner, no session
	// will ever deliver them, and the flip never commits (issue #363). Both
	// strand work that only a clean replay recovers, so fail the run for either
	// — a discarded cycle regardless of the reset error type. A supervisor
	// reset (pending) is excluded: that worker reconnects and redelivers.
	if !pending && (discarded > 0 || (errors.Is(retErr, context.Canceled) && c.workerOwes(worker))) {
		c.sessionErrs <- fmt.Errorf("coordinator: worker %s session lost owing work: %w", worker, retErr)
		return
	}
	// A session cancelled by the coordinator — a supervisor reset or a
	// re-slice retiring an owner — surfaces as errSessionReset OR, when the
	// recv goroutine wins the race, as context.Canceled. Neither is a worker
	// failure; only a real stream error is. Treating the cancel as a failure
	// ended the whole run the moment a scale-in retired an owner.
	if !errors.Is(retErr, errSessionReset) && !errors.Is(retErr, context.Canceled) && !pending {
		c.sessionErrs <- retErr
	} else if c.snapshotActive.Load() {
		c.sessionErrs <- fmt.Errorf("coordinator: worker %s session lost during snapshot: %w", worker, retErr)
	}
}

// workerOwes reports whether a worker still holds undelivered queued batches
// or delivered-but-unacked in-flight batches — work a re-slice's drain would
// wait on, and that no detached session can drain.
func (c *Coordinator) workerOwes(worker string) bool {
	c.mu.Lock()
	w := c.workers[worker]
	c.mu.Unlock()
	if w == nil {
		return false
	}
	return len(w.queue) > 0 || c.inFlight(worker) > 0
}

// ownerDetached reports whether an owner's Pod died — its session attached and
// then detached — but no re-slice has retired it yet. Such an owner can never
// drain a batch routed to it (issue #372).
func (c *Coordinator) ownerDetached(w *workerState) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return w.hadSession && !w.attached
}

// workerByTicket looks up a worker by its Flight ticket under c.mu: a
// re-slice's registerOwner writes byTicket while a worker opens or reopens
// its DoGet, and an unlocked read races that write (issue #372).
func (c *Coordinator) workerByTicket(ticket string) (*workerState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.byTicket[ticket]
	return w, ok
}

// workerRefs returns the table refs a worker owns, or nil if it is unknown.
func (c *Coordinator) workerRefs(worker string) []source.TableRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w := c.workers[worker]; w != nil {
		return w.refs
	}
	return nil
}

// workerSession is one connected worker's session-local surface; the group's
// durable state lives in workerState.
type workerSession struct {
	out  chan *pb.CoordinatorMessage
	done chan error
}

// Control is the urgent-signal plane on the same ClientConn as Session and
// DoGet. The first frame must be a Hello naming the worker so urgent
// signals route to the right stream. No data rides here.
func (s *controlServer) Control(stream pb.UrutauControl_ControlServer) (retErr error) {
	c := s.c
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := msg.GetHello()
	if hello == nil {
		return errors.New("coordinator: Control first message must be Hello")
	}
	c.mu.Lock()
	w, known := c.workers[hello.WorkerName]
	if known {
		w.control = stream
	}
	c.mu.Unlock()
	if !known {
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}
	defer func() {
		c.mu.Lock()
		w.control = nil
		c.mu.Unlock()
		// A worker mid-reset suicides and closes this stream too; only a
		// non-reset death is a real session failure (signalSessionEnd owns
		// the reset/snapshot rule).
		c.signalSessionEnd(hello.WorkerName, retErr)
	}()
	// The worker never writes again; its death is the stream ending.
	<-stream.Context().Done()
	return stream.Context().Err()
}

// gracefulShutdown tells every connected worker to drain: flush + commit +
// ack what is in flight, then exit 0 (design §5.3.2). Called on shutdown
// and before terminal exits.
func (c *Coordinator) gracefulShutdown() {
	for _, w := range c.workers {
		c.mu.Lock()
		ctrl := w.control
		c.mu.Unlock()
		if ctrl == nil {
			continue
		}
		msg := &pb.ControlMessage{Msg: &pb.ControlMessage_Shutdown{
			Shutdown: &pb.Shutdown{
				Grace: durationpb.New(30 * time.Second),
				Drain: true,
			},
		}}
		if err := ctrl.Send(msg); err != nil {
			c.log.Warn("coordinator: shutdown send", "worker", w.name, "err", err)
		}
	}
}
