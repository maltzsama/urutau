package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"

	"github.com/maltzsama/urutau/source"
	"google.golang.org/protobuf/types/known/durationpb"
)

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
	c.mu.Unlock()
	if !known {
		return fmt.Errorf("coordinator: unknown worker %q", hello.WorkerName)
	}
	// Assignment on every attach (including after a reset): the worker
	// waits for it before opening Flight, and a resurrected worker needs a
	// fresh one. It is queued before the session is published: a snapshot
	// waiting for this worker sends chunk requests the moment it is
	// attached, and a worker whose first message is not its Assignment fails
	// the handshake (chaos-1M-521691a).
	assign, err := c.assignmentFor(w)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if w.attached {
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
	sess.out <- assign
	// Publish the session BEFORE signaling ready: the ready send
	// happens-before run's receive, so run may use w.out the moment it
	// wakes — attaching after the signal is a data race.
	w.out, w.attached, w.hadSession = sess.out, true, true
	w.cancel = sessCancel
	c.mu.Unlock()
	if err := c.onAttach(hello.WorkerName); err != nil {
		sessCancel()
		return err
	}
	defer func() {
		c.mu.Lock()
		w.attached, w.out, w.cancel = false, nil, nil
		c.mu.Unlock()
		sessCancel()
		c.signalSessionEnd(hello.WorkerName, retErr)
	}()

	c.signalReady(sessCtx, stream.Context())
	c.log.Info("worker session", "worker", hello.WorkerName)

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
				c.onWorkerMetrics(hello.WorkerName, m.WorkerMetrics)
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
			case *pb.WorkerMessage_WindowOpen:
				select {
				case c.windowOpen <- m.WindowOpen:
				case <-sessCtx.Done():
					sess.done <- context.Canceled
					return
				}
			case *pb.WorkerMessage_Error:
				sess.done <- &workerReportedError{detail: m.Error.Detail}
				return
			case *pb.WorkerMessage_SchemaDrift:
				c.onSchemaDrift(hello.WorkerName, m.SchemaDrift)
			case *pb.WorkerMessage_Log:
				c.onWorkerLog(hello.WorkerName, m.Log)
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

// signalSessionEnd reports a session's terminal state to the run loop.
//
// A lost worker is recovered, not a reason to end the run (issue #461): its
// Pod comes back under the same name, reconnects, and is redelivered what it
// owed. The session's end only ends the run when the worker reported an
// error itself (schema drift, a failed commit), or when the same worker is
// lost maxConsecutiveCrashes times in a row with no committed progress in
// between (a crash loop). See recovery.go.
func (c *Coordinator) signalSessionEnd(worker string, retErr error) {
	c.pushDashState() // the worker is no longer attached
	var reported *workerReportedError
	if errors.As(retErr, &reported) {
		c.sessionErrs <- retErr
		return
	}
	if err := c.loseWorker(worker, retErr); err != nil {
		c.sessionErrs <- err
	}
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
		// Only act on the stream this handler installed: a worker that
		// reconnected before the server noticed this stream died has already
		// replaced w.control with a new stream, and niling it would make
		// gracefulShutdown skip the worker (issue #552).
		if w.control == stream {
			w.control = nil
			// The Control stream is gone while this handler still owned it:
			// end the Session too, so the worker reconnects and re-establishes
			// both streams. A Session left alive with no Control never gets a
			// shutdown drain and never reattaches (issue #552).
			if w.cancel != nil {
				w.cancel()
			}
		}
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
	for _, w := range c.workersSnapshot() {
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

type controlServer struct {
	pb.UnimplementedUrutauControlServer
	c *Coordinator
}

// errSessionReset marks a session cancelled by the supervisor; not a worker
// failure, so it must not kill the run via sessionErrs.
var errSessionReset = errors.New("session reset")

// workerSession is one connected worker's session-local surface; the group's
// durable state lives in workerState.
type workerSession struct {
	out  chan *pb.CoordinatorMessage
	done chan error
}
