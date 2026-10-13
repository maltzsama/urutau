package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	sup "github.com/maltzsama/urutau/internal/supervisor"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/eventlog"
	"k8s.io/client-go/kubernetes"

	"github.com/maltzsama/urutau/internal/coordk8s"
)

// A worker lost to a hiccup — its Pod killed or OOM-killed, its stream cut —
// is recovered, not a reason to end the run (issue #461). The StatefulSet
// brings the Pod back under the same name; it reconnects, gets a fresh
// Assignment under a new epoch, and the coordinator redelivers what it owed:
// its sent-but-unacked batches, then its queue (DoGet, issue #235). The
// worker skips any batch its committed position already covers, and its open
// staged cycles stay open for the redelivered stages. A snapshot in progress
// on the worker redoes the chunks its lost window held (snapshotPartition).
//
// Ending the run on every loss — the earlier "clean replay" — put the
// coordinator into CrashLoopBackOff under sustained worker kills: every
// restart snapshotted again from chunk 0, and no table advanced.
//
// The run still ends on what does not heal by itself:
//   - an error the worker reports itself (schema drift, a failed commit);
//   - a crash loop: the same worker crashing maxConsecutiveCrashes times in a
//     row without delivering what it owed when it came back (a batch that
//     OOM-kills it every time). Whether a loss was a crash is read from its
//     Pod when it comes back: a lost network or a Pod replaced from outside
//     is not one (workerBack);
//   - a worker owing work that delivers none of it for the delivery timeout,
//     connected or not (supervisor.tick).

// workerReportedError is an error the worker sent itself
// (WorkerMessage_Error). It ends the run: it is not a hiccup.
type workerReportedError struct{ detail string }

func (e *workerReportedError) Error() string { return "coordinator: worker error: " + e.detail }

// maxConsecutiveCrashes is the configured crash limit, or its default.
func (c *Coordinator) maxConsecutiveCrashes() int {
	return sup.MaxCrashes(c.cfg.MaxConsecutiveCrashes)
}

// loseWorker handles a worker whose session ended. A worker no longer
// registered was retired by a scale-in, whose cancel ended the session:
// nothing to do. Any other worker is awaited like a supervisor reset: a new
// epoch (a reply from the lost session is stale), its queue, sent batches and
// staged cycles kept, and its snapshot loop told. Whether the loss was a
// crash is only known once the worker is back (workerBack), from its Pod.
func (c *Coordinator) loseWorker(worker string, cause error) error {
	c.mu.Lock()
	w, ok := c.workers[worker]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	// The loss is marked pending in the same step that decides whether to
	// bump the epoch, still under c.mu: both of the worker's streams report
	// the loss, each from its own goroutine, and a check here with the mark
	// taken later let both see "not pending" and bump the epoch twice. An
	// already-pending worker was reset by the supervisor, which bumped it.
	if !c.supervisor.MarkPending(worker) {
		w.epoch++
	}
	epoch := w.epoch
	lost := w.lostCh
	w.lostCh = nil
	c.mu.Unlock()
	// The chunk windows the worker had not committed died with it. Taken
	// now, before it can reconnect and ack their redelivered Closes markers
	// as empty windows, which would hide them from the snapshot's redo.
	c.markers.Lose(worker, c.heldChunkMarkers(worker))
	// The loss is recorded before its waiters wake: a snapshot loop woken by
	// the close goes on to awaitReattached, which reads the pending mark.
	if lost != nil {
		close(lost)
	}
	if !c.supervisor.Health.Loss(worker, epoch, time.Now()) {
		// The same loss seen again on the worker's other stream (Session
		// and Control both end when it is lost).
		return nil
	}
	c.log.Warn("coordinator: worker lost; awaiting its reconnect", "worker", worker,
		"epoch", epoch, "committed", c.confirmedFor(worker), "cause", cause)
	c.emitLog(eventlog.KindWorkerReset, map[string]any{"worker": worker, "epoch": epoch, "reason": "session_lost"})
	return nil
}

// onAttach records a worker's session attaching. The caller must NOT hold
// c.mu: workerBack below takes it, and NoteAttach takes the supervisor lock.
// The supervisor's lock order is c.mu then supervisor.mu (supervisor.tick);
// acquiring supervisor.mu first would invert it and deadlock the two (audit
// #3). A worker back from a crash loop
// ends the run here, with its Pod's reason.
func (c *Coordinator) onAttach(worker string) error {
	c.supervisor.NoteAttach(worker, time.Now())
	c.pushDashState() // the worker attached
	if err := c.workerBack(worker); err != nil {
		c.fail(err)
		return err
	}
	return nil
}

// workerBack classifies the loss a reconnected worker comes back from, from
// its Pod's last container termination: an OOM kill, a panic or an error is
// a crash; a Pod replaced from outside (a pod-kill, a node drain) or a worker
// that exited because it lost the coordinator ("network: ", written to its
// termination message) is not. A crash counts toward a crash loop unless the
// worker delivered everything it owed when it came back before crashing
// again (supervisor.tick starts the count over). The limit's crash ends the
// run, with the Pod's reason.
func (c *Coordinator) workerBack(worker string) error {
	term, ok := c.terminationFor(worker)
	var mark uint64
	if idx := c.index.Of(worker); idx != nil {
		mark = idx.MaxHeldID()
	}
	back := c.supervisor.Back(worker, term, ok, mark, time.Now(), c.maxConsecutiveCrashes())
	if !back.Crashed {
		return nil
	}
	c.log.Warn("coordinator: worker crashed", "worker", worker, "crashes_in_a_row", back.Crashes, "termination", back.Desc)
	if back.Loop {
		return fmt.Errorf("coordinator: worker %s crashed %d times in a row without delivering what it owed (committed position %q); pod %s: %s: %w",
			worker, back.Crashes, c.confirmedFor(worker), worker, back.Desc, errCrashLoop)
	}
	return nil
}

// terminationFor reads worker's last container termination.
func (c *Coordinator) terminationFor(worker string) (coordk8s.Termination, bool) {
	if c.podTermination != nil {
		return c.podTermination(worker)
	}
	return c.k8sTermination(worker)
}

// k8sTermination reads worker's Pod status from Kubernetes. Outside it there
// is no Pod to read, and no loss is taken for a crash.
func (c *Coordinator) k8sTermination(worker string) (coordk8s.Termination, bool) {
	cs, ns := c.podClient()
	return coordk8s.ReadTermination(cs, ns, worker)
}

// podClient returns the coordinator's Kubernetes client and namespace, or a
// nil client outside a cluster.
func (c *Coordinator) podClient() (kubernetes.Interface, string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, ns, _, err := c.workerClientset(ctx)
	if err != nil {
		return nil, ""
	}
	return cs, ns
}

// errCrashLoop marks a run ended by a worker crashing again and again.
var errCrashLoop = sup.ErrCrashLoop

// confirmedFor returns worker's latest durably committed position, or "".
func (c *Coordinator) confirmedFor(worker string) string {
	if p := c.confirmed.Of(worker); p != nil {
		return p.String()
	}
	return ""
}

// lostSignal returns a channel closed when w's current session is lost. The
// snapshot loop waits on it alongside the chunk round-trip.
func (c *Coordinator) lostSignal(w *workerState) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w.lostCh == nil {
		w.lostCh = make(chan struct{})
	}
	return w.lostCh
}

// workerTermination describes, for an error message, why the worker's Pod
// last stopped or is not running: its last termination reason and exit
// code, a waiting reason, or an unschedulable condition. Best effort: empty
// outside Kubernetes or when the Pod cannot be read.
func (c *Coordinator) workerTermination(worker string) string {
	cs, ns := c.podClient()
	return coordk8s.ReadDescription(cs, ns, worker)
}

// workerAppends reports whether any table the worker writes is append-only,
// where a redelivered batch that was already committed would be written twice.
func (c *Coordinator) workerAppends(worker string) bool {
	if c.cfg.Spec == nil {
		return false
	}
	for _, ref := range c.workerRefs(worker) {
		if tbl, ok := c.specForSource(ref.Source); ok && tbl.WriteMode.ChangeMode() == dataplane.AppendMode {
			return true
		}
	}
	return false
}

// awaitReattached returns once w is attached and no longer awaited after a
// loss or reset (sup.Watch.AwaitBack). A worker that does not come back
// within --worker-delivery-timeout of the loss ends the run here: the
// supervisor starts after the snapshot, so its own timeout does not cover
// this wait.
func (c *Coordinator) awaitReattached(ctx context.Context, w *workerState) error {
	timeout := sup.DeliveryTimeout(c.cfg.WorkerDeliveryTimeout)
	attached := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return w.attached
	}
	err := c.supervisor.AwaitBack(ctx, w.name, timeout, attached)
	if errors.Is(err, sup.ErrNotBack) {
		return fmt.Errorf("coordinator: worker %s did not come back within %s of its loss mid-snapshot (--worker-delivery-timeout)%s",
			w.name, timeout, c.workerTermination(w.name))
	}
	return err
}

// redoFrom is where a partition's snapshot resumes after its worker was lost
// at chunk next, and that chunk's resume cursor (snapshot.Markers.RedoFrom).
func (c *Coordinator) redoFrom(w *workerState, target string, windows map[uint64]int, partition int, next int) (int, []any) {
	return c.markers.RedoFrom(w.name, target, windows, partition, next)
}

// clearChunkReady forgets that a partition's current chunk is in its
// worker's window: that window died, and the gate must hold the live batches
// again until the redone chunk's ChunkReady.
func (c *Coordinator) clearChunkReady(target string, partition int) {
	c.gate.ClearReady(target, partition)
}
