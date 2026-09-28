package coordinator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/eventlog"
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
// The run still ends on what does not heal by itself: an error the worker
// reports (schema drift, a failed commit), the same worker lost
// maxLossesWithoutProgress times in a row with no committed progress in
// between (a batch that OOM-kills it every time), or a worker gone for longer
// than the absence timeout (supervisor.tick).

// defaultMaxLossesWithoutProgress is how many times in a row one worker may
// be lost with no committed progress before the run ends.
const defaultMaxLossesWithoutProgress = 3

// workerReportedError is an error the worker sent itself
// (WorkerMessage_Error). It ends the run: it is not a hiccup.
type workerReportedError struct{ detail string }

func (e *workerReportedError) Error() string { return "coordinator: worker error: " + e.detail }

// lossRecord is a worker's run of consecutive losses without progress.
type lossRecord struct {
	count int
	at    string // the worker's committed position at those losses
	epoch uint64 // the epoch of the last loss counted
}

// maxLossesWithoutProgress is the configured limit, or its default.
func (c *Coordinator) maxLossesWithoutProgress() int {
	if c.cfg.MaxLossesWithoutProgress > 0 {
		return c.cfg.MaxLossesWithoutProgress
	}
	return defaultMaxLossesWithoutProgress
}

// loseWorker handles a worker whose session ended. A worker no longer
// registered was retired by a scale-in, whose cancel ended the session:
// nothing to do. Any other worker is awaited like a supervisor reset: a new
// epoch (a reply from the lost session is stale), its queue, sent batches and
// staged cycles kept, and its snapshot loop told. It returns an error when the
// loss ends the run.
func (c *Coordinator) loseWorker(worker string, cause error) error {
	c.mu.Lock()
	w, ok := c.workers[worker]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	pending := c.supervisor.isPending(worker)
	if !pending {
		// A supervisor reset bumped the epoch already.
		w.epoch++
	}
	epoch := w.epoch
	lost := w.lostCh
	w.lostCh = nil
	c.mu.Unlock()
	if lost != nil {
		close(lost)
	}
	c.supervisor.pendingSet(worker)

	at := c.confirmedFor(worker)
	n, counted := c.supervisor.recordLoss(worker, at, epoch)
	if !counted {
		// The same loss seen again on the worker's other stream (Session
		// and Control both end when it is lost).
		return nil
	}
	c.log.Warn("coordinator: worker lost; awaiting its reconnect", "worker", worker,
		"epoch", epoch, "losses_without_progress", n, "committed", at, "cause", cause)
	c.emitLog(eventlog.KindWorkerReset, map[string]any{
		"worker": worker, "epoch": epoch, "reason": "session_lost", "losses_without_progress": n,
	})
	if n >= c.maxLossesWithoutProgress() {
		return fmt.Errorf("coordinator: worker %s lost %d times in a row without progress (committed position %q)%s: %w",
			worker, n, at, c.workerTermination(worker), errCrashLoop)
	}
	return nil
}

// errCrashLoop marks a run ended by a worker lost without progress.
var errCrashLoop = errors.New("crash loop")

// confirmedFor returns worker's latest durably committed position, or "".
func (c *Coordinator) confirmedFor(worker string) string {
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	if p := c.confirmed[worker]; p != nil {
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

// recordLoss counts worker's consecutive losses at the same committed
// position: progress since the previous loss starts the count over. A loss is
// counted once per epoch: both of the worker's streams end when it is lost,
// and the second report returns counted false.
func (s *supervisor) recordLoss(worker, at string, epoch uint64) (n int, counted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.losses[worker]
	if r.count > 0 && r.epoch == epoch {
		return r.count, false
	}
	if r.count == 0 || r.at != at {
		r = lossRecord{at: at}
	}
	r.count++
	r.epoch = epoch
	s.losses[worker] = r
	return r.count, true
}

// workerTermination describes, for an error message, why the worker's Pod
// last stopped or is not running: its last termination reason and exit
// code, a waiting reason, or an unschedulable condition. Best effort: empty
// outside Kubernetes or when the Pod cannot be read.
func (c *Coordinator) workerTermination(worker string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, ns, _, err := c.workerClientset(ctx)
	if err != nil || cs == nil {
		return ""
	}
	pod, err := cs.CoreV1().Pods(ns).Get(ctx, worker, metav1.GetOptions{})
	if err != nil {
		return ""
	}
	var parts []string
	for _, st := range pod.Status.ContainerStatuses {
		if t := st.LastTerminationState.Terminated; t != nil {
			parts = append(parts, fmt.Sprintf("last terminated %s, exit code %d", t.Reason, t.ExitCode))
		}
		if w := st.State.Waiting; w != nil && w.Reason != "" {
			parts = append(parts, "waiting: "+w.Reason)
		}
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status != corev1.ConditionTrue && cond.Message != "" {
			parts = append(parts, "not scheduled: "+cond.Message)
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("; pod %s is %s", worker, pod.Status.Phase)
	}
	return fmt.Sprintf("; pod %s: %s", worker, strings.Join(parts, "; "))
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

// reattachPoll is how often the snapshot re-checks a lost worker's return.
const reattachPoll = 50 * time.Millisecond

// awaitReattached returns once w is attached and no longer awaited after a
// loss or reset. A worker that never comes back ends the run at the absence
// timeout (supervisor.tick), which cancels ctx.
func (c *Coordinator) awaitReattached(ctx context.Context, w *workerState) error {
	for {
		c.mu.Lock()
		attached := w.attached
		c.mu.Unlock()
		if attached && !c.supervisor.isPending(w.name) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reattachPoll):
		}
	}
}

// redoFrom is where a partition's snapshot resumes after its worker was lost
// at chunk next: the first chunk of this partition whose Closes marker the
// worker had not committed, whose rows died with its window.
func (c *Coordinator) redoFrom(w *workerState, target string, windows map[uint32]int, next int) int {
	from := next
	for _, m := range c.heldChunkMarkers(w.name) {
		if m.target != target {
			continue
		}
		if i, ok := windows[m.window]; ok && i < from {
			from = i
		}
	}
	return from
}

// clearChunkReady forgets that a partition's current chunk is in its
// worker's window: that window died, and the gate must hold the live batches
// again until the redone chunk's ChunkReady.
func (c *Coordinator) clearChunkReady(target string, partition int) {
	c.gateMu.Lock()
	delete(c.gateReady, gateKey(target, partition))
	c.gateMu.Unlock()
}
