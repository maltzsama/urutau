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
// The run still ends on what does not heal by itself:
//   - an error the worker reports itself (schema drift, a failed commit);
//   - a crash loop: the same worker crashing maxConsecutiveCrashes times in a
//     row without delivering what it owed when it came back (a batch that
//     OOM-kills it every time). Whether a loss was a crash is read from its
//     Pod when it comes back: a lost network or a Pod replaced from outside
//     is not one (workerBack);
//   - a worker owing work that delivers none of it for the delivery timeout,
//     connected or not (supervisor.tick).

// defaultMaxConsecutiveCrashes is how many times in a row one worker may
// crash without delivering what it owed before the run ends.
const defaultMaxConsecutiveCrashes = 3

// workerReportedError is an error the worker sent itself
// (WorkerMessage_Error). It ends the run: it is not a hiccup.
type workerReportedError struct{ detail string }

func (e *workerReportedError) Error() string { return "coordinator: worker error: " + e.detail }

// maxConsecutiveCrashes is the configured limit, or its default.
func (c *Coordinator) maxConsecutiveCrashes() int {
	if c.cfg.MaxConsecutiveCrashes > 0 {
		return c.cfg.MaxConsecutiveCrashes
	}
	return defaultMaxConsecutiveCrashes
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
	pending := c.supervisor.isPending(worker)
	if !pending {
		// A supervisor reset bumped the epoch already.
		w.epoch++
	}
	epoch := w.epoch
	lost := w.lostCh
	w.lostCh = nil
	c.mu.Unlock()
	// The chunk windows the worker had not committed died with it. Taken
	// now, before it can reconnect and ack their redelivered Closes markers
	// as empty windows, which would hide them from the snapshot's redo.
	c.noteLostWindows(worker)
	if lost != nil {
		close(lost)
	}
	c.supervisor.pendingSet(worker)
	if !c.supervisor.noteLoss(worker, epoch) {
		// The same loss seen again on the worker's other stream (Session
		// and Control both end when it is lost).
		return nil
	}
	c.log.Warn("coordinator: worker lost; awaiting its reconnect", "worker", worker,
		"epoch", epoch, "committed", c.confirmedFor(worker), "cause", cause)
	c.emitLog(eventlog.KindWorkerReset, map[string]any{"worker": worker, "epoch": epoch, "reason": "session_lost"})
	return nil
}

// onAttach records a worker's session attaching, outside c.mu: noteAttach
// takes the supervisor lock, and calling it under c.mu would invert the order
// supervisor.tick uses (supervisor.mu → c.mu) and deadlock the two (audit #3).
// A worker back from a crash loop ends the run here, with its Pod's reason.
func (c *Coordinator) onAttach(worker string) error {
	c.supervisor.noteAttach(worker)
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
	if idx := c.indexOf(worker); idx != nil {
		mark = idx.maxHeldID()
	}
	n, crashed, desc := c.supervisor.noteBack(worker, term, ok, mark, time.Now())
	if !crashed {
		return nil
	}
	c.log.Warn("coordinator: worker crashed", "worker", worker, "crashes_in_a_row", n, "termination", desc)
	if n >= c.maxConsecutiveCrashes() {
		return fmt.Errorf("coordinator: worker %s crashed %d times in a row without delivering what it owed (committed position %q); pod %s: %s: %w",
			worker, n, c.confirmedFor(worker), worker, desc, errCrashLoop)
	}
	return nil
}

// terminationFor reads worker's last container termination.
func (c *Coordinator) terminationFor(worker string) (podTermination, bool) {
	if c.podTermination != nil {
		return c.podTermination(worker)
	}
	return c.k8sTermination(worker)
}

// podTermination is a worker container's last termination, as its Pod status
// reports it, with the Pod's identity and restart count to tell a fresh
// termination from one already seen.
type podTermination struct {
	podUID   string
	restarts int32
	reason   string
	exitCode int32
	message  string
}

// workerContainerName is the worker container's name in the Pod template the
// operator renders (internal/operator). The kubelet sorts containerStatuses
// by container name, so an injected sidecar (istio-proxy, linkerd-proxy) can
// precede the worker; crash detection must select the worker's status by name,
// never by index (issue #553).
const workerContainerName = "worker"

// networkExitPrefix starts the termination message of a worker that exited
// because it lost the coordinator (cmd/worker writes it): not a crash.
const networkExitPrefix = "network: "

// crash reports whether the termination was the worker failing on its own,
// and describes it.
func (t podTermination) crash() (bool, string) {
	desc := fmt.Sprintf("last terminated %s, exit code %d", t.reason, t.exitCode)
	if msg := strings.TrimSpace(t.message); msg != "" {
		desc += ": " + strings.SplitN(msg, "\n", 2)[0]
	}
	switch {
	case t.reason == "OOMKilled":
		return true, desc
	case strings.HasPrefix(t.message, networkExitPrefix):
		return false, desc
	case t.exitCode == 0:
		return false, desc
	default:
		return true, desc
	}
}

// k8sTermination reads worker's Pod status from Kubernetes. Outside it there
// is no Pod to read, and no loss is taken for a crash.
func (c *Coordinator) k8sTermination(worker string) (podTermination, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, ns, _, err := c.workerClientset(ctx)
	if err != nil || cs == nil {
		return podTermination{}, false
	}
	pod, err := cs.CoreV1().Pods(ns).Get(ctx, worker, metav1.GetOptions{})
	if err != nil {
		return podTermination{}, false
	}
	var st *corev1.ContainerStatus
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == workerContainerName {
			st = &pod.Status.ContainerStatuses[i]
			break
		}
	}
	if st == nil {
		return podTermination{}, false
	}
	t := podTermination{podUID: string(pod.UID), restarts: st.RestartCount}
	if last := st.LastTerminationState.Terminated; last != nil {
		t.reason, t.exitCode, t.message = last.Reason, last.ExitCode, last.Message
	}
	return t, true
}

// errCrashLoop marks a run ended by a worker crashing again and again.
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

// workerHealth is what the supervisor tracks of a worker's losses.
type workerHealth struct {
	lossEpoch    uint64 // the epoch of the last loss seen
	lossPending  bool   // a loss not yet classified by the worker's return
	crashes      int    // crashes in a row without delivering what it owed
	seenUID      string // the Pod and restart count last read
	seenRestarts int32
	owedMark     uint64    // the highest batch id owed when it came back
	backAt       time.Time // when it came back
	lostAt       time.Time // when the last loss was seen
}

// healthOf returns worker's record. Caller holds s.mu.
func (s *supervisor) healthOf(worker string) *workerHealth {
	h := s.health[worker]
	if h == nil {
		h = &workerHealth{}
		s.health[worker] = h
	}
	return h
}

// lossTime is when worker's last loss was seen, or now if none was.
func (s *supervisor) lossTime(worker string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if h := s.health[worker]; h != nil && !h.lostAt.IsZero() {
		return h.lostAt
	}
	return time.Now()
}

// noteLoss records a loss at epoch, reporting false for a loss already seen
// (the worker's other stream).
func (s *supervisor) noteLoss(worker string, epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.healthOf(worker)
	if h.lossPending && h.lossEpoch == epoch {
		return false
	}
	h.lossEpoch, h.lossPending = epoch, true
	h.lostAt = time.Now()
	return true
}

// noteBack classifies the loss a worker comes back from (see workerBack) and
// returns its crashes in a row, whether this loss was one, and its
// description.
func (s *supervisor) noteBack(worker string, term podTermination, known bool, owedMark uint64, now time.Time) (int, bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.healthOf(worker)
	h.owedMark, h.backAt = owedMark, now
	crashed, desc := false, ""
	if known {
		fresh := (term.podUID == h.seenUID && term.restarts > h.seenRestarts) ||
			(h.seenUID == "" && term.restarts > 0)
		if h.lossPending && fresh {
			crashed, desc = term.crash()
		}
		h.seenUID, h.seenRestarts = term.podUID, term.restarts
	}
	h.lossPending = false
	if crashed {
		h.crashes++
	}
	return h.crashes, crashed, desc
}

// noteDelivering starts a worker's crash count over once it has delivered
// every batch it owed when it came back and stayed up for ack: it is not
// crash-looping. Caller holds s.mu.
func (s *supervisor) noteDeliveringLocked(worker string, minHeld uint64, now time.Time, ack time.Duration) {
	h := s.health[worker]
	if h == nil || h.crashes == 0 || h.lossPending {
		return
	}
	if (minHeld == 0 || minHeld > h.owedMark) && now.Sub(h.backAt) >= ack {
		h.crashes = 0
	}
}

// noteDeliveredAt records that worker delivered (acked, or owed nothing) at.
func (s *supervisor) noteDeliveredAt(worker string, at time.Time) {
	s.mu.Lock()
	s.lastDelivered[worker] = at
	s.mu.Unlock()
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
// loss or reset. The supervisor starts after the snapshot, so its delivery
// timeout does not cover this wait: a worker that does not come back within
// --worker-delivery-timeout of the loss ends the run here.
func (c *Coordinator) awaitReattached(ctx context.Context, w *workerState) error {
	timeout := c.cfg.WorkerDeliveryTimeout
	if timeout <= 0 {
		timeout = defaultWorkerDeliveryTimeout
	}
	deadline := c.supervisor.lossTime(w.name).Add(timeout)
	for {
		c.mu.Lock()
		attached := w.attached
		c.mu.Unlock()
		if attached && !c.supervisor.isPending(w.name) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("coordinator: worker %s did not come back within %s of its loss mid-snapshot (--worker-delivery-timeout)%s",
				w.name, timeout, c.workerTermination(w.name))
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
// worker had not committed, whose rows died with its window. It also returns
// that chunk's resume cursor — the high key of the last window whose Closes
// marker the worker DID commit — so the redo resumes the chunk from there
// instead of re-emitting committed windows (issue #646). A chunk with no
// committed window returns nil, and is re-read from its start.
func (c *Coordinator) redoFrom(w *workerState, target string, windows map[uint64]int, partition int, next int) (int, []any) {
	from := next
	for _, m := range c.takeLostWindows(w.name) {
		if m.target != target {
			continue
		}
		if i, ok := windows[m.window]; ok && i < from {
			from = i
		}
	}
	return from, c.takeCursor(chunkRef(partition, from))
}

// noteLostWindows records the chunk windows a lost worker had not committed.
func (c *Coordinator) noteLostWindows(worker string) {
	held := c.heldChunkMarkers(worker)
	if len(held) == 0 {
		return
	}
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	if c.lostWindows == nil {
		c.lostWindows = map[string][]chunkMarker{}
	}
	c.lostWindows[worker] = append(c.lostWindows[worker], held...)
}

// takeLostWindows returns and forgets the chunk windows worker lost.
func (c *Coordinator) takeLostWindows(worker string) []chunkMarker {
	c.chunkMarkersMu.Lock()
	defer c.chunkMarkersMu.Unlock()
	out := c.lostWindows[worker]
	delete(c.lostWindows, worker)
	return out
}

// clearChunkReady forgets that a partition's current chunk is in its
// worker's window: that window died, and the gate must hold the live batches
// again until the redone chunk's ChunkReady.
func (c *Coordinator) clearChunkReady(target string, partition int) {
	c.gateMu.Lock()
	delete(c.gateReady, gateKey(target, partition))
	c.gateMu.Unlock()
}
