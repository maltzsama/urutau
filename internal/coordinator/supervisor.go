package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
)

// SupervisorConfig tunes worker supervision (design §7/§8): a worker that
// stops acking past AckTimeout is reset (epoch++, session cancelled — the
// worker suicides rather than reconfigure); resets within ResetWindow beyond
// MaxResets are terminal.
type SupervisorConfig struct {
	AckTimeout time.Duration
	// MaxResets and ResetWindow are no longer read: a crash loop is a worker
	// lost MaxConsecutiveCrashes times in a row (recovery.go), not a
	// count of resets in a window. Kept for configuration compatibility.
	MaxResets   int
	ResetWindow time.Duration
	// DeliveryTimeout ends the run once a lost worker has not reconnected
	// for this long (default 5m).
	DeliveryTimeout time.Duration
	Poll            time.Duration
}

// supervisor watches the workers' ack health and owns the reset window.
type supervisor struct {
	c  *Coordinator
	mu sync.Mutex

	lastAck map[string]time.Time // worker → last ack
	resets  map[string][]time.Time
	pending map[string]bool // worker reset but not yet reattached
	// progress is each worker's recent network output reports: a worker
	// whose output grows is busy on slow storage, not stalled (#422).
	progress map[string][]progressSample
	// health is each worker's losses and crashes (recovery.go).
	health map[string]*workerHealth
	// lastDelivered is when each worker last delivered: an ack, or owing
	// nothing. Unlike lastAck, a reconnect does not move it.
	lastDelivered map[string]time.Time
}

func newSupervisor(c *Coordinator) *supervisor {
	return &supervisor{
		c:             c,
		lastAck:       map[string]time.Time{},
		resets:        map[string][]time.Time{},
		pending:       map[string]bool{},
		progress:      map[string][]progressSample{},
		health:        map[string]*workerHealth{},
		lastDelivered: map[string]time.Time{},
	}
}

// noteRegistered seeds a newly created worker group's ack clock. tick treats
// an attached worker with no lastAck entry as stale, and Session sets
// w.attached under c.mu BEFORE calling noteAttach, so a tick landing in that
// window would reset a worker that had just connected — a scale-out
// crashloop. Boot never hit it: every worker attaches before the supervisor
// starts ticking.
func (s *supervisor) noteRegistered(worker string, at time.Time) {
	s.mu.Lock()
	if _, ok := s.lastAck[worker]; !ok {
		s.lastAck[worker] = at
	}
	if _, ok := s.lastDelivered[worker]; !ok {
		s.lastDelivered[worker] = at
	}
	s.mu.Unlock()
}

// noteAck records a worker's ack time: it delivered.
func (s *supervisor) noteAck(worker string, at time.Time) {
	s.mu.Lock()
	s.lastAck[worker] = at
	s.lastDelivered[worker] = at
	s.mu.Unlock()
}

// noteAttach clears a worker's pending-reset state on a fresh Hello.
func (s *supervisor) noteAttach(worker string) {
	s.mu.Lock()
	delete(s.pending, worker)
	s.lastAck[worker] = time.Now()
	s.mu.Unlock()
}

// forget drops every trace of a worker the coordinator no longer tracks — a
// scale-out rolled back before commit. Its ack clock, pending-reset flag and
// reset window must not linger, or a later re-registration would inherit
// them.
func (s *supervisor) forget(worker string) {
	s.mu.Lock()
	delete(s.lastAck, worker)
	delete(s.pending, worker)
	delete(s.resets, worker)
	delete(s.progress, worker)
	delete(s.health, worker)
	delete(s.lastDelivered, worker)
	s.mu.Unlock()
}

// pendingSet marks a worker as reset-and-not-reattached.
func (s *supervisor) pendingSet(worker string) {
	s.mu.Lock()
	s.pending[worker] = true
	s.mu.Unlock()
}

// isPending reports whether a worker is reset-and-not-reattached.
func (s *supervisor) isPending(worker string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending[worker]
}

// run polls worker health until ctx is done or the job goes terminal.
func (s *supervisor) run(ctx context.Context, cfg SupervisorConfig, terminate chan<- error) {
	poll := cfg.Poll
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
			if err := s.tick(time.Now(), cfg); err != nil {
				select {
				case terminate <- err:
				case <-ctx.Done():
				}
				return
			}
		}
	}
}

// tick resets a worker that is attached, owes work and has stopped acking,
// and awaits one that is lost or reset until it reconnects (issue #461). It
// ends the run only for what does not heal by itself: a worker gone longer
// than the delivery timeout, or a stalled append worker whose unacked batches
// a redelivery would append twice. A crash loop — the same worker lost again
// and again without delivering what it owed — is ended by workerBack
// (recovery.go).
func (s *supervisor) tick(now time.Time, cfg SupervisorConfig) error {
	ack := cfg.AckTimeout
	if ack <= 0 {
		ack = 30 * time.Second
	}
	delivery := cfg.DeliveryTimeout
	if delivery <= 0 {
		delivery = defaultWorkerDeliveryTimeout
	}

	// Lock order: c.mu first, then s.mu. Session takes c.mu and calls
	// noteAttach (s.mu) afterwards, so tick must never hold s.mu while
	// taking c.mu — the two orders would deadlock (audit #3).
	s.c.mu.Lock()
	type attachProbe struct {
		name       string
		attached   bool
		hadSession bool
		owes       bool
	}
	probes := make([]attachProbe, 0, len(s.c.workers))
	for name, w := range s.c.workers {
		probes = append(probes, attachProbe{name: name, attached: w.attached, hadSession: w.hadSession, owes: len(w.queue) > 0})
	}
	s.c.mu.Unlock()
	// A worker owes work when it holds a delivered-but-unacked batch or has
	// one queued. inFlight takes the index's own lock, so it is read outside
	// c.mu.
	for i := range probes {
		probes[i].owes = probes[i].owes || s.c.inFlight(probes[i].name) > 0
	}

	type minHeld struct {
		name string
		id   uint64
	}
	held := make([]minHeld, 0, len(probes))
	for _, p := range probes {
		var id uint64
		if idx := s.c.indexOf(p.name); idx != nil {
			id = idx.minHeldID()
		}
		held = append(held, minHeld{p.name, id})
	}

	// Mid-snapshot only the delivery rule applies: a reset would drop the
	// worker's open windows, which the snapshot redoes on a loss instead.
	snapshotting := s.c.snapshotActive.Load()
	var stalled []string
	var gone []string
	s.mu.Lock()
	for i, p := range probes {
		if !p.owes {
			s.lastDelivered[p.name] = now
		}
		if p.attached && !s.pending[p.name] {
			s.noteDeliveringLocked(p.name, held[i].id, now, ack)
		}
		// A worker that owes work and has delivered none of it for the
		// delivery timeout ends the run, attached or not: a partition that
		// lets it reconnect for a moment every few seconds, or a Pod that
		// never starts, is not a worker making progress. A worker whose
		// uploads keep flowing is delivering to storage (#422).
		if at, ok := s.lastDelivered[p.name]; ok && p.owes && now.Sub(at) > delivery && !s.busyLocked(p.name, now, at, delivery) {
			gone = append(gone, p.name)
			continue
		}
		if s.pending[p.name] || (!p.attached && p.hadSession) || snapshotting {
			continue // lost or reset and awaited, or mid-snapshot
		}
		// An ATTACHED worker that owes nothing is merely idle — a quiet
		// table, or one that just went through a re-slice — and resetting it
		// is pointless (issue #312).
		at, ok := s.lastAck[p.name]
		if p.attached && p.owes && (!ok || now.Sub(at) > ack) && !s.busyLocked(p.name, now, at, ack) {
			stalled = append(stalled, p.name)
		}
	}
	s.mu.Unlock()

	for _, name := range gone {
		return fmt.Errorf("coordinator: worker %s has delivered nothing for over %s while owing work%s",
			name, delivery, s.c.workerTermination(name))
	}
	for _, name := range stalled {
		w := s.c.workerFor(name)
		if w == nil {
			continue
		}
		// A reset redelivers the unacked batches on reconnect (issue #235):
		// an upsert worker skips what it committed and re-applies the rest
		// idempotently, but on an append table a batch committed before its
		// ack was lost would be appended twice.
		if n := s.c.inFlight(name); n > 0 && s.c.workerAppends(name) {
			return fmt.Errorf("coordinator: worker %s stalled with %d in-flight batch(es) on an append table — a reset would append them twice",
				name, n)
		}
		s.c.resetWorker(w)
	}
	return nil
}

// defaultWorkerDeliveryTimeout is how long a lost worker may take to
// reconnect before the run ends.
const defaultWorkerDeliveryTimeout = 5 * time.Minute

// indexOf returns a worker's position index under indexMu, or nil.
func (c *Coordinator) indexOf(worker string) *positionIndex {
	c.indexMu.RLock()
	defer c.indexMu.RUnlock()
	return c.index[worker]
}

// setIndex installs a worker's position index under indexMu.
func (c *Coordinator) setIndex(worker string, idx *positionIndex) {
	c.indexMu.Lock()
	defer c.indexMu.Unlock()
	c.index[worker] = idx
}

// deleteIndex removes a worker's position index under indexMu.
func (c *Coordinator) deleteIndex(worker string) {
	c.indexMu.Lock()
	defer c.indexMu.Unlock()
	delete(c.index, worker)
}

// indexSnapshot copies the index map under indexMu, for a reader that iterates
// it (the checkpoint runner) while registerOwner may be writing.
func (c *Coordinator) indexSnapshot() map[string]*positionIndex {
	c.indexMu.RLock()
	defer c.indexMu.RUnlock()
	out := make(map[string]*positionIndex, len(c.index))
	for k, v := range c.index {
		out[k] = v
	}
	return out
}

// inFlight reports how many batches the worker has been delivered but has not
// acked. The map lookup is guarded by indexMu because registerOwner writes the
// map at runtime during a scale-out (issue #312); InFlight takes the index's
// own lock.
func (c *Coordinator) inFlight(worker string) int {
	if idx := c.indexOf(worker); idx != nil {
		return idx.InFlight()
	}
	return 0
}

// recordReset pushes a reset timestamp into the worker's sliding window,
// expiring entries older than window, and returns the resulting count — read
// under the same lock, so the crashloop check that follows cannot race a
// concurrent reset (issue #208).
func (s *supervisor) recordReset(worker string, now time.Time, window time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := now.Add(-window)
	kept := s.resets[worker][:0]
	for _, t := range s.resets[worker] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	s.resets[worker] = append(kept, now)
	return len(s.resets[worker])
}

// resetWorker bumps the epoch, cancels the worker's session, and waits for
// the next Hello (the worker suicides on channel loss and reconnects, or a
// new process takes over). Stale-epoch Hellos are rejected by onHello.
//
// Recovery of the resurrected worker rides on the resume path already in
// place: the coordinator resumes from the global min() and the worker skips
// every batch at or before its own committed position (failure-analysis case
// 4). The design's dedicated second-connection interval reader (§5.6.2) is
// intentionally not built here — go-mysql cannot run two replication
// connections to the same source concurrently (a second canal.Run() kills
// the first), and the correctness the interval reader exists for is already
// delivered by the skip; the interval reader would only save the other
// workers from re-skipping the window, a pure efficiency gain.
func (c *Coordinator) resetWorker(w *workerState) {
	c.mu.Lock()
	w.epoch++
	c.mu.Unlock()
	c.supervisor.pendingSet(w.name)
	c.log.Warn("coordinator: reset worker", "worker", w.name, "epoch", w.epoch)
	if c.metrics != nil {
		c.metrics.WorkerResets.WithLabelValues(w.name, "ack_timeout").Inc()
	}
	c.emitLog(eventlog.KindWorkerReset, map[string]any{
		"worker": w.name,
		"epoch":  w.epoch,
		"reason": "ack_timeout",
	})

	// Cancel the attached session: the stream dies, the worker suicides.
	c.mu.Lock()
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	c.mu.Unlock()
}

// A worker that acks nothing past the ack timeout may be wedged, or writing
// to storage slow enough that one commit outlasts the timeout. Terminating
// the second replays the same load against the same storage: the matrix run
// restarted 12 times against a struggling S3 and never converged (#422).
// The worker reports its network output (WorkerMetricsReport.net_tx_bytes);
// output that keeps growing marks it busy, not stalled.
const (
	// progressMinBytes is the growth that counts as progress: well above
	// the control stream's own reports and logs.
	progressMinBytes = 1 << 20
	// progressAckCap bounds how long progress stands in for acks, in ack
	// timeouts: a worker that keeps sending but never acks (retrying one
	// failing upload, say) is ended all the same.
	progressAckCap = 10
)

// progressSample is one reported network output and when it arrived.
type progressSample struct {
	at    time.Time
	bytes int64
}

// progressKeep bounds the samples kept per worker: enough to span any sane
// ack timeout at the worker's 5 s report cadence.
const progressKeep = 128

// noteProgress records a worker's cumulative network output. A counter lower
// than the last one belongs to a new process: its samples start over.
func (s *supervisor) noteProgress(worker string, txBytes int64, at time.Time) {
	if txBytes <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.progress[worker]
	if n := len(h); n > 0 && txBytes < h[n-1].bytes {
		h = h[:0]
	}
	h = append(h, progressSample{at: at, bytes: txBytes})
	if len(h) > progressKeep {
		h = append(h[:0], h[len(h)-progressKeep:]...)
	}
	s.progress[worker] = h
}

// busyLocked reports whether a worker past its ack timeout is making
// progress: its output grew by progressMinBytes over the last ack timeout
// (from the newest sample at or before its start), and its last ack is
// within progressAckCap timeouts. Caller holds s.mu.
func (s *supervisor) busyLocked(worker string, now, lastAck time.Time, ack time.Duration) bool {
	if lastAck.IsZero() || now.Sub(lastAck) > progressAckCap*ack {
		return false
	}
	h := s.progress[worker]
	if len(h) < 2 || now.Sub(h[len(h)-1].at) > ack {
		return false
	}
	from := h[0]
	for _, p := range h {
		if p.at.After(now.Add(-ack)) {
			break
		}
		from = p
	}
	return h[len(h)-1].bytes-from.bytes >= progressMinBytes
}
