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
	AckTimeout  time.Duration
	MaxResets   int
	ResetWindow time.Duration
	Poll        time.Duration
}

// supervisor watches the workers' ack health and owns the reset window.
type supervisor struct {
	c  *Coordinator
	mu sync.Mutex

	lastAck map[string]time.Time // worker → last ack
	resets  map[string][]time.Time
	pending map[string]bool // worker reset but not yet reattached
}

func newSupervisor(c *Coordinator) *supervisor {
	return &supervisor{
		c:       c,
		lastAck: map[string]time.Time{},
		resets:  map[string][]time.Time{},
		pending: map[string]bool{},
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
	s.mu.Unlock()
}

// noteAck records a worker's ack time.
func (s *supervisor) noteAck(worker string, at time.Time) {
	s.mu.Lock()
	s.lastAck[worker] = at
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

// tick resets every streaming worker past its ack timeout, and terminates
// the job once the sliding window of resets is exhausted.
func (s *supervisor) tick(now time.Time, cfg SupervisorConfig) error {
	ack := cfg.AckTimeout
	if ack <= 0 {
		ack = 30 * time.Second
	}
	maxResets := cfg.MaxResets
	if maxResets <= 0 {
		maxResets = 5
	}
	window := cfg.ResetWindow
	if window <= 0 {
		window = 15 * time.Minute
	}

	var stale []string
	// Lock order: c.mu first, then s.mu. Session takes c.mu and calls
	// noteAttach (s.mu) afterwards, so tick must never hold s.mu while
	// taking c.mu — the two orders would deadlock (audit #3).
	s.c.mu.Lock()
	type attachProbe struct {
		name     string
		attached bool
		owes     bool
	}
	probes := make([]attachProbe, 0, len(s.c.workers))
	for name, w := range s.c.workers {
		probes = append(probes, attachProbe{name: name, attached: w.attached, owes: len(w.queue) > 0})
	}
	s.c.mu.Unlock()
	// A worker owes work when it holds a delivered-but-unacked batch or has
	// one queued. inFlight takes the index's own lock, so it is read outside
	// c.mu.
	for i := range probes {
		probes[i].owes = probes[i].owes || s.c.inFlight(probes[i].name) > 0
	}

	s.mu.Lock()
	for _, p := range probes {
		at, ok := s.lastAck[p.name]
		// A reset worker that never reattached keeps the job in crashloop;
		// an attached worker that never acked is just as stale.
		// An ack timeout means a worker owes work and is not delivering it.
		// An ATTACHED worker that owes nothing is merely idle — a quiet
		// table, or one that just went through a re-slice — and resetting it
		// destroys its open staged cycles for no reason (issue #312).
		if s.pending[p.name] || (p.attached && p.owes && (!ok || now.Sub(at) > ack)) {
			stale = append(stale, p.name)
		}
	}
	s.mu.Unlock()

	for _, worker := range stale {
		w, ok := s.c.workers[worker]
		if !ok {
			continue
		}
		// CD-2: a reset cancels the session but is NOT a failure — the
		// worker reconnects and drains the queue. The in-flight (unacked)
		// batches are redelivered on reconnect (issue #235), so a reset no
		// longer loses them — but it replays them, duplicating the work (and
		// plain appends). With batches owed, terminate instead for a clean
		// replay from the committed position. A reset is safe only when the
		// worker owes nothing.
		if n := s.c.inFlight(worker); n > 0 {
			return fmt.Errorf("coordinator: worker %s stalled with %d in-flight batch(es) — a reset would replay them; terminating for a clean replay",
				worker, n)
		}
		if n := s.recordReset(worker, now, window); n >= maxResets {
			return fmt.Errorf("coordinator: crashloop: worker %s: %d resets in %s",
				worker, maxResets, window)
		}
		s.c.resetWorker(w)
	}
	return nil
}

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
