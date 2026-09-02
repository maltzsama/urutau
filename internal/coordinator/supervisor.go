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
	s.mu.Lock()
	for worker, w := range s.c.workers {
		s.c.mu.Lock()
		attached := w.attached
		s.c.mu.Unlock()
		at, ok := s.lastAck[worker]
		// A reset worker that never reattached keeps the job in crashloop;
		// an attached worker that never acked is just as stale.
		if s.pending[worker] || (attached && (!ok || now.Sub(at) > ack)) {
			stale = append(stale, worker)
		}
	}
	s.mu.Unlock()

	for _, worker := range stale {
		w, ok := s.c.workers[worker]
		if !ok {
			continue
		}
		s.recordReset(worker, now, window)
		if len(s.resets[worker]) >= maxResets {
			return fmt.Errorf("coordinator: crashloop: worker %s: %d resets in %s",
				worker, maxResets, window)
		}
		s.c.resetWorker(w)
	}
	return nil
}

// recordReset pushes a reset timestamp into the worker's sliding window,
// expiring entries older than window.
func (s *supervisor) recordReset(worker string, now time.Time, window time.Duration) {
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
