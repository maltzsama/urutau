package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
	sup "github.com/maltzsama/urutau/internal/supervisor"
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

// supervisor watches the workers' ack health: sup.Watch holds what is known
// of each worker and judges a tick; the supervisor reads the workers for it
// and acts on its verdict.
type supervisor struct {
	c *Coordinator
	sup.Watch
}

func newSupervisor(c *Coordinator) *supervisor {
	return &supervisor{c: c}
}

// run polls worker health until ctx is done or the job goes terminal.
func (s *supervisor) run(ctx context.Context, cfg SupervisorConfig, terminate chan<- error) {
	sup.Run(ctx, cfg.Poll, func(now time.Time) error { return s.tick(now, cfg) }, terminate)
}

// tick resets a worker that is attached, owes work and has stopped acking,
// and awaits one that is lost or reset until it reconnects (issue #461). It
// ends the run only for what does not heal by itself: a worker gone longer
// than the delivery timeout, or a stalled append worker whose unacked batches
// a redelivery would append twice. A crash loop — the same worker lost again
// and again without delivering what it owed — is ended by workerBack
// (recovery.go).
func (s *supervisor) tick(now time.Time, cfg SupervisorConfig) error {
	ack := sup.AckTimeout(cfg.AckTimeout)
	delivery := sup.DeliveryTimeout(cfg.DeliveryTimeout)

	// Lock order: c.mu first, then the watch's lock. Session takes c.mu and
	// calls NoteAttach afterwards, so tick must never hold the watch's lock
	// while taking c.mu — the two orders would deadlock (audit #3).
	s.c.mu.Lock()
	probes := make([]sup.Probe, 0, len(s.c.workers))
	for name, w := range s.c.workers {
		probes = append(probes, sup.Probe{Name: name, Attached: w.attached, HadSession: w.hadSession, Owes: len(w.queue) > 0})
	}
	s.c.mu.Unlock()
	// A worker owes work when it holds a delivered-but-unacked batch or has
	// one queued. The index has its own lock, so it is read outside c.mu,
	// once per worker. Its oldest held batch is needed only for a worker
	// coming back from a crash (Health.Delivering ignores the others), and
	// finding it scans the worker's whole in-flight window: it is not looked
	// up for a healthy worker, which is nearly every worker on every tick.
	recovering := s.Health.Recovering()
	for i := range probes {
		if idx := s.c.index.Of(probes[i].Name); idx != nil {
			probes[i].Owes = probes[i].Owes || idx.InFlight() > 0
			if recovering[probes[i].Name] {
				probes[i].MinHeld = idx.MinHeldID()
			}
		}
	}

	stalled, gone := s.Judge(now, ack, delivery, probes, s.c.snapshotActive.Load())

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
		if n := s.c.index.InFlight(name); n > 0 && s.c.workerAppends(name) {
			return fmt.Errorf("coordinator: worker %s stalled with %d in-flight batch(es) on an append table — a reset would append them twice",
				name, n)
		}
		s.c.resetWorker(w)
	}
	return nil
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
	// Copy under the lock: loseWorker writes w.epoch under c.mu, so reading it
	// after Unlock races (issue #559).
	epoch := w.epoch
	c.mu.Unlock()
	c.supervisor.SetPending(w.name)
	c.log.Warn("coordinator: reset worker", "worker", w.name, "epoch", epoch)
	if c.metrics != nil {
		c.metrics.WorkerResets.WithLabelValues(w.name, "ack_timeout").Inc()
	}
	c.emitLog(eventlog.KindWorkerReset, map[string]any{
		"worker": w.name,
		"epoch":  epoch,
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
