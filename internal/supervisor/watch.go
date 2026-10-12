package supervisor

import (
	"sync"
	"time"
)

// Watch is what the coordinator's supervision knows of each worker: when it
// last acked and last delivered, whether it was reset or lost and is awaited,
// its network output (Progress) and its losses and crashes (Health). Judge
// reads it to tell a stalled worker, which is reset, from one gone for too
// long, which ends the run. The zero value is ready to use.
type Watch struct {
	mu sync.Mutex

	lastAck map[string]time.Time // worker → last ack
	resets  map[string][]time.Time
	pending map[string]bool // worker reset or lost and not yet reattached
	// lastDelivered is when each worker last delivered: an ack, or owing
	// nothing. Unlike lastAck, a reconnect does not move it.
	lastDelivered map[string]time.Time

	// Progress is each worker's recent network output reports: a worker
	// whose output grows is busy on slow storage, not stalled (#422).
	Progress Progress
	// Health is each worker's losses and crashes.
	Health Health
}

// init allocates the maps. Callers hold mu.
func (w *Watch) init() {
	if w.lastAck == nil {
		w.lastAck = map[string]time.Time{}
		w.resets = map[string][]time.Time{}
		w.pending = map[string]bool{}
		w.lastDelivered = map[string]time.Time{}
	}
}

// NoteRegistered seeds a newly created worker group's ack clock. Judge treats
// an attached worker with no last ack as stale, and a session marks the
// worker attached before NoteAttach, so a tick landing in that window would
// reset a worker that had just connected — a scale-out crashloop. Boot never
// hit it: every worker attaches before the supervision starts ticking.
func (w *Watch) NoteRegistered(worker string, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	if _, ok := w.lastAck[worker]; !ok {
		w.lastAck[worker] = at
	}
	if _, ok := w.lastDelivered[worker]; !ok {
		w.lastDelivered[worker] = at
	}
}

// NoteAck records a worker's ack time: it delivered.
func (w *Watch) NoteAck(worker string, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	w.lastAck[worker] = at
	w.lastDelivered[worker] = at
}

// NoteAttach clears a worker's pending state on a fresh Hello, at now.
func (w *Watch) NoteAttach(worker string, now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	delete(w.pending, worker)
	w.lastAck[worker] = now
}

// NoteDeliveredAt records that worker delivered (acked, or owed nothing) at.
func (w *Watch) NoteDeliveredAt(worker string, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	w.lastDelivered[worker] = at
}

// Forget drops every trace of a worker the coordinator no longer tracks — a
// scale-out rolled back before commit, or an owner retired by a scale-in. Its
// clocks, pending flag, reset window, output samples and crashes must not
// linger, or a later re-registration under the same name would inherit them.
func (w *Watch) Forget(worker string) {
	w.mu.Lock()
	delete(w.lastAck, worker)
	delete(w.pending, worker)
	delete(w.resets, worker)
	delete(w.lastDelivered, worker)
	w.mu.Unlock()
	w.Progress.Forget(worker)
	w.Health.Forget(worker)
}

// Tracks reports whether any trace of worker is kept.
func (w *Watch) Tracks(worker string) bool {
	w.mu.Lock()
	_, ack := w.lastAck[worker]
	_, pending := w.pending[worker]
	_, resets := w.resets[worker]
	_, delivered := w.lastDelivered[worker]
	w.mu.Unlock()
	return ack || pending || resets || delivered || w.Progress.Tracks(worker) || w.Health.Tracks(worker)
}

// SetPending marks a worker as reset and not reattached.
func (w *Watch) SetPending(worker string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	w.pending[worker] = true
}

// MarkPending sets worker pending and reports whether it already was. The
// test and the set are one step: a loss arrives on two goroutines (the
// worker's Session and its Control stream both end), and each must be able to
// tell whether it is the first to see it.
func (w *Watch) MarkPending(worker string) (already bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	already = w.pending[worker]
	w.pending[worker] = true
	return already
}

// IsPending reports whether a worker is reset or lost and not reattached.
func (w *Watch) IsPending(worker string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pending[worker]
}

// RecordReset pushes a reset timestamp into the worker's sliding window,
// expiring entries older than window, and returns the resulting count — read
// under the same lock, so a check that follows cannot race a concurrent reset
// (issue #208).
func (w *Watch) RecordReset(worker string, now time.Time, window time.Duration) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	cutoff := now.Add(-window)
	kept := w.resets[worker][:0]
	for _, t := range w.resets[worker] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	w.resets[worker] = append(kept, now)
	return len(w.resets[worker])
}

// Probe is one worker as a tick sees it.
type Probe struct {
	Name string
	// Attached and HadSession: its session is up; it has had one.
	Attached   bool
	HadSession bool
	// Owes: it holds a delivered-but-unacked batch or has one queued.
	Owes bool
	// MinHeld is the oldest batch id it holds, read only for a worker
	// Health.Recovering names; 0 otherwise.
	MinHeld uint64
}

// Judge reads one tick's probes at now and returns the workers to reset —
// attached, owing work and silent past ack — and those gone: owing work and
// delivering none of it for the delivery timeout, attached or not. A worker
// lost or reset and awaited is not reset again. Mid-snapshot only the
// delivery rule applies: a reset would drop the worker's open windows, which
// the snapshot redoes on a loss instead.
func (w *Watch) Judge(now time.Time, ack, delivery time.Duration, probes []Probe, snapshotting bool) (stalled, gone []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.init()
	for _, p := range probes {
		if !p.Owes {
			w.lastDelivered[p.Name] = now
		}
		if p.Attached && !w.pending[p.Name] {
			w.Health.Delivering(p.Name, p.MinHeld, now, ack)
		}
		// A worker that owes work and has delivered none of it for the
		// delivery timeout ends the run, attached or not: a partition that
		// lets it reconnect for a moment every few seconds, or a Pod that
		// never starts, is not a worker making progress. A worker whose
		// uploads keep flowing is delivering to storage (#422).
		if at, ok := w.lastDelivered[p.Name]; ok && p.Owes && now.Sub(at) > delivery && !w.Progress.Busy(p.Name, now, at, delivery) {
			gone = append(gone, p.Name)
			continue
		}
		if w.pending[p.Name] || (!p.Attached && p.HadSession) || snapshotting {
			continue // lost or reset and awaited, or mid-snapshot
		}
		// An ATTACHED worker that owes nothing is merely idle — a quiet
		// table, or one that just went through a re-slice — and resetting it
		// is pointless (issue #312).
		// The clock starts at the last ack, or at the last tick that saw
		// the worker owing nothing, whichever is later: a worker that waited
		// for work acked nothing in the meantime, and that wait is not a
		// stall. After a coordinator restart the workers reconnect up to a
		// minute before the reader starts; counted from the Hello alone, the
		// first batch queued found the timeout already spent, and every
		// worker that had connected early was reset.
		at, ok := w.lastAck[p.Name]
		if idle, seen := w.lastDelivered[p.Name]; seen && idle.After(at) {
			at, ok = idle, true
		}
		if p.Attached && p.Owes && (!ok || now.Sub(at) > ack) && !w.Progress.Busy(p.Name, now, at, ack) {
			stalled = append(stalled, p.Name)
		}
	}
	return stalled, gone
}
