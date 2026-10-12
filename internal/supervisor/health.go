package supervisor

import (
	"sync"
	"time"

	"github.com/maltzsama/urutau/internal/coordk8s"
)

// workerHealth is what is tracked of a worker's losses.
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

// Health is each worker's losses and crashes: what tells a worker lost to a
// hiccup, which is awaited, from one in a crash loop, which ends the run. A
// loss is classified when the worker comes back, from its Pod's last
// termination; a crash counts toward the loop until the worker has delivered
// what it owed when it came back. The zero value is ready to use.
type Health struct {
	mu       sync.Mutex
	byWorker map[string]*workerHealth
}

// of returns worker's record. Callers hold mu.
func (h *Health) of(worker string) *workerHealth {
	w := h.byWorker[worker]
	if w == nil {
		w = &workerHealth{}
		if h.byWorker == nil {
			h.byWorker = map[string]*workerHealth{}
		}
		h.byWorker[worker] = w
	}
	return w
}

// LossTime is when worker's last loss was seen, or now if none was.
func (h *Health) LossTime(worker string, now time.Time) time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	if w := h.byWorker[worker]; w != nil && !w.lostAt.IsZero() {
		return w.lostAt
	}
	return now
}

// Loss records a loss at epoch, seen at now, reporting false for a loss
// already seen (the worker's other stream).
func (h *Health) Loss(worker string, epoch uint64, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.of(worker)
	if w.lossPending && w.lossEpoch == epoch {
		return false
	}
	w.lossEpoch, w.lossPending = epoch, true
	w.lostAt = now
	return true
}

// Back classifies the loss a worker comes back from, from its Pod's last
// container termination (known is false when there is none to read): an OOM
// kill, a panic or an error is a crash; a Pod replaced from outside or a
// worker that exited because it lost the coordinator is not. It returns the
// worker's crashes in a row, whether this loss was one, and its description.
// owedMark is the highest batch id the worker owes as it comes back.
func (h *Health) Back(worker string, term coordk8s.Termination, known bool, owedMark uint64, now time.Time) (int, bool, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.of(worker)
	w.owedMark, w.backAt = owedMark, now
	crashed, desc := false, ""
	if known {
		// A crash is fresh when the same Pod restarted since it was last
		// seen. The second arm is the first sighting of a Pod: nothing was
		// seen before, so any restart it already counts is new to us.
		fresh := (term.PodUID == w.seenUID && term.Restarts > w.seenRestarts) ||
			(w.seenUID == "" && term.Restarts > 0)
		if w.lossPending && fresh {
			crashed, desc = term.Crash()
		}
		w.seenUID, w.seenRestarts = term.PodUID, term.Restarts
	}
	w.lossPending = false
	if crashed {
		w.crashes++
	}
	return w.crashes, crashed, desc
}

// Recovering returns the workers Delivering acts on: those with a crash on
// record and no loss pending. Nil when there is none.
func (h *Health) Recovering() map[string]bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out map[string]bool
	for name, w := range h.byWorker {
		if w != nil && w.crashes > 0 && !w.lossPending {
			if out == nil {
				out = map[string]bool{}
			}
			out[name] = true
		}
	}
	return out
}

// Delivering starts a worker's crash count over once it has delivered every
// batch it owed when it came back and stayed up for ack: it is not
// crash-looping. minHeld is the oldest batch id it still holds, 0 for none.
func (h *Health) Delivering(worker string, minHeld uint64, now time.Time, ack time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.byWorker[worker]
	if w == nil || w.crashes == 0 || w.lossPending {
		return
	}
	if (minHeld == 0 || minHeld > w.owedMark) && now.Sub(w.backAt) >= ack {
		w.crashes = 0
	}
}

// Crashes is worker's crashes in a row.
func (h *Health) Crashes(worker string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if w := h.byWorker[worker]; w != nil {
		return w.crashes
	}
	return 0
}

// Tracks reports whether worker has a record.
func (h *Health) Tracks(worker string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.byWorker[worker] != nil
}

// Forget drops worker's record: its name may be reused by a later scale-out,
// which must not inherit its crashes.
func (h *Health) Forget(worker string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.byWorker, worker)
}
