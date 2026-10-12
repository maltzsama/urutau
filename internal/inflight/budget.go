package inflight

import (
	"context"
	"sync"
)

// Budget is the coordinator's global in-flight budget (design §5.4.1):
// a ceiling on serialized batch bytes queued or sent but not yet acked, per
// PROCESS — not per worker. A slow worker may consume more than its share
// but never starves the others below perWorkerMin, nor holds more than
// perWorkerMax; a budget that is full blocks the producer, which is the
// structural backpressure down to the reader loop.
type Budget struct {
	TotalBytes   int64
	perWorkerMin int64
	// perWorkerMax caps one worker's bytes in flight (0: no cap). Without it
	// a worker committing slower than the stream absorbed the whole budget:
	// under the full production-readiness profile one held ~490 MiB queued
	// for it, decoded it all, and was OOM-killed (#437). A worker with
	// nothing in flight is still admitted one batch past the cap, so a batch
	// larger than it can never deadlock.
	PerWorkerMax int64

	mu   sync.Mutex
	cond *sync.Cond
	used map[string]int64
	// usedTotal is the running sum of used, maintained on acquire/release so
	// the hot path does not scan the map (issue #582).
	usedTotal int64
	// oversizedOwner is the worker holding the single oversized batch the
	// budget admits at once. A batch larger than the whole ceiling can never
	// fit it, so waiting for room would deadlock (nothing can free the budget
	// when nothing is in flight). It is admitted regardless of the sum as
	// long as no OTHER oversized batch is in flight — that bounds the
	// over-ceiling memory to one batch while still making progress. Cleared
	// when that worker's charge returns to zero.
	oversizedOwner string
}

func NewBudget(totalBytes, perWorkerMin int64) *Budget {
	b := &Budget{
		TotalBytes:   totalBytes,
		perWorkerMin: perWorkerMin,
		used:         map[string]int64{},
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// underWorkerMax reports whether n more bytes keep worker within
// perWorkerMax; a worker with nothing in flight always may take one batch.
// Caller holds b.mu.
func (b *Budget) underWorkerMax(worker string, n int64) bool {
	used := b.used[worker]
	return b.PerWorkerMax <= 0 || used == 0 || used+n <= b.PerWorkerMax
}

// sum reports the total bytes in flight. Caller holds b.mu.
func (b *Budget) sum() int64 {
	return b.usedTotal
}

// acquire reserves n bytes for one worker, blocking while the process is
// over budget AND the worker is beyond its minimum floor. The wait is
// ctx-aware: a cancelled pipeline stops waiting.
//
// The ctx wake-up uses context.AfterFunc, which registers a callback fired
// only on cancellation — no goroutine or channel is allocated on the hot
// (under-budget) path.
func (b *Budget) Acquire(ctx context.Context, worker string, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	// Wake blocked waiters when ctx dies, so they re-check and exit.
	stop := context.AfterFunc(ctx, func() { b.cond.Broadcast() })
	defer stop()

	oversized := n > b.TotalBytes
	for {
		if oversized {
			// An oversized batch can never fit the ceiling, so waiting for
			// room would deadlock; admit it when no other oversized batch is
			// in flight (issue #209).
			if b.oversizedOwner == "" {
				break
			}
		} else if (b.sum()+n <= b.TotalBytes || b.used[worker]+n <= b.perWorkerMin) && b.underWorkerMax(worker, n) {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		b.cond.Wait()
	}
	b.used[worker] += n
	b.usedTotal += n
	if oversized {
		b.oversizedOwner = worker
	}
	return nil
}

// release returns n bytes to the pool.
func (b *Budget) Release(worker string, n int64) {
	b.mu.Lock()
	b.used[worker] -= n
	b.usedTotal -= n
	if b.used[worker] <= 0 {
		delete(b.used, worker)
		if b.oversizedOwner == worker {
			b.oversizedOwner = ""
		}
	}
	b.mu.Unlock()
	b.cond.Broadcast()
}

// isOversized reports whether n is larger than the whole budget — a batch that
// can never fit the ceiling and so takes the single oversized slot.
func (b *Budget) IsOversized(n int64) bool { return n > b.TotalBytes }

// clearOversized frees the oversized slot when its batch is acked, so a later
// oversized batch is not blocked while the owner keeps normal traffic in
// flight (issue #209 review).
func (b *Budget) ClearOversized(worker string) {
	b.mu.Lock()
	if b.oversizedOwner == worker {
		b.oversizedOwner = ""
	}
	b.mu.Unlock()
	b.cond.Broadcast()
}

// inFlight reports the bytes reserved for one worker.
func (b *Budget) InFlight(worker string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used[worker]
}
