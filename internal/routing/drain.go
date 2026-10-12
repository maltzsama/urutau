package routing

import (
	"context"
	"fmt"
	"time"
)

const (
	// DefaultDrainTimeout bounds the staged-cycle and in-flight drains of a
	// re-slice when the pipeline sets none.
	DefaultDrainTimeout = 60 * time.Second
	// DefaultMaxWorkers is a table's partition cap when neither the table
	// nor the pipeline sets one.
	DefaultMaxWorkers = 32
	// drainPoll is the fallback re-check cadence of a drain: it wakes on the
	// ack notification, and this bounds how long a missed one could stall it.
	drainPoll = 10 * time.Millisecond
)

// DrainTimeout is the configured drain bound, or its default.
func DrainTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return DefaultDrainTimeout
}

// WorkerCap is a table's partition cap: the table's own maximum when set,
// else the pipeline's, else the default. A table that declares its own cap
// ignores the pipeline's.
func WorkerCap(tableMax, pipelineMax int) int {
	if tableMax > 0 {
		return tableMax
	}
	if pipelineMax > 0 {
		return pipelineMax
	}
	return DefaultMaxWorkers
}

// Owed is what a table still owes before its layout may flip.
type Owed struct {
	// InFlight and Queued are the batches delivered and not acked, and not
	// delivered yet, over the table's current owners.
	InFlight, Queued int
	// CyclesOpen and CyclesDone are the table's staged cycles still waiting
	// for a delivery, and complete but not committed.
	CyclesOpen, CyclesDone int
}

// Total is everything still owed.
func (o Owed) Total() int { return o.InFlight + o.Queued + o.CyclesOpen + o.CyclesDone }

// AwaitDrained blocks until a table owes nothing: no in-flight or queued
// batch on any current owner and no open staged cycle. wake is the ack
// notification (issue #587); openSeqs names the cycles still open, for the
// timeout's error.
//
// In-flight batches matter because a worker's window frees strictly from the
// head, in send order, and a batch leaves only when the table's acked
// position covers it. After a flip the owner receives a DIFFERENT key range,
// whose acks need not cover the batch still at its head, so the window can
// wedge: the owner stops acking, the supervisor calls it stale, and the reset
// discards its open staged cycles — silently losing the rows they carried.
// The staged cycles must commit as one unit and so must not span two layouts.
func AwaitDrained(ctx context.Context, timeout time.Duration, wake <-chan struct{}, owed func() Owed, openSeqs func() []uint64) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(drainPoll)
	defer tick.Stop()
	for {
		o := owed()
		if o.Total() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("%d batch(es)/cycle(s) still pending after %s (in-flight=%d queued=%d staged-cycles=%d open=%d done=%d open-seqs=%v)",
				o.Total(), timeout, o.InFlight, o.Queued, o.CyclesOpen+o.CyclesDone, o.CyclesOpen, o.CyclesDone, openSeqs())
		case <-wake:
		case <-tick.C:
		}
	}
}

// IdleEnd is how a wait for an owner to drain ended.
type IdleEnd int

const (
	// Idle: the owner drained.
	Idle IdleEnd = iota
	// IdleCancelled: the context ended first.
	IdleCancelled
	// IdleTimedOut: the bound passed first.
	IdleTimedOut
)

// AwaitIdle waits, bounded, until busy reports false: a removed owner's
// in-flight and queued batches are gone. It does not return an error: the
// flip has already committed, so the caller retires the owner whichever way
// the wait ends, and an owner that never drains has its range replayed by
// the one that inherits it, which is safe because the writes are idempotent
// upserts.
func AwaitIdle(ctx context.Context, timeout time.Duration, wake <-chan struct{}, busy func() bool) IdleEnd {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(drainPoll)
	defer tick.Stop()
	for busy() {
		select {
		case <-ctx.Done():
			return IdleCancelled
		case <-deadline.C:
			return IdleTimedOut
		case <-wake:
		case <-tick.C:
		}
	}
	return Idle
}
