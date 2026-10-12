package routing

import (
	"context"
	"fmt"
	"sync"

	"github.com/maltzsama/urutau/source"
)

// ScaleHost is the pipeline a Scaler re-slices: it owns the layout and the
// owners, and does every step of a re-slice that touches them.
type ScaleHost[O any] interface {
	// Layout returns the live layout; Publish swaps in its successor.
	Layout() *Layout[O]
	Publish(l *Layout[O])

	// Admit returns why target cannot be re-sliced to n owners now, or nil.
	Admit(target string, n int) error
	// Ranges resolves the key ranges of target split n ways.
	Ranges(ctx context.Context, target string, n int) ([]source.Chunk, error)

	// Owner returns the owner of target's partition p, registering it when
	// it does not exist, and whether this call created it.
	Owner(target string, p int) (owner O, created bool, err error)
	// Name returns an owner's name.
	Name(owner O) string
	// Stranded reports whether owner owes work that nothing will deliver:
	// it is not attached and holds queued or in-flight batches.
	Stranded(owner O) bool
	// Forget drops an owner created for a re-slice that did not commit.
	Forget(owner O)
	// Seed gives the created owners a position to resume from.
	Seed(created []O)

	// PauseInput and ResumeInput hold and release target's input.
	PauseInput(target string)
	ResumeInput(target string)
	// Drain waits, bounded, until target owes nothing on these owners.
	Drain(ctx context.Context, target string, owners []O) error
	// Retire drains an owner the new layout dropped, then removes it.
	Retire(ctx context.Context, owner O)

	// Fail ends the run on an error a retry cannot recover from.
	Fail(err error)
	// Scaled records that target went from one owner count to another.
	Scaled(target string, from, to int)
}

// Scaler re-slices tables, one at a time: two concurrent scale events on the
// same table would each read-modify-write the layout and one would be lost.
// The zero value is ready to use.
type Scaler[O any] struct {
	mu sync.Mutex
}

// Scale re-slices target across n partitions, adding or removing owners so
// the table ends up with exactly n of them. It reports whether the layout
// changed.
//
// The sequence is prepare → commit, and every wait is bounded: a step that
// cannot complete fails the scale and leaves the OLD layout intact, so a
// scale is a retryable no-op rather than an incident (never an unbounded
// pause, never data loss):
//
//  1. Resolve the new ranges.
//  2. Register any new owner, so its worker is accepted the moment its pod
//     starts.
//  3. Seed the new owners with a position to resume from.
//  4. PREPARE: pause the table's input and drain what it owes on the
//     current owners.
//  5. COMMIT: publish the new layout and resume the input. The flip is
//     atomic; every batch after it is routed by the new layout.
//  6. Retire the owners the new layout dropped — only now: a removed owner
//     still holds in-flight batches for keys the new layout has reassigned,
//     and draining it before the flip would race the reader still routing
//     to it.
//
// A timeout in the prepare phase resumes the input and returns, leaving the
// old layout in place: the scaler retries, and worker recovery stays the
// supervisor's job.
func (s *Scaler[O]) Scale(ctx context.Context, host ScaleHost[O], target string, n int) (bool, error) {
	if n < 1 {
		return false, fmt.Errorf("scale %s: worker count must be >= 1, got %d", target, n)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cur := host.Layout()
	owners, ok := cur.OwnersOf(target)
	if !ok {
		return false, fmt.Errorf("scale %s: no such routed table", target)
	}
	if len(owners) == n {
		return false, nil
	}
	if err := host.Admit(target, n); err != nil {
		return false, fmt.Errorf("scale %s: %w", target, err)
	}

	ranges, err := host.Ranges(ctx, target, n)
	if err != nil {
		return false, fmt.Errorf("scale %s: %w", target, err)
	}
	if len(ranges) == 0 {
		return false, fmt.Errorf("scale %s: no partition range resolved", target)
	}

	next, removed, created, err := Reslice(owners, n, func(p int) (O, bool, error) {
		return host.Owner(target, p)
	})
	if err != nil {
		return false, fmt.Errorf("scale %s: %w", target, err)
	}

	// A scale-in removes owners whose Pods are already deleted. One that is
	// not attached yet owes work can never drain it: the drain below would
	// time out on every retry and the table would wedge. The case the
	// supervisor cannot see is an owner that never attached at all — its Pod
	// died while still connecting — so it is never "detached". Its work is
	// uncommitted, so a clean replay recovers it.
	for _, o := range removed {
		if host.Stranded(o) {
			err := fmt.Errorf("scale %s: removed owner %s is not attached and owes work; terminating for a clean replay", target, host.Name(o))
			host.Fail(err)
			return false, err
		}
	}

	// A pre-commit failure leaves the old layout active, so the owners this
	// call CREATED must be rolled back: otherwise they linger as ghosts —
	// accepting workers, supervised, and holding a retention position they
	// will never advance.
	committed := false
	defer func() {
		if !committed {
			for _, o := range created {
				host.Forget(o)
			}
		}
	}()

	// The new owners must be able to resume: an owner with no confirmed
	// position stalls source retention for the WHOLE pipeline, not just this
	// table. A scale-in creates none.
	host.Seed(created)

	// PREPARE: pause the table's input. The drain waits until the table
	// owes nothing, and a continuously loaded table never reaches that on
	// its own. Pausing makes it converge; on any failure the input resumes
	// and the old layout stands.
	host.PauseInput(target)
	resumed := false
	defer func() {
		if !resumed {
			host.ResumeInput(target)
		}
	}()
	if err := host.Drain(ctx, target, owners); err != nil {
		return false, fmt.Errorf("scale %s: drain: %w", target, err)
	}

	// COMMIT: the flip is atomic and instant, and happens only now that the
	// table owes nothing. Resuming routes the held batches — and every
	// batch after them — by the new layout.
	host.Publish(cur.WithTable(target, next, ranges))
	host.ResumeInput(target)
	resumed = true
	committed = true

	host.Scaled(target, len(owners), n)

	for _, o := range removed {
		host.Retire(ctx, o)
	}
	return true, nil
}
