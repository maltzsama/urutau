package coordinator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/position"
)

func mustLSN(t *testing.T, s string) position.Position {
	t.Helper()
	p, err := position.ParseLSN(s)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// owingCoordinator builds a coordinator with one worker that owes work, so
// confirmedPosition consults it.
func owingCoordinator(t *testing.T, pos position.Position) *Coordinator {
	t.Helper()
	q := make(chan queuedBatch, 1)
	q <- queuedBatch{}
	return &Coordinator{
		metrics:   observability.New(),
		confirmed: map[string]position.Position{"w": pos},
		workers:   map[string]*workerState{"w": {name: "w", queue: q}},
	}
}

// #602 review: the confirmed-position age grows while work is pending and the
// minimum committed position does not advance, resets only on FORWARD progress
// (not on a regression), starts when a first commit is awaited, and is zero
// when the pipeline is caught up.
func TestPublishConfirmedAge(t *testing.T) {
	base := time.Unix(1000, 0)
	c := owingCoordinator(t, mustLSN(t, "0/16B6C50"))

	// First observation while owing and unconfirmed-yet: the clock starts, so
	// a commit that never arrives becomes visible instead of a gauge pinned at
	// zero.
	c.publishConfirmedAge(base)
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 0 {
		t.Fatalf("age at first observation = %v, want 0", got)
	}
	c.publishConfirmedAge(base.Add(30 * time.Second))
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 30 {
		t.Fatalf("age after 30s stuck = %v, want 30", got)
	}

	// Forward progress resets the clock.
	c.confirmedMu.Lock()
	c.confirmed["w"] = mustLSN(t, "0/16B6C51")
	c.confirmedMu.Unlock()
	c.publishConfirmedAge(base.Add(45 * time.Second))
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 0 {
		t.Fatalf("age after the position advanced = %v, want 0", got)
	}

	// A REGRESSION (the minimum drops) must not reset the clock.
	c.confirmedMu.Lock()
	c.confirmed["w"] = mustLSN(t, "0/16B6C00")
	c.confirmedMu.Unlock()
	c.publishConfirmedAge(base.Add(50 * time.Second))
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 5 {
		t.Fatalf("age after a regression = %v, want 5 (clock kept)", got)
	}

	// Caught up: the worker no longer owes, so nothing can stall — the gauge
	// is zero regardless of the last position.
	c.mu.Lock()
	c.workers["w"].queue = make(chan queuedBatch)
	c.mu.Unlock()
	c.publishConfirmedAge(base.Add(60 * time.Second))
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 0 {
		t.Fatalf("age while caught up = %v, want 0", got)
	}
}

// #602: totalOpen counts staged cycles still accumulating or waiting to
// commit.
func TestStagedCyclesTotalOpen(t *testing.T) {
	s := newStagedCycles()
	if s.totalOpen() != 0 {
		t.Fatalf("empty stagedCycles totalOpen = %d, want 0", s.totalOpen())
	}
	s.expect(core.TableRef{Source: "shop.a", Target: "raw.a"}, 1, []string{"w0"})
	if s.totalOpen() != 1 {
		t.Fatalf("totalOpen after one open cycle = %d, want 1", s.totalOpen())
	}
}
