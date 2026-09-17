package maintenance

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(quietWriter{}, nil))
}

type quietWriter struct{}

func (quietWriter) Write(p []byte) (int, error) { return len(p), nil }

// partialMaintainer fails one designated operation and completes every
// other, mimicking a real persistent failure of a single operation (an
// expired credential, a catalog that keeps rejecting that one commit).
type partialMaintainer struct {
	mu        sync.Mutex
	failOn    sink.MaintenanceOp
	completed []sink.MaintenanceOp
}

func (p *partialMaintainer) RunOnce(ctx context.Context, ops []sink.MaintenanceOp) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, op := range ops {
		if op == p.failOn {
			return errors.New("simulated failure")
		}
		p.completed = append(p.completed, op)
	}

	return nil
}

func (p *partialMaintainer) count(op sink.MaintenanceOp) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.completed {
		if c == op {
			n++
		}
	}

	return n
}

// defaultCfg enables all three operations at their production defaults
// (5m compaction, 10m expiry, 1h orphan cleanup), which is what makes the
// interaction below realistic: the intervals are deliberately far apart,
// so the operations only land on the same tick occasionally.
func defaultCfg() *spec.Maintenance {
	return &spec.Maintenance{
		Enabled:        true,
		Compaction:     &spec.CompactionConfig{},
		SnapshotExpiry: &spec.SnapshotExpiryConfig{},
		OrphanCleanup:  &spec.OrphanCleanupConfig{},
	}
}

// driveTurns drives RunTurn — the exact function RunLoop calls on every
// tick — against a virtual clock for the given span. Going through RunTurn
// rather than reimplementing it is the point: a copy of the scheduling
// logic would pass no matter what RunLoop actually does.
//
// The virtual clock is what makes production intervals testable. At a
// 2m30s check interval, a faithful real-time test would run for over an
// hour; this covers the same hour in microseconds.
// TestRunLoopDispatchesPerOperation covers the ticker wiring itself.
func driveTurns(t *testing.T, m sink.Maintainer, sched *Schedule, cfg *spec.Maintenance, span time.Duration) {
	t.Helper()

	tick := CheckInterval(cfg)
	now := time.Now()
	for elapsed := time.Duration(0); elapsed < span; elapsed += tick {
		now = now.Add(tick)
		RunTurn(context.Background(), m, "raw.orders", cfg, sched, quietLogger(), now)
	}
}

// TestFailingOperationDoesNotStarveLaterOnes is the sharp end of the bug
// this scheduling fix addresses.
//
// The three operations have deliberately different cadences (5m / 10m /
// 1h), so they only share a tick now and then. That separation collapses
// the moment one of them fails, if a failed turn is abandoned wholesale:
//
//   - the failing operation never completes, so it is never marked, so it
//     is due on every single tick from then on;
//   - every operation ordered after it is therefore blocked on every tick;
//   - and every operation before it re-runs on every tick, because a batch
//     that returns an error marks nothing.
//
// The worst of those is the middle one. A snapshot expiry that keeps
// failing would stop orphan cleanup — an hourly operation that physically
// frees warehouse storage — from ever running again, silently. Not
// degraded: stopped.
//
// Dispatching per operation and continuing past a failure keeps each
// operation on its own schedule, so only the broken one is broken.
func TestFailingOperationDoesNotStarveLaterOnes(t *testing.T) {
	cfg := defaultCfg()
	m := &partialMaintainer{failOn: sink.MaintenanceSnapshotExpiry}

	driveTurns(t, m, NewSchedule(), cfg, time.Hour)

	// Orphan cleanup runs hourly and is ordered last, behind the failing
	// expiry: it is the operation a whole-turn abort would starve.
	if got := m.count(sink.MaintenanceOrphanCleanup); got == 0 {
		t.Error("orphan cleanup never ran in an hour while snapshot expiry was failing — " +
			"an operation ordered after a failing one must still run; it reloads the table " +
			"and deletes only unreferenced files behind its own OlderThan window, so a " +
			"skipped expiry means less to collect, not an unsafe collection")
	}

	// Compaction must hold its own 5m cadence rather than firing on every
	// 2m30s tick. Its successes are marked even though the expiry that
	// follows it in the turn keeps failing.
	compactions := m.count(sink.MaintenanceCompaction)
	if compactions > 13 {
		t.Errorf("compaction ran %d times in an hour, want ~12 (its 5m interval) — "+
			"a failing expiry must not knock compaction onto the %v check interval",
			compactions, CheckInterval(cfg))
	}
	if compactions < 11 {
		t.Errorf("compaction ran %d times in an hour, want ~12 (its 5m interval)", compactions)
	}
}

// TestHealthyScheduleKeepsEachCadence is the control for the test above:
// with nothing failing, each operation runs on its own interval. Without
// it, a fix that simply ran everything on every tick would satisfy the
// starvation assertion.
func TestHealthyScheduleKeepsEachCadence(t *testing.T) {
	cfg := defaultCfg()
	m := &partialMaintainer{} // nothing fails

	driveTurns(t, m, NewSchedule(), cfg, time.Hour)

	// ~12 compactions (5m), ~6 expiries (10m), 1-2 cleanups (1h). The
	// ranges absorb the boundary effects of a 2m30s check interval landing
	// on or just past each due time.
	for _, c := range []struct {
		op       sink.MaintenanceOp
		min, max int
	}{
		{sink.MaintenanceCompaction, 11, 13},
		{sink.MaintenanceSnapshotExpiry, 5, 7},
		{sink.MaintenanceOrphanCleanup, 1, 2},
	} {
		if got := m.count(c.op); got < c.min || got > c.max {
			t.Errorf("%s ran %d times in an hour, want %d-%d", c.op, got, c.min, c.max)
		}
	}
}

// TestRunLoopDispatchesPerOperation covers the real RunLoop against a real
// ticker. It uses 2s intervals (a 1s check interval) purely so the test
// finishes quickly; the scheduling semantics it asserts are the ones
// driveTurns replays at production intervals above.
//
// The assertion is that a failing operation no longer takes the rest of
// the turn down with it: orphan cleanup completes even though the expiry
// ordered before it fails on every pass.
func TestRunLoopDispatchesPerOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &spec.Maintenance{
		Enabled:        true,
		Compaction:     &spec.CompactionConfig{Interval: "2s"},
		SnapshotExpiry: &spec.SnapshotExpiryConfig{Interval: "2s"},
		OrphanCleanup:  &spec.OrphanCleanupConfig{Interval: "2s"},
	}
	m := &partialMaintainer{failOn: sink.MaintenanceSnapshotExpiry}

	go RunLoop(ctx, m, "raw.orders", cfg, NewSchedule(), quietLogger())

	deadline := time.After(5 * time.Second)
	for m.count(sink.MaintenanceOrphanCleanup) == 0 {
		select {
		case <-deadline:
			t.Fatal("orphan cleanup never ran while snapshot expiry was failing — " +
				"RunLoop must not abandon the turn at the first failure")
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()

	if got := m.count(sink.MaintenanceCompaction); got == 0 {
		t.Error("compaction never completed")
	}
}
