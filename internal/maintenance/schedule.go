// Package maintenance schedules the engine's table-maintenance operations.
// It is the neutral seam between the spec (which declares per-operation
// intervals) and the sink (which executes one operation at a time via
// sink.Maintainer.RunOnce): the collapsed runner and the distributed
// coordinator both use it to decide which operations are due for a table,
// without either importing a concrete sink package.
package maintenance

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// Schedule tracks, per table, when each operation last ran, and reports the
// operations due at a given time. Safe for concurrent use.
//
// State is in-memory only: a restart loses it and every enabled operation
// becomes due again immediately. That is safe — each operation is
// idempotent — and costs at most one extra pass after a coordinator/runner
// restart.
type Schedule struct {
	mu   sync.Mutex
	last map[string]map[sink.MaintenanceOp]time.Time
}

// NewSchedule returns an empty schedule: every enabled operation is due on
// the first Due call.
func NewSchedule() *Schedule {
	return &Schedule{last: map[string]map[sink.MaintenanceOp]time.Time{}}
}

// Due returns the enabled operations whose interval has elapsed since they
// last ran, in execution order (compaction, snapshot expiry, orphan
// cleanup). An operation that has never run is due immediately. A disabled
// operation — its sub-config nil, or maintenance off entirely — is never
// returned.
func (s *Schedule) Due(table string, cfg *spec.Maintenance, now time.Time) []sink.MaintenanceOp {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	last := s.last[table]
	var due []sink.MaintenanceOp
	for _, op := range opsInOrder {
		interval, enabled := intervalFor(cfg, op)
		if !enabled {
			continue
		}
		ran, ok := last[op]
		if !ok || now.Sub(ran) >= interval {
			due = append(due, op)
		}
	}
	return due
}

// MarkRun records that the given operations completed for a table at now.
func (s *Schedule) MarkRun(table string, ops []sink.MaintenanceOp, now time.Time) {
	if len(ops) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.last[table]
	if m == nil {
		m = map[sink.MaintenanceOp]time.Time{}
		s.last[table] = m
	}
	for _, op := range ops {
		m[op] = now
	}
}

// opsInOrder is the order the scheduler reports due operations in, and the
// order they must execute in: compaction creates new files and invalidates
// old ones, snapshot expiry dereferences them, orphan cleanup physically
// removes them.
var opsInOrder = []sink.MaintenanceOp{
	sink.MaintenanceCompaction,
	sink.MaintenanceSnapshotExpiry,
	sink.MaintenanceOrphanCleanup,
}

// intervalFor returns an operation's effective interval and whether it is
// enabled at all (its sub-config is non-nil).
func intervalFor(cfg *spec.Maintenance, op sink.MaintenanceOp) (time.Duration, bool) {
	switch op {
	case sink.MaintenanceCompaction:
		if cfg.Compaction == nil {
			return 0, false
		}
		return durationOrDefault(cfg.Compaction.Interval, spec.DefaultCompactionInterval), true
	case sink.MaintenanceSnapshotExpiry:
		if cfg.SnapshotExpiry == nil {
			return 0, false
		}
		return durationOrDefault(cfg.SnapshotExpiry.Interval, spec.DefaultSnapshotExpiryInterval), true
	case sink.MaintenanceOrphanCleanup:
		if cfg.OrphanCleanup == nil {
			return 0, false
		}
		return durationOrDefault(cfg.OrphanCleanup.Interval, spec.DefaultOrphanCleanupInterval), true
	}
	return 0, false
}

// CheckInterval is how often a scheduler should look for due operations:
// half the shortest enabled interval, so a due operation runs within its
// own interval rather than waiting for a coarse fixed poll. Floor of one
// second keeps a very short interval (tests, demos) from spinning.
func CheckInterval(cfg *spec.Maintenance) time.Duration {
	if cfg == nil {
		return time.Second
	}
	min := time.Duration(0)
	for _, op := range opsInOrder {
		if interval, enabled := intervalFor(cfg, op); enabled {
			if min == 0 || interval < min {
				min = interval
			}
		}
	}
	if min == 0 {
		return time.Second
	}
	if half := min / 2; half > time.Second {
		return half
	}
	return time.Second
}

// RunLoop runs maintenance for one table in-process until ctx is done: on
// each check tick it computes the due operations and runs them through the
// Maintainer. This is the collapsed runner's mode; the distributed
// coordinator instead provisions an ephemeral maintenance worker per table
// and pushes the pass to it, so no long-lived maintenance goroutine lives in
// the coordinator.
func RunLoop(ctx context.Context, m sink.Maintainer, table string, cfg *spec.Maintenance, sched *Schedule, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(CheckInterval(cfg))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			due := sched.Due(table, cfg, now)
			if len(due) == 0 {
				continue
			}
			if err := m.RunOnce(ctx, due); err != nil {
				log.Warn("maintenance: run failed", "table", table, "ops", due, "err", err)
				continue
			}
			sched.MarkRun(table, due, time.Now())
		}
	}
}

// durationOrDefault parses a spec duration string, falling back to def when
// empty or malformed. Validate rejects malformed strings before a spec
// reaches here, so the fallback is defense, not the primary path.
func durationOrDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
