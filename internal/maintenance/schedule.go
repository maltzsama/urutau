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
	"strings"
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
// half the shortest enabled interval, so a due operation runs within its own
// interval rather than waiting for a coarse fixed poll. Floored at one second,
// which keeps a very short interval (tests, demos) from spinning. A disabled
// config (nil, or Enabled false) has no due operations, so it returns the
// floor too, mirroring Due (issue #246).
func CheckInterval(cfg *spec.Maintenance) time.Duration {
	if cfg == nil || !cfg.Enabled {
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
//
// Operations are dispatched one per RunOnce call rather than as one batch,
// and a failure in one does not abandon the rest of the turn. Both halves
// matter, because RunOnce runs a batch in order and returns the first
// error, which says that something failed but not how much of the batch
// committed first:
//
//   - Marking per operation. The operations before a failure really did
//     commit — a compaction rewrite snapshot is on the table whether or not
//     the expiry that followed it succeeded. A batch call can only mark all
//     or none, and marking none leaves them due on the next tick, so with a
//     persistently failing operation the earlier ones re-run every tick
//     instead of once per interval. Dispatching singly costs nothing
//     (RunOnce is sequential either way) and lets each success be recorded
//     before the next operation is attempted.
//
//   - Continuing past a failure. Abandoning the turn starves every
//     operation ordered after the failing one: an operation that never
//     completes is never marked, so it is due on every subsequent tick and
//     blocked on every subsequent tick. A snapshot expiry that keeps
//     failing would stop orphan cleanup from ever running again, which is
//     worse than the redundant work it was meant to avoid.
//
// Continuing is safe because the operations are independent, not a
// pipeline: each reloads the table from the catalog and acts on whatever
// state it finds (see iceberg.Maintainer's compactOnce, expireSnapshotsOnce
// and cleanOrphans). opsInOrder is a preference — do the cheap
// metadata-level work before the expensive physical sweep — not a
// dependency. Orphan cleanup in particular removes only files unreferenced
// by the state it loads, behind its own OlderThan safety window, so a
// skipped expiry means it finds fewer files to delete, never that it
// deletes something it should not have.
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
		case <-ticker.C:
			RunTurn(ctx, m, table, cfg, sched, log, time.Now)
		}
	}
}

// RunTurn runs one maintenance turn: the operations due at clock(), each
// dispatched on its own, with each success recorded at its own completion time
// (clock() again). RunLoop calls it on every tick; it is exported so a
// scheduler driving a virtual clock (tests, and any future non-ticker driver)
// exercises the same bookkeeping rather than a copy of it. clock is time.Now
// in production; tests inject a virtual clock.
//
// See RunLoop's doc comment for why operations are dispatched singly and
// why a failure does not abandon the rest of the turn.
func RunTurn(ctx context.Context, m sink.Maintainer, table string, cfg *spec.Maintenance, sched *Schedule, log *slog.Logger, clock func() time.Time) {
	if log == nil {
		log = slog.Default()
	}
	if clock == nil {
		clock = time.Now
	}
	for _, op := range sched.Due(table, cfg, clock()) {
		if ctx.Err() != nil {
			return
		}
		if err := m.RunOnce(ctx, []sink.MaintenanceOp{op}); err != nil {
			log.Warn("maintenance: run failed", "table", table, "op", op, "err", err)

			continue
		}
		// Record the operation's own completion time, not the turn's start:
		// the collapsed runner and the coordinator must agree (the coordinator
		// marks the report time), and a long operation must not re-fire
		// immediately on the next tick (issue #244).
		sched.MarkRun(table, []sink.MaintenanceOp{op}, clock())
	}
}

// durationOrDefault applies the shared spec duration rule, silently: the
// schedule has no logger, and Validate rejects malformed durations before a
// spec reaches here.
func durationOrDefault(s string, def time.Duration) time.Duration {
	return spec.ParseDurationOrDefault(s, def, nil, "")
}

// WorkerName derives the DNS-1123 name of a table's ephemeral maintenance
// worker from the pipeline and table target ("<pipeline>-<target>-maint"),
// sanitizing the dots a target carries and truncating to Kubernetes'
// 63-character limit.
//
// It lives here, in the seam both sides already import, because two
// packages must agree on it exactly: the coordinator creates and deletes
// Pods under this name, and the operator names the same Pods in the
// coordinator's RBAC Role. A private copy in either package would let the
// Role and the Pods drift apart, and the failure mode is a coordinator that
// cannot clean up after itself.
func WorkerName(pipeline, table string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(pipeline + "-" + table + "-maint") {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	if s == "" {
		s = "urutau-maintenance"
	}
	if len(s) > 63 {
		s = strings.Trim(s[:63], "-")
	}
	return s
}
