package iceberg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// Default tunables, mirrored from the spec doc comments (spec/spec.go).
// Applied here — at the point of use — rather than in spec.Validate, which
// only rejects malformed strings and leaves zero values as "unset."
const (
	// Compaction's own MinInputFiles/TargetFileSize defaults come straight
	// from compaction.DefaultConfig() (compactionConfigFrom) rather than
	// being re-declared here — one source of truth for iceberg-go's values.
	//
	// The per-operation INTERVAL defaults live in spec
	// (spec.DefaultCompactionInterval and friends): the scheduler, not the
	// Maintainer, decides when an operation is due, and the scheduler
	// (runner/coordinator) cannot import this package.
	defaultSnapshotExpiryMaxAge   = 168 * time.Hour // 7 days
	defaultSnapshotExpiryRetain   = 1
	defaultOrphanCleanupOlderThan = 72 * time.Hour // 3 days, iceberg-go's own default

	// maintainerMaxTries/maintainerBackoff deliberately exceed TableWriter's
	// own tuning (maxTries: 5, backoff: 200ms). A maintenance commit races the
	// table's live CDC writer, and a compaction pass over many files runs for
	// minutes — so a conflict is the expected outcome, not an exception, and
	// the budget must span the writer's commit cadence instead of giving up
	// after ~6s. With 8 tries and a 1s base the jittered exponential backoff
	// (capped at 30s) retries for roughly two minutes before giving up. Same
	// isRetryableError classification and backoffDuration jitter as the
	// writer.
	maintainerMaxTries = 8
)

// maintainerBackoff is the maintenance retry base delay (see maintainerMaxTries
// for the budget rationale). It is a var, not a const, so a test can collapse
// the backoff instead of waiting ~two minutes.
var maintainerBackoff = time.Second

// Maintainer executes the three Iceberg table-maintenance operations
// (compaction, snapshot expiry, orphan cleanup) for one table. It is
// one-shot: RunOnce executes the requested operations, in order, and
// returns; the caller owns scheduling (see sink.Maintainer). In the
// distributed engine that caller is the coordinator, which provisions an
// ephemeral maintenance worker per table and pushes the pass to it; in the
// collapsed runner it is an in-process scheduler.
//
// Because RunOnce runs the operations sequentially, they never race each
// other: a compaction pass completes before snapshot expiry begins. That
// matters — compact and expireSnapshots are both metadata commits subject
// to the same stale-branch-pointer conflict, and a compaction pass over
// many groups can run for minutes. What RunOnce does NOT serialize against
// is a concurrent CDC commit from the table's own TableWriter; that is
// handled by each operation's own retry loop, which relies on iceberg-go's
// own rewriteValidator / stale-ref rejection (isRetryableError /
// table.ErrCommitFailed), because urutau does not own that writer's
// lifecycle.
type Maintainer struct {
	cat   catalog.Catalog
	ident table.Identifier
	cfg   spec.Maintenance
	log   *slog.Logger
	// currentPosition returns the table's most recently known committed
	// cdc.position, so a compaction commit can carry it forward on the
	// rewrite snapshot instead of relying on walkBackPosition's fallback
	// (issue #96 finding). Reads a live value on every pass rather than a
	// value captured at construction, since the position advances
	// continuously while the table is being replicated. May return ""
	// (nothing committed yet, or the caller does not track it) — a
	// compaction commit with no position property is still correct, just
	// one that must fall back to the walk-back if this snapshot becomes the
	// newest one lacking the property.
	currentPosition func() string
	metrics         sink.MaintainerMetrics
}

// NewMaintainer builds a Maintainer for one table. currentPosition and
// metrics may be nil (nil currentPosition means compaction never attaches
// cdc.position; nil metrics means no metrics are recorded).
func NewMaintainer(cat catalog.Catalog, ident table.Identifier, cfg spec.Maintenance, log *slog.Logger, currentPosition func() string, metrics sink.MaintainerMetrics) *Maintainer {
	if log == nil {
		log = slog.Default()
	}
	return &Maintainer{cat: cat, ident: ident, cfg: cfg, log: log, currentPosition: currentPosition, metrics: metrics}
}

// RunOnce runs the requested operations once, in the given order, and
// returns the first error. An empty ops slice is a no-op; an operation the
// table's config does not enable is skipped. The caller owns scheduling —
// the runner's in-process scheduler, or the coordinator, which provisions an
// ephemeral maintenance worker per table and pushes the pass to it.
//
// Operations run sequentially, so no locking is needed: compaction creates
// new files and invalidates old ones, snapshot expiry dereferences them,
// orphan cleanup physically removes them — the order the caller passes is
// the order they execute in.
func (m *Maintainer) RunOnce(ctx context.Context, ops []sink.MaintenanceOp) error {
	if !m.cfg.Enabled {
		return nil
	}
	for _, op := range ops {
		if err := m.runOne(ctx, op); err != nil {
			return fmt.Errorf("iceberg maintenance: %s: %w", op, err)
		}
	}
	return nil
}

// runOne dispatches one operation, skipping it when the table's config does
// not enable it (a due op the operator never configured cannot run).
func (m *Maintainer) runOne(ctx context.Context, op sink.MaintenanceOp) error {
	switch op {
	case sink.MaintenanceCompaction:
		if m.cfg.Compaction == nil {
			return nil
		}
		return m.compact(ctx)
	case sink.MaintenanceSnapshotExpiry:
		if m.cfg.SnapshotExpiry == nil {
			return nil
		}
		return m.expireSnapshots(ctx)
	case sink.MaintenanceOrphanCleanup:
		if m.cfg.OrphanCleanup == nil {
			return nil
		}
		return m.cleanOrphans(ctx)
	default:
		return fmt.Errorf("unknown operation %q", op)
	}
}

// compact plans and executes small-file compaction for one tick. A plan
// with no groups (nothing needs compacting) is not an error and not logged
// as a run.
//
// cdc.position is attached to the rewrite snapshot's properties when
// currentPosition is set and returns a non-empty value — the issue #96
// finding: RewriteDataFilesOptions.SnapshotProps flows through to the new
// snapshot's summary exactly like props(pos) does for an ordinary commit,
// so compaction does not have to depend on walkBackPosition's fallback in
// the common case. The walk-back stays as defense for a third-party tool
// that does not know about this property.
//
// Retried like TableWriter's own commits (writer.go's isRetryableError).
// RunOnce runs the operations sequentially, so this retry guards only
// against a concurrent CDC commit from the table's own TableWriter — a
// source urutau does not serialize against, since that writer's lifecycle
// belongs to the worker, not the Maintainer. Re-planning from scratch on
// every attempt (not just re-committing) is deliberate: a stale plan's file
// paths may no longer exist after the winner of the race committed.
func (m *Maintainer) compact(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt < maintainerMaxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(maintainerBackoff, attempt)); err != nil {
				return err
			}
		}
		did, err := m.compactOnce(ctx)
		if err == nil {
			return nil
		}
		if !did && errors.Is(err, errNothingToCompact) {
			return nil // nothing to compact — not a failure, not a metrics event
		}
		if !isRetryableError(err) {
			// A terminal error still counts as a run: the "runs" counter is
			// what makes a stalled maintainer visible, and it must not stay
			// at zero just because every attempt failed terminally.
			if m.metrics != nil {
				m.metrics.CompactionRun(identString(m.ident), 0, 0, 0, 0, err)
			}
			return err
		}
		lastErr = err
	}
	if m.metrics != nil {
		m.metrics.CompactionRun(identString(m.ident), 0, 0, 0, 0, lastErr)
	}
	return fmt.Errorf("%w: compaction on %v: %w", ErrCommitExhausted, m.ident, lastErr)
}

// errNothingToCompact signals compactOnce found no candidate groups — a
// sentinel rather than a bool return, since it must survive compact's
// "was this a real failure" branch alongside every other error.
var errNothingToCompact = errors.New("iceberg maintenance: nothing to compact")

// compactOnce is one load-plan-rewrite-commit attempt. did reports whether
// a rewrite was attempted at all (false + errNothingToCompact means the
// plan was empty, not that the commit failed).
func (m *Maintainer) compactOnce(ctx context.Context) (did bool, err error) {
	tbl, err := m.cat.LoadTable(ctx, m.ident)
	if err != nil {
		return false, fmt.Errorf("iceberg maintenance: compaction: load table: %w", err)
	}

	compCfg := compactionConfigFrom(m.cfg.Compaction, m.log)
	plan, err := compaction.Analyze(ctx, tbl, compCfg)
	if err != nil {
		return false, fmt.Errorf("iceberg maintenance: compaction: analyze: %w", err)
	}
	if len(plan.Groups) == 0 {
		return false, errNothingToCompact
	}

	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes}
	}

	props := iceberg.Properties{}
	if m.currentPosition != nil {
		if pos := m.currentPosition(); pos != "" {
			props[propPosition] = pos
		}
	}

	txn := tbl.NewTransaction()
	result, err := txn.RewriteDataFiles(ctx, groups, table.RewriteDataFilesOptions{SnapshotProps: props})
	if err != nil {
		return true, fmt.Errorf("iceberg maintenance: compaction: rewrite: %w", err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		return true, fmt.Errorf("iceberg maintenance: compaction: commit: %w", err)
	}

	if m.metrics != nil {
		m.metrics.CompactionRun(identString(m.ident), result.RemovedDataFiles, result.AddedDataFiles, result.BytesBefore, result.BytesAfter, nil)
	}
	m.log.Info("iceberg maintenance: compaction", "table", m.ident,
		"removed_files", result.RemovedDataFiles, "added_files", result.AddedDataFiles,
		"bytes_before", result.BytesBefore, "bytes_after", result.BytesAfter)
	return true, nil
}

// expireSnapshots prunes old snapshot history. MaxAge is the operator's
// safety window (spec.SnapshotExpiryConfig doc comment): no snapshot
// younger than it is ever expired, so a crashed pipeline retains enough
// history to recover cdc.position via CommittedPosition's walk-back even
// after the fast-path table property is gone.
//
// Retried on the same terms as compact: RunOnce runs the operations
// sequentially, so the retry here is for a concurrent CDC commit racing the
// branch pointer this transaction was built from — see compact's doc
// comment.
func (m *Maintainer) expireSnapshots(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt < maintainerMaxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(maintainerBackoff, attempt)); err != nil {
				return err
			}
		}
		err := m.expireSnapshotsOnce(ctx)
		if err == nil {
			return nil
		}
		if !isRetryableError(err) {
			if m.metrics != nil {
				m.metrics.SnapshotExpiryRun(identString(m.ident), 0, err)
			}
			return err
		}
		lastErr = err
	}
	if m.metrics != nil {
		m.metrics.SnapshotExpiryRun(identString(m.ident), 0, lastErr)
	}
	return fmt.Errorf("%w: snapshot expiry on %v: %w", ErrCommitExhausted, m.ident, lastErr)
}

// expireSnapshotsOnce runs one snapshot-expiry attempt against the table.
func (m *Maintainer) expireSnapshotsOnce(ctx context.Context) error {
	e := m.cfg.SnapshotExpiry
	tbl, err := m.cat.LoadTable(ctx, m.ident)
	if err != nil {
		return fmt.Errorf("iceberg maintenance: snapshot expiry: load table: %w", err)
	}

	before := len(tbl.Metadata().Snapshots())

	txn := tbl.NewTransaction()
	if err := txn.ExpireSnapshots(
		table.WithRetainLast(intOr(e.RetainLast, defaultSnapshotExpiryRetain, m.log, "retainLast")),
		table.WithOlderThan(durationOr(e.MaxAge, defaultSnapshotExpiryMaxAge, m.log, "maxAge")),
	); err != nil {
		return fmt.Errorf("iceberg maintenance: snapshot expiry: %w", err)
	}
	newTbl, err := txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("iceberg maintenance: snapshot expiry: commit: %w", err)
	}

	removed := before - len(newTbl.Metadata().Snapshots())
	if m.metrics != nil {
		m.metrics.SnapshotExpiryRun(identString(m.ident), removed, nil)
	}
	if removed > 0 {
		m.log.Info("iceberg maintenance: snapshot expiry", "table", m.ident, "removed_snapshots", removed)
	}
	return nil
}

// cleanOrphans deletes unreferenced files from the warehouse. OlderThan is
// iceberg-go's own native safety window (default 72h): a different hazard
// than SnapshotExpiryConfig.MaxAge — this one protects against deleting a
// file a concurrent read or in-flight commit might still reference, not
// against stranding cdc.position recovery.
//
// Retried on the same terms as compact and expireSnapshots: the LoadTable
// competes with a concurrent CDC commit on the metadata pointer, so a
// transient failure gets a short retry rather than skipping the run.
func (m *Maintainer) cleanOrphans(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt < maintainerMaxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(maintainerBackoff, attempt)); err != nil {
				return err
			}
		}
		err := m.cleanOrphansOnce(ctx)
		if err == nil {
			return nil
		}
		if !isRetryableError(err) {
			if m.metrics != nil {
				m.metrics.OrphanCleanupRun(identString(m.ident), 0, 0, err)
			}
			return err
		}
		lastErr = err
	}
	if m.metrics != nil {
		m.metrics.OrphanCleanupRun(identString(m.ident), 0, 0, lastErr)
	}
	return fmt.Errorf("%w: orphan cleanup on %v: %w", ErrCommitExhausted, m.ident, lastErr)
}

// cleanOrphansOnce runs one orphan-cleanup attempt against the table.
func (m *Maintainer) cleanOrphansOnce(ctx context.Context) error {
	o := m.cfg.OrphanCleanup
	tbl, err := m.cat.LoadTable(ctx, m.ident)
	if err != nil {
		return fmt.Errorf("iceberg maintenance: orphan cleanup: load table: %w", err)
	}

	result, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(durationOr(o.OlderThan, defaultOrphanCleanupOlderThan, m.log, "olderThan")))
	if err != nil {
		return fmt.Errorf("iceberg maintenance: orphan cleanup: %w", err)
	}

	if m.metrics != nil {
		m.metrics.OrphanCleanupRun(identString(m.ident), len(result.DeletedFiles), result.TotalSizeBytes, nil)
	}
	if len(result.DeletedFiles) > 0 {
		m.log.Info("iceberg maintenance: orphan cleanup", "table", m.ident, "deleted_files", len(result.DeletedFiles), "bytes_freed", result.TotalSizeBytes)
	}
	return nil
}

// compactionConfigFrom translates the spec's user-facing string fields into
// iceberg-go's compaction.Config, applying urutau's own defaults (matching
// the doc comments on spec.CompactionConfig) where the operator left a
// field unset. TargetFileSize is parsed once here rather than in Validate,
// which only rejects malformed strings.
func compactionConfigFrom(c *spec.CompactionConfig, log *slog.Logger) compaction.Config {
	cfg := compaction.DefaultConfig()
	if c == nil {
		return cfg
	}
	if c.TargetFileSize != "" {
		n, err := spec.ParseBytes(c.TargetFileSize)
		if err != nil || n <= 0 {
			log.Warn("iceberg: ignoring invalid maintenance.compaction.targetFileSize", "value", c.TargetFileSize)
		} else {
			cfg.TargetFileSizeBytes = n
			cfg.MinFileSizeBytes = n * 3 / 4
			cfg.MaxFileSizeBytes = n * 9 / 5
		}
	}
	if c.MinInputFiles > 0 {
		cfg.MinInputFiles = c.MinInputFiles
	}
	return cfg
}

// durationOr parses a spec duration string, falling back to def when s is
// empty. A non-empty but malformed (or non-positive) value is invalid, not
// unset: it is replaced by the default and logged, so a configuration mistake
// is visible. Validate already rejects malformed strings before a spec reaches
// here, so this is defense, not the primary path.
// durationOr resolves a maintenance duration with the shared spec rule,
// warning on an invalid value (the iceberg maintainer has a logger).
func durationOr(s string, def time.Duration, log *slog.Logger, field string) time.Duration {
	return spec.ParseDurationOrDefault(s, def, log, field)
}

// intOr returns n, or def when n is zero (an unset config value). A negative
// value is invalid: it is replaced by the default and logged.
func intOr(n int, def int, log *slog.Logger, field string) int {
	if n == 0 {
		return def
	}
	if n < 0 {
		log.Warn("iceberg: ignoring negative maintenance value", "field", field, "value", n)
		return def
	}
	return n
}

// identString renders a table.Identifier ([]string) as its dotted name for
// metric labels and log fields.
func identString(ident table.Identifier) string {
	return strings.Join(ident, ".")
}
