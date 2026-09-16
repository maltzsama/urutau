package iceberg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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
	defaultCompactionInterval = 5 * time.Minute
	// Compaction's own MinInputFiles/TargetFileSize defaults come straight
	// from compaction.DefaultConfig() (compactionConfigFrom) rather than
	// being re-declared here — one source of truth for iceberg-go's values.
	defaultSnapshotExpiryInterval = 10 * time.Minute
	defaultSnapshotExpiryMaxAge   = 168 * time.Hour // 7 days
	defaultSnapshotExpiryRetain   = 1
	defaultOrphanCleanupInterval  = time.Hour
	defaultOrphanCleanupOlderThan = 72 * time.Hour // 3 days, iceberg-go's own default

	// maxConsecutiveFailures: after this many consecutive failed runs of
	// ONE operation, that operation logs and stops trying until the next
	// process restart — a persistently broken catalog/warehouse should not
	// spam retries forever, per the issue's "3 consecutive failures" rule.
	// The other two operations are unaffected: each ticker fails
	// independently.
	maxConsecutiveFailures = 3

	// maintainerMaxTries/maintainerBackoff mirror TableWriter's own retry
	// tuning (writer.go's maxTries: 5, backoff: 200ms) — same isRetryableError
	// classification, same backoffDuration jitter.
	maintainerMaxTries = 5
	maintainerBackoff  = 200 * time.Millisecond
)

// Maintainer runs the three Iceberg table-maintenance operations
// (compaction, snapshot expiry, orphan cleanup) for one table on
// independent tickers, following the checkpoint.go pattern: context-
// cancelled, ticker-driven, non-fatal errors logged and looped.
//
// The three operations serialize against EACH OTHER via runMu: compact and
// expireSnapshots are both metadata commits subject to the same
// stale-branch-pointer conflict a concurrent CDC write can trigger, and
// unlike that CDC write (whose duration is bounded by one collapsed batch),
// a compaction pass over many groups can run for minutes. Retrying a short
// backoff against an operation that legitimately takes 15 minutes just
// burns through maintainerMaxTries and then waits out the OTHER ticker's
// full Interval before trying again — for a table with expiry every 5m and
// a compaction run that takes 15m, that is not an isolated collision, it is
// expiry repeatedly failing against the same in-progress compaction. runMu
// removes that cause entirely: only one of the three ever executes at a
// time for this table. It does not, and cannot, prevent a collision with a
// concurrent CDC commit from the table's own TableWriter — that one is still
// handled by the retry loop below, which relies on iceberg-go's own
// rewriteValidator / stale-ref rejection (isRetryableError /
// table.ErrCommitFailed), because urutau does not own that writer's
// lifecycle the way it owns its own three tickers.
type Maintainer struct {
	cat   catalog.Catalog
	ident table.Identifier
	cfg   spec.Maintenance
	log   *slog.Logger
	// runMu serializes compact/expireSnapshots/cleanOrphans against each
	// other for this table. See the type doc for why.
	runMu sync.Mutex
	// currentPosition returns the table's most recently known committed
	// cdc.position, so a compaction commit can carry it forward on the
	// rewrite snapshot instead of relying on walkBackPosition's fallback
	// (issue #96 finding). Reads a live value on every compaction tick
	// rather than a value captured at construction, since the position
	// advances continuously while the Maintainer runs for the life of the
	// pipeline. May return "" (nothing committed yet, or the caller does
	// not track it) — a compaction commit with no position property is
	// still correct, just one that must fall back to the walk-back if this
	// snapshot becomes the newest one lacking the property.
	currentPosition func() string
	metrics         sink.MaintainerMetrics

	compactFailures int
	expireFailures  int
	orphanFailures  int
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

// Run starts the three maintenance tickers and blocks until ctx is
// cancelled. Each disabled or nil sub-config's ticker is simply never
// started — a table with only SnapshotExpiry configured never touches
// compaction or orphan cleanup.
//
// Per-operation failures are logged and looped (never fatal to the process:
// a maintenance failure must not take down the CDC pipeline it runs
// alongside), except after maxConsecutiveFailures consecutive failures of
// the SAME operation, which then stops its own ticker and logs a warning —
// the other two operations are unaffected.
func (m *Maintainer) Run(ctx context.Context) {
	if !m.cfg.Enabled {
		return
	}
	var wg sync.WaitGroup
	if c := m.cfg.Compaction; c != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.runTicker(ctx, "compaction", durationOr(c.Interval, defaultCompactionInterval), m.compact, &m.compactFailures)
		}()
	}
	if e := m.cfg.SnapshotExpiry; e != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.runTicker(ctx, "snapshot_expiry", durationOr(e.Interval, defaultSnapshotExpiryInterval), m.expireSnapshots, &m.expireFailures)
		}()
	}
	if o := m.cfg.OrphanCleanup; o != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.runTicker(ctx, "orphan_cleanup", durationOr(o.Interval, defaultOrphanCleanupInterval), m.cleanOrphans, &m.orphanFailures)
		}()
	}
	wg.Wait()
}

// runTicker is the shared ticker loop: fire on interval, run once, track
// consecutive failures, stop this ticker (not the others) after
// maxConsecutiveFailures. Mirrors checkpoint.run's ctx.Done()/ticker.C
// select and its "log and continue" error handling.
//
// A tick that fires while another of this Maintainer's operations holds
// runMu WAITS for it (queued, not a failed attempt) — see the Maintainer
// type doc. The wait itself is cancellable: lockOrDone below returns early
// on ctx.Done() instead of blocking past shutdown, at the cost of a leaked
// goroutine still waiting on the mutex in the background (acceptable: the
// process is shutting down and Go reclaims it on exit).
func (m *Maintainer) runTicker(ctx context.Context, op string, interval time.Duration, run func(context.Context) error, failures *int) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.lockOrDone(ctx) {
				return // ctx cancelled while queued behind another operation
			}
			err := run(ctx)
			m.runMu.Unlock()
			if err != nil {
				*failures++
				m.log.Warn("iceberg maintenance: run failed", "op", op, "table", m.ident, "attempt", *failures, "err", err)
				if *failures >= maxConsecutiveFailures {
					m.log.Warn("iceberg maintenance: stopping after consecutive failures", "op", op, "table", m.ident, "failures", *failures)
					return
				}
				continue
			}
			*failures = 0
		}
	}
}

// lockOrDone acquires runMu, but gives up and returns false if ctx is
// cancelled first — sync.Mutex has no context-aware Lock, so this races a
// blocking Lock() (on a background goroutine, since it cannot be
// interrupted once started) against ctx.Done(). On cancellation the
// background goroutine is left to acquire the mutex whenever the operation
// holding it finishes and then immediately unlock; harmless; by then the
// process is shutting down.
func (m *Maintainer) lockOrDone(ctx context.Context) bool {
	acquired := make(chan struct{})
	go func() {
		m.runMu.Lock()
		close(acquired)
	}()
	select {
	case <-acquired:
		return true
	case <-ctx.Done():
		go func() {
			<-acquired
			m.runMu.Unlock()
		}()
		return false
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
// runMu already rules out a collision with this table's OWN expireSnapshots
// or cleanOrphans (runTicker queues behind them instead), so what this
// retry actually guards against is a concurrent CDC commit from the
// table's TableWriter — a source urutau does not serialize against, since
// that writer's lifecycle belongs to the worker, not the Maintainer.
// Re-planning from scratch on every attempt (not just re-committing) is
// deliberate: a stale plan's file paths may no longer exist after the
// winner of the race committed.
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
			return err
		}
		lastErr = err
	}
	if m.metrics != nil {
		m.metrics.CompactionRun(identString(m.ident), 0, 0, 0, 0, lastErr)
	}
	return fmt.Errorf("%w: compaction on %v: %v", ErrCommitExhausted, m.ident, lastErr)
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

	compCfg := compactionConfigFrom(m.cfg.Compaction)
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
			props["cdc.position"] = pos
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
// Retried on the same terms as compact: runMu already rules out a race
// against this table's own compaction, so the retry here is for a
// concurrent CDC commit racing the branch pointer this transaction was
// built from — see compact's doc comment.
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
			return err
		}
		lastErr = err
	}
	if m.metrics != nil {
		m.metrics.SnapshotExpiryRun(identString(m.ident), 0, lastErr)
	}
	return fmt.Errorf("%w: snapshot expiry on %v: %v", ErrCommitExhausted, m.ident, lastErr)
}

func (m *Maintainer) expireSnapshotsOnce(ctx context.Context) error {
	e := m.cfg.SnapshotExpiry
	tbl, err := m.cat.LoadTable(ctx, m.ident)
	if err != nil {
		return fmt.Errorf("iceberg maintenance: snapshot expiry: load table: %w", err)
	}

	before := len(tbl.Metadata().Snapshots())

	txn := tbl.NewTransaction()
	if err := txn.ExpireSnapshots(
		table.WithRetainLast(intOr(e.RetainLast, defaultSnapshotExpiryRetain)),
		table.WithOlderThan(durationOr(e.MaxAge, defaultSnapshotExpiryMaxAge)),
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
func (m *Maintainer) cleanOrphans(ctx context.Context) error {
	o := m.cfg.OrphanCleanup
	tbl, err := m.cat.LoadTable(ctx, m.ident)
	if err != nil {
		return fmt.Errorf("iceberg maintenance: orphan cleanup: load table: %w", err)
	}

	result, err := tbl.DeleteOrphanFiles(ctx, table.WithFilesOlderThan(durationOr(o.OlderThan, defaultOrphanCleanupOlderThan)))
	if err != nil {
		if m.metrics != nil {
			m.metrics.OrphanCleanupRun(identString(m.ident), 0, 0, err)
		}
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
func compactionConfigFrom(c *spec.CompactionConfig) compaction.Config {
	cfg := compaction.DefaultConfig()
	if c == nil {
		return cfg
	}
	if c.TargetFileSize != "" {
		if n, err := spec.ParseBytes(c.TargetFileSize); err == nil && n > 0 {
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
// empty or malformed. Validate already rejects malformed strings before a
// spec reaches here, so the fallback on parse error is defense, not the
// primary path.
func durationOr(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func intOr(n int, def int) int {
	if n <= 0 {
		return def
	}
	return n
}

// identString renders a table.Identifier ([]string) as its dotted name for
// metric labels and log fields.
func identString(ident table.Identifier) string {
	return strings.Join(ident, ".")
}
