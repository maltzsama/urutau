package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/maltzsama/urutau/internal/dashboard"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/spec"
)

// errOperatorCancel marks a run terminated by the dashboard's Cancel action —
// distinct from a crashloop so the terminal event says so.
var errOperatorCancel = errors.New("coordinator: cancelled by operator")

// tableStats is the per-table aggregate the dashboard serves. Updated on every
// ack (rows/deletes/commits) and by the worker's metrics report (the series the
// coordinator cannot derive from acks).
type tableStats struct {
	commits          int64
	rows             int64
	deletes          int64
	lastCommit       time.Time
	commitLatencyMs  float64
	commitFailures   int64
	deletesDropped   int64
	snapshotProgress float64
}

// maintStats is the per-table, per-operation maintenance aggregate, folded from
// each maintenance worker's reported MaintenanceResult.
type maintStats struct {
	runs         int64
	lastRun      time.Time
	filesRemoved int64
	filesAdded   int64
	bytesBefore  int64
	bytesAfter   int64
	snapshots    int64
	filesDeleted int64
	bytesFreed   int64
}

// dashState adapts the coordinator to dashboard.State. A separate type so the
// dashboard's method names never collide with the coordinator's own.
type dashState struct{ c *Coordinator }

// Summary implements dashboard.State.
func (s dashState) Summary() dashboard.PipelineSummary {
	c := s.c
	status := "streaming"
	switch {
	case c.snapshotActive.Load():
		status = "snapshotting"
	case c.anyWorkerDown():
		status = "degraded"
	}
	return dashboard.PipelineSummary{
		Pipeline:           c.cfg.Spec.Pipeline,
		RunID:              c.runID,
		SourceKind:         c.cfg.Spec.Source.Kind,
		SinkType:           c.cfg.Spec.Sink.Type,
		Tables:             len(c.cfg.Spec.Tables),
		Workers:            len(c.workers),
		StartedAt:          c.startedAt.UTC().Format(time.RFC3339),
		UptimeS:            int64(time.Since(c.startedAt).Seconds()),
		Status:             status,
		SnapshotActive:     c.snapshotActive.Load(),
		MaintenanceEnabled: c.cfg.Spec.Sink.MaintenanceEnabled(),
	}
}

// anyWorkerDown reports whether any registered worker is not attached. Takes
// only c.mu (the supervisor lock order is supervisor.mu → c.mu, so callers must
// not hold c.mu when probing the supervisor).
func (c *Coordinator) anyWorkerDown() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.workers {
		if !w.attached {
			return true
		}
	}
	return false
}

// Tables implements dashboard.State.
func (s dashState) Tables() []dashboard.TableStatus {
	c := s.c
	if c.cfg.Spec == nil {
		return nil
	}
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	out := make([]dashboard.TableStatus, 0, len(c.cfg.Spec.Tables))
	for _, t := range c.cfg.Spec.Tables {
		st := dashboard.TableStatus{
			Source:    t.Source,
			Target:    t.Target,
			WriteMode: writeModeLabel(t.WriteMode),
		}
		if ts := c.tableStats[t.Target]; ts != nil {
			st.Commits = ts.commits
			st.RowsTotal = ts.rows
			st.EqualityDeletes = ts.deletes
			st.CommitFailures = ts.commitFailures
			st.CommitLatencyMs = ts.commitLatencyMs
			st.DeletesDropped = ts.deletesDropped
			st.SnapshotProgress = ts.snapshotProgress
			if !ts.lastCommit.IsZero() {
				st.LagS = time.Since(ts.lastCommit).Seconds()
			}
		}
		if m := c.maintStats[t.Target]; m != nil {
			st.Maintenance = maintenanceView(m)
		}
		out = append(out, st)
	}
	return out
}

// Workers implements dashboard.State.
func (s dashState) Workers() []dashboard.WorkerStatus {
	c := s.c

	// Snapshot the worker map under c.mu, then probe the supervisor and the
	// stats map after releasing it — the supervisor's lock order is
	// supervisor.mu → c.mu, so holding c.mu while locking the supervisor would
	// invert it.
	type snap struct {
		name      string
		attached  bool
		epoch     uint64
		tables    []string
		committed map[string]string
	}
	var snaps []snap
	c.mu.Lock()
	for name, w := range c.workers {
		sn := snap{name: name, attached: w.attached, epoch: w.epoch, committed: w.committed}
		for _, ref := range w.refs {
			sn.tables = append(sn.tables, ref.Target)
		}
		snaps = append(snaps, sn)
	}
	c.mu.Unlock()

	c.statsMu.Lock()
	lastAck := make(map[string]time.Time, len(c.lastAck))
	for k, v := range c.lastAck {
		lastAck[k] = v
	}
	c.statsMu.Unlock()

	out := make([]dashboard.WorkerStatus, 0, len(snaps))
	for _, sn := range snaps {
		status := "detached"
		switch {
		case sn.attached:
			status = "attached"
		case c.supervisor.isPending(sn.name):
			status = "pending"
		}
		ws := dashboard.WorkerStatus{
			Name:          sn.name,
			Status:        status,
			Epoch:         sn.epoch,
			Tables:        sn.tables,
			InflightBytes: c.budget.inFlight(sn.name),
			Committed:     sn.committed,
		}
		if t, ok := lastAck[sn.name]; ok {
			ws.LastAckS = time.Since(t).Seconds()
		}
		out = append(out, ws)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Cancel implements dashboard.State: drain every worker, then terminate the run.
func (s dashState) Cancel() error {
	s.c.gracefulShutdown()
	select {
	case s.c.terminate <- errOperatorCancel:
	default:
	}
	return nil
}

// RestartWorker implements dashboard.State: reset one worker's session, with
// the reset-window bookkeeping the supervisor's own resets use.
func (s dashState) RestartWorker(name string) error {
	c := s.c
	c.mu.Lock()
	w, ok := c.workers[name]
	c.mu.Unlock()
	if !ok {
		return fmt.Errorf("no worker %q", name)
	}
	c.supervisor.recordReset(name, time.Now(), supervisionConfig(c.cfg).ResetWindow)
	c.resetWorker(w)
	return nil
}

// pushDashState pushes the current pipeline/tables/workers to the dashboard's
// SSE subscribers. Call it after a state mutation, OUTSIDE any statsMu lock:
// PublishState reads back through State, which takes that lock.
func (c *Coordinator) pushDashState() {
	if c.dash != nil {
		c.dash.PublishState()
	}
}

// recordTableStats folds one ack into the per-table aggregate. The maps are
// lazily initialized so a zero Coordinator (tests) is safe.
func (c *Coordinator) recordTableStats(worker, table string, rows, deletes int64, commitLatencyMs float64, at time.Time) {
	c.statsMu.Lock()
	if c.tableStats == nil {
		c.tableStats = map[string]*tableStats{}
	}
	if c.lastAck == nil {
		c.lastAck = map[string]time.Time{}
	}
	ts := c.tableStats[table]
	if ts == nil {
		ts = &tableStats{}
		c.tableStats[table] = ts
	}
	ts.commits++
	ts.rows += rows
	ts.deletes += deletes
	ts.commitLatencyMs = commitLatencyMs
	ts.lastCommit = at
	c.lastAck[worker] = at
	c.statsMu.Unlock()
	c.pushDashState()
}

// recordMaintStats folds one maintenance operation result into the per-table,
// per-op aggregate.
func (c *Coordinator) recordMaintStats(table, op string, at time.Time, apply func(*maintStats)) {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	if c.maintStats == nil {
		c.maintStats = map[string]map[string]*maintStats{}
	}
	byOp := c.maintStats[table]
	if byOp == nil {
		byOp = map[string]*maintStats{}
		c.maintStats[table] = byOp
	}
	s := byOp[op]
	if s == nil {
		s = &maintStats{}
		byOp[op] = s
	}
	s.runs++
	s.lastRun = at
	apply(s)
}

// onWorkerMetrics folds a worker's reported series into the per-table
// aggregate the dashboard serves. It does not duplicate into the coordinator's
// Prometheus registry: the worker's own /metrics is the Prometheus source for
// these series, and the report exists for the dashboard, which cannot scrape
// the worker.
func (c *Coordinator) onWorkerMetrics(rep *pb.WorkerMetricsReport) {
	if rep == nil {
		return
	}
	c.statsMu.Lock()
	if c.tableStats == nil {
		c.tableStats = map[string]*tableStats{}
	}
	for _, tm := range rep.Tables {
		ts := c.tableStats[tm.Table]
		if ts == nil {
			ts = &tableStats{}
			c.tableStats[tm.Table] = ts
		}
		ts.commitFailures = tm.CommitFailures
		ts.deletesDropped = tm.DeletesDropped
		ts.snapshotProgress = tm.SnapshotProgress
	}
	c.statsMu.Unlock()
	c.pushDashState()
}

// maintenanceView renders the per-op aggregate for the API.
func maintenanceView(m map[string]*maintStats) *dashboard.Maintenance {
	out := &dashboard.Maintenance{}
	if s := m["compaction"]; s != nil {
		out.Compaction = &dashboard.CompactionStatus{
			Runs: s.runs, LastRun: tsOrEmpty(s.lastRun),
			FilesRemoved: s.filesRemoved, FilesAdded: s.filesAdded,
			BytesBefore: s.bytesBefore, BytesAfter: s.bytesAfter,
		}
	}
	if s := m["snapshot_expiry"]; s != nil {
		out.Expiry = &dashboard.ExpiryStatus{
			Runs: s.runs, LastRun: tsOrEmpty(s.lastRun), SnapshotsRemoved: s.snapshots,
		}
	}
	if s := m["orphan_cleanup"]; s != nil {
		out.Orphan = &dashboard.OrphanStatus{
			Runs: s.runs, LastRun: tsOrEmpty(s.lastRun),
			FilesDeleted: s.filesDeleted, BytesFreed: s.bytesFreed,
		}
	}
	if out.Compaction == nil && out.Expiry == nil && out.Orphan == nil {
		return nil
	}
	return out
}

// writeModeLabel renders a table's write mode, defaulting to upsert (the
// engine's default) when the spec left it unset.
func writeModeLabel(m spec.WriteMode) string {
	if m == "" {
		return string(spec.WriteModeUpsert)
	}
	return string(m)
}

func tsOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// terminateReason labels a terminal run: an operator cancel is not a crashloop.
func terminateReason(err error) string {
	if errors.Is(err, errOperatorCancel) {
		return "cancelled"
	}
	return "crashloop"
}
