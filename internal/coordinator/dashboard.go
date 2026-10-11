package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/maltzsama/urutau/internal/dashboard"
	"github.com/maltzsama/urutau/internal/version"
	"github.com/maltzsama/urutau/spec"
)

// errOperatorCancel marks a run terminated by the dashboard's Cancel action —
// distinct from a crashloop so the terminal event says so.
var errOperatorCancel = errors.New("coordinator: cancelled by operator")

// dashState adapts the coordinator to dashboard.State. A separate type so the
// dashboard's method names never collide with the coordinator's own.
type dashState struct{ c *Coordinator }

// pumpHealthWindow is how long with no pump heartbeat is treated as wedged for
// the liveness probe. The pump ticks every cycleCheckEvery (20ms), so this is
// generous (issue #601).
const pumpHealthWindow = 30 * time.Second

// Ready implements dashboard.Prober: routing is published and the stream pump
// is running (issue #601).
func (s dashState) Ready() bool { return s.c.readiness.Load() }

// Healthy implements dashboard.Prober: the pump signalled life within the
// window. A coordinator wedged before the pump starts (lastPump==0) or with a
// stalled pump reads unhealthy, so the liveness probe restarts it (issue #601).
func (s dashState) Healthy() bool {
	ns := s.c.lastPump.Load()
	return ns != 0 && time.Since(time.Unix(0, ns)) < pumpHealthWindow
}

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
	c.mu.Lock()
	workers := len(c.workers)
	tables := len(c.tablesOrSpec())
	c.mu.Unlock()
	return dashboard.PipelineSummary{
		Pipeline:           c.cfg.Spec.Pipeline,
		RunID:              c.runID,
		SourceKind:         c.cfg.Spec.Source.Kind,
		SinkType:           c.cfg.Spec.Sink.Type,
		Tables:             tables,
		Workers:            workers,
		StartedAt:          c.startedAt.UTC().Format(time.RFC3339),
		UptimeS:            int64(time.Since(c.startedAt).Seconds()),
		Status:             status,
		SnapshotActive:     c.snapshotActive.Load(),
		MaintenanceEnabled: c.cfg.Spec.Sink.MaintenanceEnabled(),
		Version:            version.Version,
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
	c.mu.Lock()
	tables := c.tablesOrSpec()
	c.mu.Unlock()
	positions := c.tablePositions()
	// Pending before statsMu, for the same reason publishLag does it: it
	// takes other locks, and statsMu must not be held across them.
	pending := make(map[string]int, len(tables))
	for _, t := range tables {
		pending[t.Target] = c.tablePending(t.Target)
	}
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	out := make([]dashboard.TableStatus, 0, len(tables))
	for _, t := range tables {
		st := dashboard.TableStatus{
			Source:    t.Source,
			Target:    t.Target,
			WriteMode: writeModeLabel(t.WriteMode),
			Position:  positions[t.Target],
		}
		if ts := c.tableStats[t.Target]; ts != nil {
			st.Commits = ts.commits
			st.RowsTotal = ts.rows
			st.RowsRate = ts.rowsRate
			st.EqualityDeletes = ts.deletes
			st.CommitFailures = ts.commitFailures
			st.CommitLatencyMs = ts.commitLatencyMs
			st.DeletesDropped = ts.deletesDropped
			st.SnapshotProgress = ts.snapshotProgress
			if !ts.lastCommit.IsZero() && pending[t.Target] > 0 {
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
			InflightBytes: c.budget.InFlight(sn.name),
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
		return fmt.Errorf("no worker %q: %w", name, dashboard.ErrUnknownWorker)
	}
	// A reset cancels the session and bumps the epoch. The in-flight batches
	// are redelivered on reconnect (issue #235): re-applied idempotently on
	// an upsert table, but appended twice on an append table if one was
	// committed before its ack was lost. The supervisor refuses the same
	// reset there, so the dashboard must too (issues #205, #461).
	if n := c.inFlight(name); n > 0 && c.workerAppends(name) {
		return fmt.Errorf("coordinator: worker %s has %d in-flight batch(es) on an append table — a restart would append them twice; wait for it to drain: %w", name, n, dashboard.ErrRestartConflict)
	}
	c.supervisor.recordReset(name, time.Now(), supervisionConfig(c.cfg).ResetWindow)
	c.resetWorker(w)
	return nil
}

// pushDashState marks the dashboard state dirty. The actual snapshot + publish
// runs on dashPushLoop's ticker, so the per-ack hot path never builds the
// state (issue #591). Call it after a state mutation, OUTSIDE any statsMu
// lock.
func (c *Coordinator) pushDashState() {
	if c.dash != nil {
		c.dashDirty.Store(true)
	}
}

// dashPushLoop publishes the dashboard state at most ~4 Hz, coalescing the
// per-ack dirty marks into one build (issue #591).
func (c *Coordinator) dashPushLoop(ctx context.Context) {
	if c.dash == nil {
		return
	}
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.dashDirty.Swap(false) {
				c.dash.PublishState()
			}
		}
	}
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
