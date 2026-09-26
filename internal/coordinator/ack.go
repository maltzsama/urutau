package coordinator

import (
	"fmt"
	"time"

	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/faultinject"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
)

// recordConfirmed stores a worker's latest durably-committed position and
// recomputes the pipeline-wide minimum. The minimum uses the position's own
// ordering — LSNs and GTID sets are not lexicographically ordered, and a
// wrong minimum would advance the source slot past data still in flight.
//
// Keyed by worker (not table): for a partitioned table the N workers commit
// disjoint key ranges and must each hold their own position; a per-table key
// would let the last acker overwrite the lagging partitions (WK-001 §2.2).
func (c *Coordinator) recordConfirmed(worker string, pos position.Position) {
	if pos == nil {
		return
	}
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	c.confirmed[worker] = pos
}

// confirmedPosition returns the minimum committed position across all
// workers; nil while nothing is durably committed.
//
// MinSafe, not Min: if the committed positions are not mutually comparable
// (never for one source, but a guard), there is no safe minimum — advancing
// the source's retention to an arbitrary one could pass uncommitted data.
// Nil means "nothing provably committed", which holds retention back.
func (c *Coordinator) confirmedPosition() position.Position {
	c.confirmedMu.Lock()
	defer c.confirmedMu.Unlock()
	if len(c.confirmed) == 0 {
		return nil
	}
	vals := make([]position.Position, 0, len(c.confirmed))
	for _, p := range c.confirmed {
		if p == nil {
			// A registered worker with no committed position yet (its boot
			// baseline): nothing is provably committed past the resume point,
			// so hold retention back rather than advance over its data.
			return nil
		}
		vals = append(vals, p)
	}
	best, err := position.MinSafe(vals)
	if err != nil {
		c.log.Warn("coordinator: incomparable committed positions; not advancing retention", "err", err)
		return nil
	}
	return best
}

// onHello processes a worker's ready Hello: it carries the phase and the
// committed positions the worker read from Iceberg. A Hello with a stale
// epoch is a zombie from a superseded generation — reject it (design §5.5).
func (c *Coordinator) onHello(worker string, h *pb.Hello) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.workers[worker]
	if !ok {
		c.log.Warn("coordinator: Hello for unknown worker", "worker", worker)
		return
	}
	if h.Epoch != w.epoch {
		c.log.Warn("coordinator: stale Hello epoch", "worker", worker, "have", w.epoch, "got", h.Epoch)
		return
	}
	w.committed = h.Committed
	c.log.Info("worker hello", "worker", worker, "phase", h.Phase.String(),
		"committed", len(h.Committed))
}

// onSchemaDrift records a worker's schema-drift report, so the log and the
// event trail surface WHY a worker is stopping instead of a bare
// CrashLoopBackOff (issue #272).
func (c *Coordinator) onSchemaDrift(worker string, d *pb.SchemaDrift) {
	if d == nil {
		return
	}
	c.log.Error("coordinator: worker schema drift",
		"worker", worker, "table", d.Table, "column", d.Column, "kind", d.Kind)
	c.emitLog(eventlog.KindSchemaDrift, map[string]any{
		"worker": worker, "table": d.Table, "column": d.Column, "kind": d.Kind,
	})
}

// onAck advances the worker's position index: every head batch the commit
// covers leaves the flight window and returns its bytes to the budget.
func (c *Coordinator) onAck(worker string, ack *pb.Ack) {
	c.supervisor.noteAck(worker, time.Now())
	pos, err := c.src.ParsePosition(ack.Position)
	if err != nil {
		// The position format is a shared contract; a worker that cannot
		// produce a valid one is broken, and the ack's batch would sit at the
		// head of the index forever, leaking its budget charge with no visible
		// error (issue #210). Terminate for replay instead of continuing.
		c.log.Warn("coordinator: ack position", "worker", worker, "err", err)
		c.fail(fmt.Errorf("coordinator: worker %s: unparsable ack position %q: %w", worker, ack.Position, err))
		return
	}
	faultinject.At(faultinject.CoordinatorAckBeforeRecord,
		"table", ack.Table, "worker", worker, "position", ack.Position)
	idx := c.indexOf(worker)
	if idx == nil {
		// The worker's index is gone (unregistered on a rolled-back scale):
		// nothing to truncate, and it holds no budget this ack would free.
		return
	}
	freed, freedOversized, popped := idx.truncate(ack.Table, pos)
	if freed > 0 {
		c.budget.release(worker, freed)
	}
	if freedOversized {
		c.budget.clearOversized(worker)
	}
	// The ack is durable evidence: the popped batches can leave the sent list
	// and need no redelivery (issue #235). c.workers is immutable after boot.
	if w := c.workers[worker]; w != nil {
		w.dropSent(popped)
	}
	// The ack is evidence of a durable commit: record it and recompute the
	// pipeline-wide minimum the source's retention may advance to. Keyed by
	// worker, so a partitioned table's N partitions each hold their own
	// position and the minimum is the lagging one (WK-001 §2.2).
	//
	// EXCEPT on a staged table, where the ack precedes the coordinator's
	// CommitStaged: there the confirmed position advances from
	// commitStagedCycle, after the cycle is durable, so source retention
	// never passes data the cycle still owes (WK-001 §2.2/F2). The ack still
	// released the budget above.
	if !c.isStagedTable(ack.Table) {
		c.recordConfirmed(worker, pos)
	}
	var commitLatencyMs float64
	if d := ack.GetCommitDuration(); d != nil {
		commitLatencyMs = float64(d.AsDuration().Milliseconds())
	}
	c.recordTableStats(worker, ack.Table, int64(ack.Rows), int64(ack.Deletes), commitLatencyMs, time.Now())
	if c.metrics != nil {
		c.metrics.InflightBytes.WithLabelValues(worker).Set(float64(c.budget.inFlight(worker)))
		c.metrics.CommitsTotal.WithLabelValues(ack.Table).Inc()
	}
	c.log.Info("worker ack", "worker", worker, "table", ack.Table,
		"rows", ack.Rows, "position", ack.Position, "inflight", c.budget.inFlight(worker))
	// The audit trail upload is a synchronous S3 put; on the ack hot path a
	// slow endpoint would delay budget release and trip the supervisor's
	// stale-ack resets (audit #15). Fire it and forget.
	go func() {
		if err := c.emit(eventlog.KindCommit, map[string]any{
			"worker":   worker,
			"table":    ack.Table,
			"rows":     ack.Rows,
			"deletes":  ack.Deletes,
			"position": ack.Position,
		}); err != nil {
			c.log.Warn("coordinator: eventlog emit", "err", err)
		}
	}()
}
