package coordinator

import (
	"errors"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/internal/confirmed"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
)

// The confirmed positions, the last sent positions and the rules over them
// live in internal/confirmed. This file is the coordinator's side: what it
// reads from its registry, indexes and buffers to feed them, and what it
// does with an ack once the package has judged it.

// recordConfirmed stores a worker's latest durably-committed position and
// recomputes the pipeline-wide minimum. The minimum uses the position's own
// ordering — LSNs and GTID sets are not lexicographically ordered, and a
// wrong minimum would advance the source slot past data still in flight.
//
// Keyed by worker (not table): for a partitioned table the N workers commit
// disjoint key ranges and must each hold their own position; a per-table key
// would let the last acker overwrite the lagging partitions (WK-001 §2.2).
func (c *Coordinator) recordConfirmed(worker string, pos position.Position) {
	c.confirmed.Record(worker, pos)
}

// confirmedPosition returns the minimum committed position across the workers
// that still owe work; nil while nothing is provably committed (see
// confirmed.Positions.Min for the rule).
func (c *Coordinator) confirmedPosition() position.Position {
	// The dispatched frontier is read FIRST: enqueueTo makes a batch visible
	// to the queue/index before noteSent records its position, so a frontier
	// read before the owing snapshot can never name a batch that snapshot
	// missed.
	dispatched := c.dispatchedPosition()
	owing := c.workersOwing()
	best, err := c.confirmed.Min(owing, dispatched)
	if err != nil {
		c.log.Warn("coordinator: incomparable committed positions; not advancing retention", "err", err)
		return nil
	}
	return best
}

// workersOwing reports which workers hold data not yet durable (see
// confirmed.Owing): the coordinator reads their queues, their in-flight
// indexes, and the owners of every table with batches buffered before the
// send.
func (c *Coordinator) workersOwing() map[string]bool {
	c.mu.Lock()
	loads := make([]confirmed.Load, 0, len(c.workers))
	for name, w := range c.workers {
		loads = append(loads, confirmed.Load{Name: name, Queued: len(w.queue) > 0})
	}
	c.mu.Unlock()
	var buffered []string
	rt := c.loadRouting()
	for table := range c.bufferedTables() {
		owners, _ := rt.OwnersOf(table)
		for _, w := range owners {
			buffered = append(buffered, w.name)
		}
	}
	return confirmed.Owing(loads, c.index.InFlight, buffered)
}

// bufferedTables returns the tables with decoded-but-unsent data in the
// snapshot gate, the re-slice pause buffer, or the staged accumulator.
func (c *Coordinator) bufferedTables() map[string]struct{} {
	seen := map[string]struct{}{}
	if c.gate != nil {
		c.gate.Lock()
		for _, target := range c.gate.BufferedTablesLocked() {
			seen[target] = struct{}{}
		}
		for table, acc := range c.accum {
			if acc.Holds() {
				seen[table] = struct{}{}
			}
		}
		c.gate.Unlock()
	}
	for _, table := range c.pause.HeldTables() {
		seen[table] = struct{}{}
	}
	return seen
}

// dispatchedPosition is the greatest position handed to a worker, the frontier
// the confirmed may advance to when nothing is in flight; nil before any
// dispatch, and nil with a warning when the tables' positions cannot be
// ordered (see confirmed.Sent.Frontier for the rule).
func (c *Coordinator) dispatchedPosition() position.Position {
	// The parser is reached only when something was sent: a coordinator with
	// no dispatch yet may have no source.
	parse := func(s string) (position.Position, error) { return c.src.ParsePosition(s) }
	best, err := c.sent.Frontier(parse, func(table, pos string, err error) {
		// deliverBatch parsed this very string before it was recorded, so a
		// failure here means the source's parser changed its mind. The
		// frontier is advisory, so the table is skipped, not the run ended;
		// but it is said.
		c.log.Debug("coordinator: dispatched position does not parse", "table", table, "pos", pos, "err", err)
	})
	var inc *confirmed.IncomparableError
	if errors.As(err, &inc) {
		c.log.Warn("coordinator: incomparable dispatched positions; not advancing retention past what is confirmed",
			"have", inc.Have.String(), "candidate", inc.Candidate.String())
		return nil
	}
	return best
}

// onAck handles a worker's ack: confirmed.Positions.OnAck releases what the
// commit covers and records the position (or drops a stale generation's ack
// on a staged table); the coordinator then accounts for the commit.
func (c *Coordinator) onAck(worker string, ack *pb.Ack) {
	c.supervisor.NoteAck(worker, time.Now())
	c.lastProgress.Store(time.Now().UnixNano())
	outcome, err := c.confirmed.OnAck(ackHost{c}, worker, confirmed.Ack{
		Table: ack.Table, Position: ack.Position, MarkerID: ack.BatchId, Epoch: ack.Epoch,
	})
	switch outcome {
	case confirmed.AckStaleMarker:
		c.log.Warn("coordinator: stale staged marker ack", "worker", worker, "table", ack.Table, "marker", ack.BatchId, "got", ack.Epoch)
		return
	case confirmed.AckStale:
		c.log.Warn("coordinator: stale staged ack", "worker", worker, "table", ack.Table, "position", ack.Position, "got", ack.Epoch)
		return
	case confirmed.AckUnparsable:
		// Terminate for replay instead of continuing (issue #210).
		c.log.Warn("coordinator: ack position", "worker", worker, "err", err)
		c.fail(fmt.Errorf("coordinator: %w", err))
		return
	case confirmed.AckMarker, confirmed.AckNoWindow:
		return
	}
	var commitLatencyMs float64
	if d := ack.GetCommitDuration(); d != nil {
		commitLatencyMs = float64(d.AsDuration().Milliseconds())
	}
	c.recordTableStats(worker, ack.Table, int64(ack.Rows), int64(ack.Deletes), commitLatencyMs, time.Now())
	if c.metrics != nil {
		c.metrics.InflightBytes.WithLabelValues(worker).Set(float64(c.budget.InFlight(worker)))
		c.metrics.CommitsTotal.WithLabelValues(ack.Table).Inc()
	}
	c.log.Info("worker ack", "worker", worker, "table", ack.Table,
		"rows", ack.Rows, "position", ack.Position, "inflight", c.budget.InFlight(worker))
	// The audit trail upload is a synchronous S3 put; on the ack hot path a
	// slow endpoint would delay budget release and trip the supervisor's
	// stale-ack resets (audit #15). Fire it and forget, bounded (#494).
	c.emitCommit(worker, ack)
}

// ackHost is the coordinator as an ack sees it (confirmed.AckHost).
type ackHost struct{ c *Coordinator }

func (h ackHost) ParsePosition(s string) (position.Position, error) {
	return h.c.src.ParsePosition(s)
}
func (h ackHost) Staged(table string) bool { return h.c.isStagedTable(table) }
func (h ackHost) Superseded(worker string, epoch uint64) bool {
	return h.c.supersededEpoch(worker, epoch)
}
func (h ackHost) ReleaseMarker(worker string, id uint64) { h.c.onMarkerAck(worker, id) }

// Release truncates worker's position index at pos and returns what it freed
// to the flow budget. The popped batches leave the sent list: the ack is
// durable evidence, they need no redelivery (issue #235); the registry is
// read under c.mu (#479).
func (h ackHost) Release(worker, table string, pos position.Position) bool {
	c := h.c
	idx := c.index.Of(worker)
	if idx == nil {
		return false
	}
	freed, freedOversized, popped := idx.Truncate(table, pos)
	c.notifyAck()
	if freed > 0 {
		c.budget.Release(worker, freed)
	}
	if freedOversized {
		c.budget.ClearOversized(worker)
	}
	if w := c.workerFor(worker); w != nil {
		w.dropSent(popped)
	}
	return true
}
