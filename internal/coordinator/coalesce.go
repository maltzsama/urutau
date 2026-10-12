package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/columnar"
	"github.com/maltzsama/urutau/internal/staged"
)

// A staged table (several owners on a staging sink) commits one Iceberg
// snapshot per cycle, and a cycle was one source batch. The source cuts a
// batch at every change of table, so a binlog interleaving tables arrives as
// batches of one or two rows: each became a commit and a data file per owner,
// and the full production-readiness profile's staged tables committed
// 40-100 rows/s against ~500/s of load (#437, #414).
//
// Consecutive batches of a staged table are now held in a per-table
// accumulator and sent as one cycle — concatenated in source order, so the
// cycle is exactly what a bigger source batch would have been. Merging whole
// cycles into one commit instead would be wrong: every file of one snapshot
// shares its sequence number, and an equality delete never applies to rows of
// its own sequence.
//
// The accumulator is a group commit: it sends at once while none of the
// table's cycles is pending, and grows while one is being committed — up to
// the cycle's row, byte or age bound (staged.Bounds), whichever comes first.
//
// Ordering is the invariant. A table's accumulated batches are older than
// anything a DBLog window gates or a re-slice pauses after them, so they are
// sent first: opening a window sends them before the window opens to new
// batches (openWindowFlushed), a pause sends them before holding the first
// paused batch (flushAccumBeforePause), and every send runs under
// gateFlushMu, like the gate's own drains.

// cycleCheckEvery is how often the pump checks the accumulators it did not
// flush on arrival.
const cycleCheckEvery = 20 * time.Millisecond

// cycleBounds are the row, byte and age bounds of one cycle.
func (c *Coordinator) cycleBounds() staged.Bounds {
	return staged.NewBounds(c.cfg.CycleMaxRows, c.cfg.CycleMaxBytes, c.cfg.CycleMaxAge)
}

// batchBytes is a batch's in-memory size, the byte bound's measure.
func batchBytes(b *dataplane.Batch) int64 { return columnar.BatchBytes(b) }

// accumulate takes a live batch of a staged table into its accumulator and
// reports whether it did. A batch of any other table is not taken: it is
// enqueued on its own, as before. A window that opened since gateHold looked
// takes the batch into its gate instead.
func (c *Coordinator) accumulate(ctx context.Context, b *dataplane.Batch) (bool, error) {
	if b.Record == nil || !c.isStagedTable(b.Table) {
		return false, nil
	}
	rows, bytes := b.Record.NumRows(), batchBytes(b)
	for {
		c.gate.Lock()
		if key, held := c.gate.OpenKeyLocked(b.Table); held {
			c.gate.AppendLocked(key, b)
			c.gate.Unlock()
			return true, nil
		}
		if c.accum == nil {
			c.accum = map[string]*staged.Accum{}
		}
		a := c.accum[b.Table]
		if a == nil {
			a = staged.NewAccum(time.Now())
			c.accum[b.Table] = a
		}
		if c.cycleBounds().Fits(a.Rows, a.Bytes, rows, bytes) {
			a.Add(b, rows, bytes)
			c.gate.Unlock()
			return true, c.flushAccumIfDue(ctx, b.Table)
		}
		c.gate.Unlock()
		// The batch would take the cycle past a bound: send what is held
		// first, so no cycle outgrows it, then take the batch.
		if err := c.flushAccum(ctx, b.Table); err != nil {
			b.Release()
			return true, err
		}
	}
}

// accumDue reports whether a table's accumulator should be sent now.
func (c *Coordinator) accumDue(table string, a *staged.Accum) bool {
	if !a.Holds() {
		return false
	}
	if a.Full(c.cycleBounds(), time.Now()) {
		return true
	}
	// Nothing of the table is being committed: waiting would only add latency.
	return c.staged.OpenFor(core.TableRef{Target: table}) == 0
}

func (c *Coordinator) flushAccumIfDue(ctx context.Context, table string) error {
	c.gate.Lock()
	due := c.accumDue(table, c.accum[table])
	c.gate.Unlock()
	if !due {
		return nil
	}
	return c.flushAccum(ctx, table)
}

// flushDueAccums sends every accumulator that is due; the pump calls it on
// its tick.
func (c *Coordinator) flushDueAccums(ctx context.Context) error {
	c.gate.Lock()
	var due []string
	for table, a := range c.accum {
		if c.accumDue(table, a) {
			due = append(due, table)
		}
	}
	c.gate.Unlock()
	for _, table := range due {
		if err := c.flushAccum(ctx, table); err != nil {
			return err
		}
	}
	return nil
}

// flushAccum sends a table's accumulated batches as one cycle.
func (c *Coordinator) flushAccum(ctx context.Context, table string) error {
	var err error
	c.gate.WithFlush(func() {
		err = c.sendAccumLocked(ctx, table)
	})
	return err
}

// sendAccumLocked takes and sends a table's accumulator. Caller holds the
// gate's flush lock.
func (c *Coordinator) sendAccumLocked(ctx context.Context, table string) error {
	c.gate.Lock()
	a := c.accum[table]
	delete(c.accum, table)
	c.gate.Unlock()
	if !a.Holds() {
		return nil
	}
	n := len(a.Batches)
	merged, err := staged.Concat(a.Batches)
	a.Release()
	if err != nil {
		return fmt.Errorf("coordinator: %s: coalesce %d batches: %w", table, n, err)
	}
	return c.enqueueBatch(ctx, merged, nil)
}

// flushAccumBeforePause sends a table's accumulated batches when the table is
// paused for a re-slice, before its first paused batch is held: they are
// older than everything the pause holds.
func (c *Coordinator) flushAccumBeforePause(ctx context.Context, table string) error {
	if !c.pause.Paused(table) {
		return nil
	}
	return c.flushAccum(ctx, table)
}

// releaseAccums drops every accumulated batch when the pump ends: nothing
// sends them any more, and the stream replays them from the committed
// position.
func (c *Coordinator) releaseAccums() {
	c.gate.Lock()
	defer c.gate.Unlock()
	for table, a := range c.accum {
		a.Release()
		delete(c.accum, table)
	}
}
