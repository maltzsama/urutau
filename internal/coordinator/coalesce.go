package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
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
// cycleMaxRows rows, cycleMaxBytes bytes or cycleMaxAge, whichever comes
// first. The byte bound matters as much as the row bound: rows carry
// payloads (the full profile's reach 256 KiB), and a 10,000-row batch of them
// OOM-killed a worker.
//
// Ordering is the invariant. A table's accumulated batches are older than
// anything a DBLog window gates or a re-slice pauses after them, so they are
// sent first: opening a window sends them before the window opens to new
// batches (openWindowFlushed), a pause sends them before holding the first
// paused batch (flushAccumBeforePause), and every send runs under
// gateFlushMu, like the gate's own drains.

const (
	defaultCycleMaxRows  = 10000
	defaultCycleMaxBytes = 16 << 20
	defaultCycleMaxAge   = time.Second
	// cycleCheckEvery is how often the pump checks the accumulators it did
	// not flush on arrival.
	cycleCheckEvery = 20 * time.Millisecond
)

// cycleAccum is one staged table's batches not sent yet, oldest first.
type cycleAccum struct {
	batches []*dataplane.Batch
	rows    int64
	bytes   int64
	since   time.Time
}

func (c *Coordinator) cycleMaxRows() int64 {
	if c.cfg.CycleMaxRows > 0 {
		return int64(c.cfg.CycleMaxRows)
	}
	return defaultCycleMaxRows
}

func (c *Coordinator) cycleMaxBytes() int64 {
	if c.cfg.CycleMaxBytes > 0 {
		return c.cfg.CycleMaxBytes
	}
	return defaultCycleMaxBytes
}

// batchBytes is a batch's in-memory size, the byte bound's measure.
func batchBytes(b *dataplane.Batch) int64 { return dpint.BatchBytes(b) }

// fits reports whether a batch of rows/bytes may join one already holding
// accRows/accBytes: always for the first, then within both bounds.
func (c *Coordinator) fits(accRows, accBytes, rows, bytes int64) bool {
	if accRows == 0 {
		return true
	}
	return accRows+rows <= c.cycleMaxRows() && accBytes+bytes <= c.cycleMaxBytes()
}

func (c *Coordinator) cycleMaxAge() time.Duration {
	if c.cfg.CycleMaxAge > 0 {
		return c.cfg.CycleMaxAge
	}
	return defaultCycleMaxAge
}

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
		c.gateMu.Lock()
		if key, held := c.openKeyForTableLocked(b.Table); held {
			c.gateBuf[key] = append(c.gateBuf[key], b)
			c.gateMu.Unlock()
			return true, nil
		}
		if c.accum == nil {
			c.accum = map[string]*cycleAccum{}
		}
		a := c.accum[b.Table]
		if a == nil {
			a = &cycleAccum{since: time.Now()}
			c.accum[b.Table] = a
		}
		if c.fits(a.rows, a.bytes, rows, bytes) {
			a.batches = append(a.batches, b)
			a.rows += rows
			a.bytes += bytes
			c.gateMu.Unlock()
			return true, c.flushAccumIfDue(ctx, b.Table)
		}
		c.gateMu.Unlock()
		// The batch would take the cycle past a bound: send what is held
		// first, so no cycle outgrows it, then take the batch.
		if err := c.flushAccum(ctx, b.Table); err != nil {
			b.Release()
			return true, err
		}
	}
}

// accumDue reports whether a table's accumulator should be sent now.
func (c *Coordinator) accumDue(table string, a *cycleAccum) bool {
	if a == nil || len(a.batches) == 0 {
		return false
	}
	if a.rows >= c.cycleMaxRows() || a.bytes >= c.cycleMaxBytes() || time.Since(a.since) >= c.cycleMaxAge() {
		return true
	}
	// Nothing of the table is being committed: waiting would only add latency.
	return c.staged.openFor(core.TableRef{Target: table}) == 0
}

func (c *Coordinator) flushAccumIfDue(ctx context.Context, table string) error {
	c.gateMu.Lock()
	due := c.accumDue(table, c.accum[table])
	c.gateMu.Unlock()
	if !due {
		return nil
	}
	return c.flushAccum(ctx, table)
}

// flushDueAccums sends every accumulator that is due; the pump calls it on
// its tick.
func (c *Coordinator) flushDueAccums(ctx context.Context) error {
	c.gateMu.Lock()
	var due []string
	for table, a := range c.accum {
		if c.accumDue(table, a) {
			due = append(due, table)
		}
	}
	c.gateMu.Unlock()
	for _, table := range due {
		if err := c.flushAccum(ctx, table); err != nil {
			return err
		}
	}
	return nil
}

// flushAccum sends a table's accumulated batches as one cycle.
func (c *Coordinator) flushAccum(ctx context.Context, table string) error {
	c.gateFlushMu.Lock()
	defer c.gateFlushMu.Unlock()
	return c.sendAccumLocked(ctx, table)
}

// sendAccumLocked takes and sends a table's accumulator. Caller holds
// gateFlushMu.
func (c *Coordinator) sendAccumLocked(ctx context.Context, table string) error {
	c.gateMu.Lock()
	a := c.accum[table]
	delete(c.accum, table)
	c.gateMu.Unlock()
	if a == nil || len(a.batches) == 0 {
		return nil
	}
	merged, err := concatSourceBatches(a.batches)
	for _, b := range a.batches {
		b.Release()
	}
	if err != nil {
		return fmt.Errorf("coordinator: %s: coalesce %d batches: %w", table, len(a.batches), err)
	}
	return c.enqueueBatch(ctx, merged, nil)
}

// openWindowFlushed opens a DBLog window after sending the table's
// accumulated batches, which predate it: sent later, they would reach the
// worker after the window's own events. The window opens first, under
// gateFlushMu, so no new batch joins the accumulator in between and no gate
// drain overtakes the send.
func (c *Coordinator) openWindowFlushed(ctx context.Context, target string, partition int) error {
	c.gateFlushMu.Lock()
	defer c.gateFlushMu.Unlock()
	c.openWindow(target, partition)
	return c.sendAccumLocked(ctx, target)
}

// flushAccumBeforePause sends a table's accumulated batches when the table is
// paused for a re-slice, before its first paused batch is held: they are
// older than everything the pause holds.
func (c *Coordinator) flushAccumBeforePause(ctx context.Context, table string) error {
	c.pausedMu.Lock()
	_, paused := c.paused[table]
	c.pausedMu.Unlock()
	if !paused {
		return nil
	}
	return c.flushAccum(ctx, table)
}

// releaseAccums drops every accumulated batch when the pump ends: nothing
// sends them any more, and the stream replays them from the committed
// position.
func (c *Coordinator) releaseAccums() {
	c.gateMu.Lock()
	defer c.gateMu.Unlock()
	for table, a := range c.accum {
		for _, b := range a.batches {
			b.Release()
		}
		delete(c.accum, table)
	}
}

// concatSourceBatches concatenates one table's source batches, in order, into
// one owned batch: the watermark and position columns are the rows' own, so
// the merged batch is what a bigger source batch would have been. The inputs
// stay the caller's.
func concatSourceBatches(bs []*dataplane.Batch) (*dataplane.Batch, error) {
	first := bs[0]
	if len(bs) == 1 {
		first.Record.Retain()
		return &dataplane.Batch{Table: first.Table, Record: first.Record, Watermark: first.Watermark, Mode: first.Mode}, nil
	}
	schema := first.Record.Schema()
	var rows int64
	for _, b := range bs {
		if !b.Record.Schema().Equal(schema) {
			return nil, fmt.Errorf("schema changed between batches of %s", first.Table)
		}
		rows += b.Record.NumRows()
	}
	cols := make([]arrow.Array, schema.NumFields())
	for i := range cols {
		parts := make([]arrow.Array, len(bs))
		for j, b := range bs {
			parts[j] = b.Record.Column(i)
		}
		col, err := array.Concatenate(parts, memory.DefaultAllocator)
		if err != nil {
			for _, done := range cols[:i] {
				done.Release()
			}
			return nil, err
		}
		cols[i] = col
	}
	rec := array.NewRecordBatch(schema, cols, rows)
	for _, col := range cols {
		col.Release()
	}
	last := bs[len(bs)-1]
	return &dataplane.Batch{Table: first.Table, Record: rec, Watermark: last.Watermark, Mode: first.Mode}, nil
}
