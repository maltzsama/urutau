package staged

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/gate"
)

// A staged table (several owners on a staging sink) commits one Iceberg
// snapshot per cycle, and a cycle was one source batch. The source cuts a
// batch at every change of table, so a binlog interleaving tables arrives as
// batches of one or two rows: each became a commit and a data file per owner,
// and the full production-readiness profile's staged tables committed
// 40-100 rows/s against ~500/s of load (#437, #414).
//
// Consecutive batches of a staged table are held in a per-table accumulator
// and sent as one cycle — concatenated in source order, so the cycle is
// exactly what a bigger source batch would have been. Merging whole cycles
// into one commit instead would be wrong: every file of one snapshot shares
// its sequence number, and an equality delete never applies to rows of its
// own sequence.
//
// The accumulator is a group commit: it sends at once while none of the
// table's cycles is pending, and grows while one is being committed — up to
// the cycle's row, byte or age bound (Bounds), whichever comes first.
//
// Ordering is the invariant. A table's accumulated batches are older than
// anything a DBLog window gates or a re-slice pauses after them, so they are
// sent first: the caller sends them under the gate's flush lock before a
// window opens to new batches and before a pause holds its first batch, and
// every send here runs under that lock, like the gate's own drains.

// CoalesceHost is the coordinator as the accumulators see it.
type CoalesceHost interface {
	// Gate is the DBLog window gate: its lock guards the accumulators, and
	// its flush lock orders their sends against its drains.
	Gate() *gate.Gate[*dataplane.Batch]
	// Bounds are the row, byte and age bounds of one cycle.
	Bounds() Bounds
	// Size is a batch's in-memory size, the byte bound's measure.
	Size(b *dataplane.Batch) int64
	// OpenCycles is how many of table's cycles are being committed.
	OpenCycles(table string) int
	// Send sends one batch as one cycle. It takes ownership of the batch.
	Send(ctx context.Context, b *dataplane.Batch) error
}

// Coalescer holds each staged table's batches not sent yet. The zero value
// is ready to use; the map is guarded by the host gate's lock.
type Coalescer struct {
	accum map[string]*Accum
}

// Take takes a live batch of a staged table into its accumulator. A window
// that opened since the caller looked takes the batch into its gate instead.
// It owns the batch either way.
func (c *Coalescer) Take(ctx context.Context, h CoalesceHost, b *dataplane.Batch) error {
	g := h.Gate()
	rows, bytes := b.Record.NumRows(), h.Size(b)
	for {
		g.Lock()
		if key, held := g.OpenKeyLocked(b.Table); held {
			g.AppendLocked(key, b)
			g.Unlock()
			return nil
		}
		if c.accum == nil {
			c.accum = map[string]*Accum{}
		}
		a := c.accum[b.Table]
		if a == nil {
			a = NewAccum(time.Now())
			c.accum[b.Table] = a
		}
		if h.Bounds().Fits(a.Rows, a.Bytes, rows, bytes) {
			a.Add(b, rows, bytes)
			g.Unlock()
			return c.flushIfDue(ctx, h, b.Table)
		}
		g.Unlock()
		// The batch would take the cycle past a bound: send what is held
		// first, so no cycle outgrows it, then take the batch.
		if err := c.Flush(ctx, h, b.Table); err != nil {
			b.Release()
			return err
		}
	}
}

// due reports whether a table's accumulator should be sent now. Callers hold
// the gate's lock.
func (c *Coalescer) due(h CoalesceHost, table string, a *Accum) bool {
	if !a.Holds() {
		return false
	}
	if a.Full(h.Bounds(), time.Now()) {
		return true
	}
	// Nothing of the table is being committed: waiting would only add latency.
	return h.OpenCycles(table) == 0
}

func (c *Coalescer) flushIfDue(ctx context.Context, h CoalesceHost, table string) error {
	g := h.Gate()
	g.Lock()
	due := c.due(h, table, c.accum[table])
	g.Unlock()
	if !due {
		return nil
	}
	return c.Flush(ctx, h, table)
}

// FlushDue sends every accumulator that is due; the pump calls it on its
// tick.
func (c *Coalescer) FlushDue(ctx context.Context, h CoalesceHost) error {
	g := h.Gate()
	g.Lock()
	var due []string
	for table, a := range c.accum {
		if c.due(h, table, a) {
			due = append(due, table)
		}
	}
	g.Unlock()
	for _, table := range due {
		if err := c.Flush(ctx, h, table); err != nil {
			return err
		}
	}
	return nil
}

// Flush sends a table's accumulated batches as one cycle.
func (c *Coalescer) Flush(ctx context.Context, h CoalesceHost, table string) error {
	var err error
	h.Gate().WithFlush(func() {
		err = c.SendLocked(ctx, h, table)
	})
	return err
}

// SendLocked takes and sends a table's accumulator. Caller holds the gate's
// flush lock.
func (c *Coalescer) SendLocked(ctx context.Context, h CoalesceHost, table string) error {
	g := h.Gate()
	g.Lock()
	a := c.accum[table]
	delete(c.accum, table)
	g.Unlock()
	if !a.Holds() {
		return nil
	}
	n := len(a.Batches)
	merged, err := Concat(a.Batches)
	a.Release()
	if err != nil {
		return fmt.Errorf("staged: %s: coalesce %d batches: %w", table, n, err)
	}
	return h.Send(ctx, merged)
}

// Release drops every accumulated batch when the pump ends: nothing sends
// them any more, and the stream replays them from the committed position.
func (c *Coalescer) Release(h CoalesceHost) {
	g := h.Gate()
	g.Lock()
	defer g.Unlock()
	for table, a := range c.accum {
		a.Release()
		delete(c.accum, table)
	}
}

// HoldingLocked returns the tables with accumulated batches. Caller holds
// the gate's lock.
func (c *Coalescer) HoldingLocked() []string {
	var out []string
	for table, a := range c.accum {
		if a.Holds() {
			out = append(out, table)
		}
	}
	return out
}

// SendGrouped sends held batches of one table, in order, concatenated up to
// the cycle's row and byte bounds per send: one held batch per cycle made a
// window's drain hundreds of one-row commits (#437). It releases every batch
// of buf, sent or not. send takes ownership of the merged batch.
func SendGrouped(ctx context.Context, bounds Bounds, size func(*dataplane.Batch) int64, buf []*dataplane.Batch, send func(context.Context, *dataplane.Batch) error) error {
	for len(buf) > 0 {
		n, rows, bytes := 0, int64(0), int64(0)
		for n < len(buf) {
			// Sized once: size walks every buffer of the record.
			addRows, addBytes := buf[n].Record.NumRows(), size(buf[n])
			if !bounds.Fits(rows, bytes, addRows, addBytes) {
				break
			}
			rows += addRows
			bytes += addBytes
			n++
		}
		group := buf[:n]
		buf = buf[n:]
		merged, err := Concat(group)
		for _, b := range group {
			b.Release()
		}
		if err == nil {
			err = send(ctx, merged)
		}
		if err != nil {
			for _, rest := range buf {
				rest.Release()
			}
			return err
		}
	}
	return nil
}
