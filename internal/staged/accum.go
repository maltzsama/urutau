package staged

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
)

// The bounds of one cycle when the pipeline sets none.
const (
	DefaultCycleMaxRows  = 10000
	DefaultCycleMaxBytes = 16 << 20
	DefaultCycleMaxAge   = time.Second
)

// Bounds are the limits of one cycle: it is sent at MaxRows rows, MaxBytes
// bytes or MaxAge, whichever comes first. The byte bound matters as much as
// the row bound: rows carry payloads (the full profile's reach 256 KiB), and
// a 10,000-row batch of them OOM-killed a worker.
type Bounds struct {
	MaxRows  int64
	MaxBytes int64
	MaxAge   time.Duration
}

// NewBounds returns the bounds for the configured limits, taking the default
// of each one that is not set.
func NewBounds(maxRows int, maxBytes int64, maxAge time.Duration) Bounds {
	b := Bounds{MaxRows: DefaultCycleMaxRows, MaxBytes: DefaultCycleMaxBytes, MaxAge: DefaultCycleMaxAge}
	if maxRows > 0 {
		b.MaxRows = int64(maxRows)
	}
	if maxBytes > 0 {
		b.MaxBytes = maxBytes
	}
	if maxAge > 0 {
		b.MaxAge = maxAge
	}
	return b
}

// Fits reports whether a batch of rows/bytes may join one already holding
// accRows/accBytes: always for the first, then within both bounds.
func (b Bounds) Fits(accRows, accBytes, rows, bytes int64) bool {
	if accRows == 0 {
		return true
	}
	return accRows+rows <= b.MaxRows && accBytes+bytes <= b.MaxBytes
}

// Accum is one staged table's batches not sent yet, oldest first.
type Accum struct {
	Batches []*dataplane.Batch
	Rows    int64
	Bytes   int64
	Since   time.Time
}

// NewAccum returns an empty accumulator whose age counts from now.
func NewAccum(now time.Time) *Accum { return &Accum{Since: now} }

// Holds reports whether the accumulator has a batch to send.
func (a *Accum) Holds() bool { return a != nil && len(a.Batches) > 0 }

// Add takes a batch of rows/bytes as the accumulator's newest.
func (a *Accum) Add(b *dataplane.Batch, rows, bytes int64) {
	a.Batches = append(a.Batches, b)
	a.Rows += rows
	a.Bytes += bytes
}

// Full reports whether the accumulator reached one of the bounds at now: it
// is sent whatever the table's cycles are doing.
func (a *Accum) Full(b Bounds, now time.Time) bool {
	return a.Rows >= b.MaxRows || a.Bytes >= b.MaxBytes || now.Sub(a.Since) >= b.MaxAge
}

// Release drops every batch the accumulator holds.
func (a *Accum) Release() {
	for _, b := range a.Batches {
		b.Release()
	}
	a.Batches = nil
}

// Concat concatenates one table's source batches, in order, into one owned
// batch: the watermark and position columns are the rows' own, so the merged
// batch is what a bigger source batch would have been. The inputs stay the
// caller's.
func Concat(bs []*dataplane.Batch) (*dataplane.Batch, error) {
	first := bs[0]
	if len(bs) == 1 {
		first.Record.Retain()
		return &dataplane.Batch{Table: first.Table, Record: first.Record, Watermark: first.Watermark, Mode: first.Mode, Partial: first.Partial}, nil
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
	// The merged batch ends where its last one does: partial when that one is.
	return &dataplane.Batch{Table: first.Table, Record: rec, Watermark: last.Watermark, Mode: first.Mode, Partial: last.Partial}, nil
}
