package transport

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// WithPosition returns rec with every row's __pos replaced by pos. A source
// that emits a transaction split across several records stamps its rows with
// the last SAFE position and calls this on the transaction's FINAL record
// only, so acking a split piece cannot advance the durable checkpoint past
// rows a later piece still owes (#456).
//
// OWNERSHIP: rec is released; the returned record is the caller's.
func WithPosition(rec arrow.RecordBatch, pos string) (arrow.RecordBatch, error) {
	sch := rec.Schema()
	numData := sch.NumFields() - len(WireMetadataFields())
	if numData < 0 {
		return nil, fmt.Errorf("transport: record is not wire schema (%d columns)", sch.NumFields())
	}
	n := int(rec.NumRows())
	cols := make([]arrow.Array, sch.NumFields())
	for j := range cols {
		cols[j] = rec.Column(j)
	}
	b := array.NewStringBuilder(memory.DefaultAllocator)
	for i := 0; i < n; i++ {
		b.Append(pos)
	}
	posArr := b.NewStringArray()
	b.Release()
	cols[numData+1] = posArr
	out := array.NewRecordBatch(sch, cols, int64(n))
	rec.Release()
	posArr.Release()
	return out, nil
}
