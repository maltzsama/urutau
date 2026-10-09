package worker

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A snapshot chunk travels the worker's commit path as Arrow, and every step
// used to copy it: a window close took every row it kept, even all of them,
// and the batcher merged its pending batches pairwise, copying the growing
// prefix again for every batch. The full profile's dying events worker held
// ~373 MB of such copies against 5 MB in the Parquet writer (#448). A step
// that keeps its input whole now passes it on, and merging copies each batch
// once.

// takeRows returns the rows of rec at idx as an owned record. Every row in
// order is rec itself, retained, not a copy.
func takeRows(rec arrow.RecordBatch, idx []int32) (arrow.RecordBatch, error) {
	if everyRow(idx, rec.NumRows()) {
		rec.Retain()
		return rec, nil
	}
	ib := array.NewInt32Builder(memory.DefaultAllocator)
	for _, v := range idx {
		ib.Append(v)
	}
	idxArr := ib.NewInt32Array()
	ib.Release()
	defer idxArr.Release()

	cols := make([]arrow.Array, rec.NumCols())
	for i := range int(rec.NumCols()) {
		t, err := compute.TakeArray(context.Background(), rec.Column(i), idxArr)
		if err != nil {
			for j := range i {
				cols[j].Release()
			}
			return nil, err
		}
		cols[i] = t
	}
	newRec := array.NewRecordBatch(rec.Schema(), cols, int64(len(idx)))
	for _, c := range cols {
		c.Release()
	}
	return newRec, nil
}

// everyRow reports whether idx is 0..n-1 in order.
func everyRow(idx []int32, n int64) bool {
	if int64(len(idx)) != n {
		return false
	}
	for i, v := range idx {
		if v != int32(i) {
			return false
		}
	}
	return true
}
