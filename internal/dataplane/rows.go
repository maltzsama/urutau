package dataplane

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// This file holds the pure batch primitives moved out of internal/worker
// (issue #401): they take and return only *dataplane.Batch / Arrow records and
// have no worker state, so the worker, the runner and the distributed worker
// share one implementation.

// LastRowPos returns the __pos of the batch's last row (its commit
// coordinate), or "" for an empty batch.
func LastRowPos(b *dataplane.Batch) string {
	if b.Record == nil || b.Record.NumRows() == 0 {
		return ""
	}
	reader, err := transport.NewBatchReader(b.Record, nil)
	if err != nil {
		return ""
	}
	// A snapshot window's rows carry no position (#468): the batch's is its
	// last positioned row's.
	for i := reader.NumRows() - 1; i >= 0; i-- {
		if pos := reader.Position(i); pos != "" {
			return pos
		}
	}
	return ""
}

// SelectRows returns a new owned batch holding the rows at the given indices,
// with the given mode and position. The input is NOT released.
func SelectRows(b *dataplane.Batch, idx []int32, pos string, mode dataplane.WriteMode, snapState string, snapPending []uint32) (*dataplane.Batch, error) {
	if len(idx) == 0 {
		return nil, nil
	}
	newRec, err := takeRows(b.Record, idx)
	if err != nil {
		return nil, err
	}
	return &dataplane.Batch{
		Table:           b.Table,
		Record:          newRec,
		Watermark:       []byte(pos),
		Mode:            mode,
		SnapshotState:   snapState,
		SnapshotPending: snapPending,
		// Seq is the coordinator cycle key (WK-001 C5): a reconstruction
		// that dropped it sent live batches back as seq-0 snapshot cycles.
		Seq: b.Seq,
		// Staged travels with the cycle key: the reconstruction must not
		// silently downgrade a staged cycle to a direct commit.
		Staged: b.Staged,
	}, nil
}

// EmptyBatch returns a 0-row batch with b's schema, carrying b's Seq and
// watermark. A staged cycle must still deliver a descriptor when its
// sub-batch produced no data — e.g. an append-mode delete with no before
// image: the coordinator expects exactly one delivery per (table, seq), and
// a missing one leaves the cycle open, blocking every cycle behind it in the
// table's send order.
func EmptyBatch(b *dataplane.Batch, mode dataplane.WriteMode) *dataplane.Batch {
	bld := array.NewRecordBuilder(memory.DefaultAllocator, b.Record.Schema())
	rec := bld.NewRecordBatch()
	bld.Release()
	return &dataplane.Batch{
		Table:     b.Table,
		Record:    rec,
		Watermark: b.Watermark,
		// The pipeline's write mode, not the wire batch's: the wire batch
		// carries ModeUnset, and the committer rejects an unset mode.
		Mode:            mode,
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
		Seq:             b.Seq,
		Staged:          b.Staged,
	}
}

// CountOps returns the number of upsert and delete rows in a batch by
// scanning its __op column. Used by OnCommit observers for bookkeeping.
func CountOps(b *dataplane.Batch) (upserts, deletes int) {
	if b == nil || b.Record == nil {
		return 0, 0
	}
	opIdx := colIndex(b.Record.Schema(), "__op")
	if opIdx < 0 {
		return int(b.Record.NumRows()), 0
	}
	opCol, ok := b.Record.Column(opIdx).(*array.Uint8)
	if !ok {
		return int(b.Record.NumRows()), 0
	}
	for i := range opCol.Len() {
		if opCol.Value(i) == uint8(rowchange.OpDelete) {
			deletes++
		} else {
			upserts++
		}
	}
	return upserts, deletes
}

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
