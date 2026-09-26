package dataplane

// Batch operations the worker applies on its way to the committer (issue
// #401): concatenating, merging and selecting rows of *dataplane.Batch, and
// reading a batch's commit coordinate and op counts. Every function that
// returns a batch returns one that owns its record; the inputs stay the
// caller's.

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

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
	return reader.Position(reader.NumRows() - 1)
}

// ConcatBatches concatenates the row lists of the given batches, in order,
// into one owned batch. The input batches are NOT released. All batches must
// share the same record schema (columns in the same order) — a source that
// emits schema-varying batches (per-drain inference) fails loud here instead
// of corrupting column alignment.
func ConcatBatches(bs []*dataplane.Batch) (*dataplane.Batch, error) {
	if len(bs) == 0 {
		return nil, nil
	}
	first := bs[0].Record.Schema()
	for _, b := range bs[1:] {
		sch := b.Record.Schema()
		if !sameSchema(sch, first) {
			return nil, fmt.Errorf("dataplane: concat: schema mismatch: %s vs %s (source batches must share a stable schema)", colsOf(sch), colsOf(first))
		}
	}
	acc, err := MergeBatches(bs[0], nil, nil)
	if err != nil {
		return nil, err
	}
	for _, b := range bs[1:] {
		next, err := MergeBatches(acc, b, nil)
		acc.Release()
		if err != nil {
			return nil, err
		}
		acc = next
	}
	return acc, nil
}

// sameSchema reports field-for-field equality (names, types, order).
func sameSchema(a, b *arrow.Schema) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i := range a.NumFields() {
		af, bf := a.Field(i), b.Field(i)
		if af.Name != bf.Name || !arrow.TypeEqual(af.Type, bf.Type) {
			return false
		}
	}
	return true
}

func colsOf(s *arrow.Schema) string {
	names := make([]string, s.NumFields())
	for i := range s.NumFields() {
		names[i] = s.Field(i).Name
	}
	return strings.Join(names, ",")
}

// SelectRows returns a new owned batch holding the rows at the given
// indices, with the given mode and position. The input is NOT released.
func SelectRows(b *dataplane.Batch, idx []int32, pos string, mode dataplane.WriteMode, snapState string, snapPending []uint32) (*dataplane.Batch, error) {
	if len(idx) == 0 {
		return nil, nil
	}
	rec := b.Record
	ib := array.NewInt32Builder(memory.DefaultAllocator)
	for _, v := range idx {
		ib.Append(v)
	}
	idxArr := ib.NewInt32Array()
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
	opIdx := -1
	schema := b.Record.Schema()
	for i := range schema.NumFields() {
		if schema.Field(i).Name == "__op" {
			opIdx = i
			break
		}
	}
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

// MergeBatches concatenates two same-schema batches (upserts + deletes)
// into a single batch. Returns nil when both inputs are empty. The caller
// owns the input batches; they are NOT released here.
func MergeBatches(a, b *dataplane.Batch, alloc memory.Allocator) (*dataplane.Batch, error) {
	// Ownership: the returned batch ALWAYS holds its own retained refs.
	// The caller owns a and b and releases them after this call. Never
	// return an input pointer directly — the caller's Release would free
	// the returned batch's record.
	if a == nil || a.Record == nil || a.Record.NumRows() == 0 {
		if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
			return nil, nil
		}
		b.Record.Retain()
		return &dataplane.Batch{Table: b.Table, Record: b.Record, Watermark: b.Watermark,
			Mode: b.Mode, SnapshotState: b.SnapshotState, SnapshotPending: b.SnapshotPending, Seq: b.Seq, Staged: b.Staged}, nil
	}
	if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		a.Record.Retain()
		return &dataplane.Batch{Table: a.Table, Record: a.Record, Watermark: a.Watermark,
			Mode: a.Mode, SnapshotState: a.SnapshotState, SnapshotPending: a.SnapshotPending, Seq: a.Seq, Staged: a.Staged}, nil
	}
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	schema := a.Record.Schema()
	ncols := int(schema.NumFields())
	nrows := a.Record.NumRows() + b.Record.NumRows()
	cols := make([]arrow.Array, ncols)
	for i := range ncols {
		cat, err := array.Concatenate([]arrow.Array{a.Record.Column(i), b.Record.Column(i)}, alloc)
		if err != nil {
			for j := range i {
				cols[j].Release()
			}
			return nil, err
		}
		cols[i] = cat
	}
	rec := array.NewRecordBatch(schema, cols, nrows)
	for _, c := range cols {
		c.Release()
	}
	return &dataplane.Batch{
		Table:           a.Table,
		Record:          rec,
		Watermark:       a.Watermark,
		Mode:            a.Mode,
		SnapshotState:   a.SnapshotState,
		SnapshotPending: a.SnapshotPending,
		Seq:             a.Seq,
		Staged:          a.Staged || b.Staged,
	}, nil
}
