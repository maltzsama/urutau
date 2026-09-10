package dataplane

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Collapse deduplicates by PK, keeping last-occurrence per group in
// first-appearance emission order (CR-069 §3.2).
//
// The encoding MUST be exact: length-prefixed binary concat of encoded
// key columns. Naive concat ("ab"+"c" == "a"+"bc") collides.
//
// Null in a PK column is a source contract violation → error.
func Collapse(ctx context.Context, alloc memory.Allocator, batch *Batch, pkCols []string) (upserts, deletes *Batch, err error) {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		return nil, nil, nil
	}

	nrows := int(batch.Record.NumRows())

	// Resolve PK column indices ONCE before the scan.
	pkIdxs := make([]int, len(pkCols))
	for i, col := range pkCols {
		idx := colIndex(batch.Record.Schema(), col)
		if idx < 0 {
			return nil, nil, fmt.Errorf("dataplane: collapse: PK column %q not found", col)
		}
		pkIdxs[i] = idx
	}

	// 1. Build exact keys — EncodeKey is the authority for null PKs and
	// column validity; indices resolved once (M-9).
	keys := make([][]byte, nrows)
	//allow:rowloop composite-PK hashing: EncodeKey is a type-tagged binary concat, no kernel equivalent.
	for row := range nrows {
		key, err := EncodeKey(batch.Record, row, pkIdxs, pkCols)
		if err != nil {
			return nil, nil, fmt.Errorf("dataplane: collapse: %w", err)
		}
		keys[row] = key
	}

	// 2. Validate __op on the original batch before Take — invalid ops
	// in losing rows would otherwise be silently dropped.
	opIdxOrig := colIndex(batch.Record.Schema(), "__op")
	if opIdxOrig < 0 {
		return nil, nil, fmt.Errorf("dataplane: collapse: __op column not found")
	}
	opColOrig, ok := batch.Record.Column(opIdxOrig).(*array.Uint8)
	if !ok {
		return nil, nil, fmt.Errorf("dataplane: collapse: __op column type %T, want *array.Uint8", batch.Record.Column(opIdxOrig))
	}
	if err := validateOpColumn(opColOrig); err != nil {
		return nil, nil, err
	}

	// 3. Map-based scan: last occurrence wins per group, first-appearance
	// order for emission. O(n), no hashing, no collision risk.
	// groups maps key → winning row (M-9: no groupInfo struct).
	groups := make(map[string]int, nrows)
	var groupOrder []string

	//allow:rowloop last-write-wins grouping by exact key: hash grouping, no kernel gives the row-index map.
	for row, key := range keys {
		k := string(key)
		if _, exists := groups[k]; !exists {
			groupOrder = append(groupOrder, k)
		}
		groups[k] = row // last occurrence always wins
	}

	// 4. The winner index array for Take, in first-appearance order.
	//    groupOrder and its lookups are group-count-sized bookkeeping, not
	//    row data — this range is over the DISTINCT-KEY slice.
	idxBuilder := array.NewInt32Builder(alloc)
	defer idxBuilder.Release()
	//allow:rowloop distinct-key winner list: one entry per group, not per input row.
	for _, k := range groupOrder {
		idxBuilder.Append(int32(groups[k]))
	}
	idxArr := idxBuilder.NewInt32Array()
	defer idxArr.Release()
	nWinners := idxArr.Len()

	// 5. Take each column independently — TakeArray works on flat arrays.
	//    The range is over the COLUMN count (schema-level), not rows.
	ncols := int(batch.Record.NumCols())
	cols := make([]arrow.Array, ncols)
	for i := 0; i < ncols; i++ {
		taken, err := compute.TakeArray(ctx, batch.Record.Column(i), idxArr)
		if err != nil {
			for j := 0; j < i; j++ {
				cols[j].Release()
			}
			return nil, nil, fmt.Errorf("dataplane: collapse take col %d: %w", i, err)
		}
		cols[i] = taken
	}

	// 6. Build the collapsed RecordBatch.
	schema := batch.Record.Schema()
	collapsed := array.NewRecordBatch(schema, cols, int64(nWinners))
	for _, c := range cols {
		c.Release()
	}
	defer collapsed.Release()

	// 7. Split into upserts and deletes by __op.
	opIdx := colIndex(batch.Record.Schema(), "__op")
	if opIdx < 0 {
		return nil, nil, fmt.Errorf("dataplane: collapse: __op column not found")
	}
	opArr, ok := collapsed.Column(opIdx).(*array.Uint8)
	if !ok {
		return nil, nil, fmt.Errorf("dataplane: collapse: __op column type %T, want *array.Uint8", collapsed.Column(opIdx))
	}

	// delMask = __op == OpDelete; insUpdMask = NOT delMask. __op was
	// validated as {0,1,2} on the original batch (step 2), so "not a
	// delete" is exactly "an insert or an update" — no third case, no
	// per-row loop.
	delEq, err := compute.CallFunction(ctx, "equal", nil,
		&compute.ArrayDatum{Value: opArr.Data()}, compute.NewDatum(uint8(OpDelete)))
	if err != nil {
		return nil, nil, fmt.Errorf("dataplane: collapse op mask: %w", err)
	}
	delBool := delEq.(*compute.ArrayDatum).MakeArray().(*array.Boolean)
	delEq.Release()
	defer delBool.Release()

	insUpdNot, err := compute.CallFunction(ctx, "not", nil, &compute.ArrayDatum{Value: delBool.Data()})
	if err != nil {
		return nil, nil, fmt.Errorf("dataplane: collapse op mask: %w", err)
	}
	insUpdBool := insUpdNot.(*compute.ArrayDatum).MakeArray().(*array.Boolean)
	insUpdNot.Release()
	defer insUpdBool.Release()

	filterOpts := compute.DefaultFilterOptions()

	filteredUpserts, err := compute.FilterRecordBatch(ctx, collapsed, insUpdBool, filterOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("dataplane: collapse filter upserts: %w", err)
	}
	filteredDeletes, err := compute.FilterRecordBatch(ctx, collapsed, delBool, filterOpts)
	if err != nil {
		filteredUpserts.Release()
		return nil, nil, fmt.Errorf("dataplane: collapse filter deletes: %w", err)
	}

	if filteredUpserts.NumRows() > 0 {
		upserts = &Batch{Table: batch.Table, Record: filteredUpserts, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredUpserts.Release()
	}
	if filteredDeletes.NumRows() > 0 {
		deletes = &Batch{Table: batch.Table, Record: filteredDeletes, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredDeletes.Release()
	}
	return upserts, deletes, nil
}
