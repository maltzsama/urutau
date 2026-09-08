package dataplane

import (
	"context"
	"encoding/binary"
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
func Collapse(ctx context.Context, batch *Batch, pkCols []string) (upserts, deletes *Batch, err error) {
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		return nil, nil, nil
	}

	nrows := int(batch.Record.NumRows())

	// 1. Build exact keys and check for null PKs.
	keys := make([][]byte, nrows)
	for row := range nrows {
		for _, col := range pkCols {
			idx := colIndex(batch.Record.Schema(), col)
			if idx < 0 {
				return nil, nil, fmt.Errorf("dataplane: collapse: PK column %q not found", col)
			}
			if batch.Record.Column(idx).IsNull(row) {
				return nil, nil, fmt.Errorf("dataplane: collapse: null in PK column %q at row %d", col, row)
			}
		}
		keys[row] = EncodeKey(batch.Record, row, pkCols)
	}

	// 2. Map-based scan: last occurrence wins per group, first-appearance
	// order for emission. O(n), no hashing, no collision risk.
	type groupInfo struct {
		winnerRow int
	}
	groups := make(map[string]*groupInfo, nrows)
	var groupOrder []string

	for row, key := range keys {
		k := string(key)
		g, exists := groups[k]
		if !exists {
			groupOrder = append(groupOrder, k)
			groups[k] = &groupInfo{winnerRow: row}
		} else {
			g.winnerRow = row // last occurrence always wins
		}
	}

	// 3. Collect winner indices in first-appearance order.
	winnerIndices := make([]int32, 0, len(groupOrder))
	for _, k := range groupOrder {
		winnerIndices = append(winnerIndices, int32(groups[k].winnerRow))
	}

	// 4. Build an index array for Take.
	alloc := memory.NewGoAllocator()
	idxBuilder := array.NewInt32Builder(alloc)
	defer idxBuilder.Release()
	for _, idx := range winnerIndices {
		idxBuilder.Append(idx)
	}
	idxArr := idxBuilder.NewInt32Array()
	defer idxArr.Release()

	// 5. Take each column independently — TakeArray works on flat arrays.
	cols := make([]arrow.Array, batch.Record.NumCols())
	for i := range int(batch.Record.NumCols()) {
		taken, err := compute.TakeArray(ctx, batch.Record.Column(i), idxArr)
		if err != nil {
			return nil, nil, fmt.Errorf("dataplane: collapse take col %d: %w", i, err)
		}
		cols[i] = taken
	}

	// 6. Build the collapsed RecordBatch.
	collapsed := array.NewRecordBatch(batch.Record.Schema(), cols, int64(len(winnerIndices)))
	for _, c := range cols {
		c.Release()
	}
	defer collapsed.Release()

	// 7. Split into upserts and deletes by __op.
	opIdx := colIndex(batch.Record.Schema(), "__op")
	if opIdx < 0 {
		return nil, nil, fmt.Errorf("dataplane: __op column not found")
	}
	opArr := collapsed.Column(opIdx).(*array.Uint8)

	insUpdMask := array.NewBooleanBuilder(alloc)
	defer insUpdMask.Release()
	delMask := array.NewBooleanBuilder(alloc)
	defer delMask.Release()
	for i := range opArr.Len() {
		op := opArr.Value(i)
		isInsertOrUpdate := op == OpInsert || op == OpUpdate
		isDelete := op == OpDelete
		insUpdMask.Append(isInsertOrUpdate)
		delMask.Append(isDelete)
	}

	filterOpts := compute.DefaultFilterOptions()

	insUpdBool := insUpdMask.NewBooleanArray()
	defer insUpdBool.Release()
	delBool := delMask.NewBooleanArray()
	defer delBool.Release()

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
		upserts = &Batch{Table: batch.Table, Record: filteredUpserts, Watermark: batch.Watermark}
	} else {
		filteredUpserts.Release()
	}
	if filteredDeletes.NumRows() > 0 {
		deletes = &Batch{Table: batch.Table, Record: filteredDeletes, Watermark: batch.Watermark}
	} else {
		filteredDeletes.Release()
	}
	return upserts, deletes, nil
}

func encodeKeyLen(val string) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(len(val)))
	return buf[:]
}
