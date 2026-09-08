package dataplane

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Op values matching the wire schema __op column (CR-021).
const (
	OpInsert = 0
	OpUpdate = 1
	OpDelete = 2
)

// Filter splits a Batch into inserts, deletes, and updates based on a
// boolean mask evaluated over the data columns. The mask must have the
// same length as the batch. Watermark and Table are preserved on every
// output — transforms never move them (CR-069 §3.3).
//
// Rows that don't pass the mask are dropped entirely. The three output
// batches are independent; each may be nil if empty.
func Filter(ctx context.Context, alloc memory.Allocator, batch *Batch, mask arrow.Array) (inserts, deletes, updates *Batch, err error) {
	if batch.Record == nil {
		return nil, nil, nil, nil
	}
	nrows := int(batch.Record.NumRows())
	if mask.Len() != nrows {
		return nil, nil, nil, fmt.Errorf("dataplane: filter mask length %d != batch rows %d", mask.Len(), nrows)
	}

	boolMask, ok := mask.(*array.Boolean)
	if !ok {
		return nil, nil, nil, fmt.Errorf("dataplane: filter mask type %T, want *array.Boolean", mask)
	}

	opIdx := colIndex(batch.Record.Schema(), "__op")
	if opIdx < 0 {
		return nil, nil, nil, fmt.Errorf("dataplane: __op column not found")
	}
	opCol, ok := batch.Record.Column(opIdx).(*array.Uint8)
	if !ok {
		return nil, nil, nil, fmt.Errorf("dataplane: __op column type %T, want *array.Uint8", batch.Record.Column(opIdx))
	}

	delMask := buildOpMask(alloc, opCol, OpDelete, boolMask)
	insMask := buildOpMask(alloc, opCol, OpInsert, boolMask)
	updMask := buildOpMask(alloc, opCol, OpUpdate, boolMask)
	defer delMask.Release()
	defer insMask.Release()
	defer updMask.Release()

	filterOpts := compute.DefaultFilterOptions()

	filteredDelete, err := compute.FilterRecordBatch(ctx, batch.Record, delMask, filterOpts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dataplane: filter deletes: %w", err)
	}
	filteredInsert, err := compute.FilterRecordBatch(ctx, batch.Record, insMask, filterOpts)
	if err != nil {
		filteredDelete.Release()
		return nil, nil, nil, fmt.Errorf("dataplane: filter inserts: %w", err)
	}
	filteredUpdate, err := compute.FilterRecordBatch(ctx, batch.Record, updMask, filterOpts)
	if err != nil {
		filteredDelete.Release()
		filteredInsert.Release()
		return nil, nil, nil, fmt.Errorf("dataplane: filter updates: %w", err)
	}

	if filteredDelete.NumRows() > 0 {
		deletes = &Batch{Table: batch.Table, Record: filteredDelete, Watermark: batch.Watermark}
	} else {
		filteredDelete.Release()
	}
	if filteredInsert.NumRows() > 0 {
		inserts = &Batch{Table: batch.Table, Record: filteredInsert, Watermark: batch.Watermark}
	} else {
		filteredInsert.Release()
	}
	if filteredUpdate.NumRows() > 0 {
		updates = &Batch{Table: batch.Table, Record: filteredUpdate, Watermark: batch.Watermark}
	} else {
		filteredUpdate.Release()
	}
	return inserts, deletes, updates, nil
}

// Predicate is a simple column predicate for EvaluatePredicate.
// Supported ops: "=", "!=".
type Predicate struct {
	Column string
	Op     string
	Value  any
}

// EvaluatePredicate builds a boolean mask from a predicate over the
// batch's columns. Null values are treated as false (coalesce — matches
// the row-oriented path, NOT Kleene semantics).
func EvaluatePredicate(ctx context.Context, alloc memory.Allocator, batch *Batch, pred Predicate) (arrow.Array, error) {
	if batch.Record == nil {
		return nil, fmt.Errorf("dataplane: nil record")
	}
	idx := colIndex(batch.Record.Schema(), pred.Column)
	if idx < 0 {
		return nil, fmt.Errorf("dataplane: column %q not found", pred.Column)
	}
	col := batch.Record.Column(idx)
	return evaluateColPredicate(ctx, alloc, col, pred)
}

func evaluateColPredicate(ctx context.Context, alloc memory.Allocator, col arrow.Array, pred Predicate) (arrow.Array, error) {
	n := col.Len()
	switch pred.Op {
	case "=":
		return compareEqual(ctx, alloc, col, pred.Value, n)
	case "!=":
		eq, err := compareEqual(ctx, alloc, col, pred.Value, n)
		if err != nil {
			return nil, err
		}
		defer eq.Release()
		return invertBool(alloc, eq)
	default:
		return nil, fmt.Errorf("dataplane: unsupported predicate op %q (supported: \"=\", \"!=\")", pred.Op)
	}
}

func compareEqual(_ context.Context, alloc memory.Allocator, col arrow.Array, val any, n int) (arrow.Array, error) {
	switch a := col.(type) {
	case *array.Int64:
		v, ok := val.(int64)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want int64 for column of type Int64", val)
		}
		return compareInt64Eq(alloc, a, v, n), nil
	case *array.String:
		v, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want string for column of type String", val)
		}
		return compareStringEq(alloc, a, v, n), nil
	case *array.Boolean:
		v, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want bool for column of type Boolean", val)
		}
		return compareBoolEq(alloc, a, v, n), nil
	default:
		return nil, fmt.Errorf("dataplane: unsupported column type %T for equality predicate", col)
	}
}

func compareInt64Eq(alloc memory.Allocator, col *array.Int64, val int64, n int) arrow.Array {
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range n {
		if col.IsNull(i) {
			bb.Append(false) // coalesce null → false
		} else {
			bb.Append(col.Value(i) == val)
		}
	}
	return bb.NewBooleanArray()
}

func compareStringEq(alloc memory.Allocator, col *array.String, val string, n int) arrow.Array {
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range n {
		if col.IsNull(i) {
			bb.Append(false)
		} else {
			bb.Append(col.Value(i) == val)
		}
	}
	return bb.NewBooleanArray()
}

func compareBoolEq(alloc memory.Allocator, col *array.Boolean, val bool, n int) arrow.Array {
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range n {
		if col.IsNull(i) {
			bb.Append(false)
		} else {
			bb.Append(col.Value(i) == val)
		}
	}
	return bb.NewBooleanArray()
}

func invertBool(alloc memory.Allocator, col arrow.Array) (arrow.Array, error) {
	b, ok := col.(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("dataplane: invertBool: type %T, want *array.Boolean", col)
	}
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range b.Len() {
		bb.Append(!b.Value(i))
	}
	return bb.NewBooleanArray(), nil
}

// buildOpMask creates a boolean mask that is true only where the __op
// column equals the given value AND the input mask is true.
func buildOpMask(alloc memory.Allocator, opCol *array.Uint8, opVal uint8, mask *array.Boolean) arrow.Array {
	n := opCol.Len()
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range n {
		opMatch := opCol.Value(i) == opVal
		maskPass := mask.Value(i)
		bb.Append(opMatch && maskPass)
	}
	return bb.NewBooleanArray()
}

// TransitionMask builds a boolean mask from the __op column for a
// specific operation type. No input filter mask — all rows of that op
// type pass. Used when splitting a batch by operation without filtering.
func TransitionMask(alloc memory.Allocator, opCol *array.Uint8, opVal uint8) arrow.Array {
	n := opCol.Len()
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range n {
		bb.Append(opCol.Value(i) == opVal)
	}
	return bb.NewBooleanArray()
}

// SplitByOp splits a batch into inserts, deletes, and updates using the
// __op column. Every row goes to exactly one output. Watermark and Table
// are preserved. Empty outputs are nil (not empty batches).
func SplitByOp(ctx context.Context, alloc memory.Allocator, batch *Batch) (inserts, deletes, updates *Batch, err error) {
	if batch.Record == nil {
		return nil, nil, nil, nil
	}
	opIdx := colIndex(batch.Record.Schema(), "__op")
	if opIdx < 0 {
		return nil, nil, nil, fmt.Errorf("dataplane: __op column not found")
	}
	opCol, ok := batch.Record.Column(opIdx).(*array.Uint8)
	if !ok {
		return nil, nil, nil, fmt.Errorf("dataplane: __op column type %T, want *array.Uint8", batch.Record.Column(opIdx))
	}

	insMask := TransitionMask(alloc, opCol, OpInsert)
	delMask := TransitionMask(alloc, opCol, OpDelete)
	updMask := TransitionMask(alloc, opCol, OpUpdate)
	defer insMask.Release()
	defer delMask.Release()
	defer updMask.Release()

	filterOpts := compute.DefaultFilterOptions()

	filteredIns, err := compute.FilterRecordBatch(ctx, batch.Record, insMask, filterOpts)
	if err != nil {
		return nil, nil, nil, err
	}
	filteredDel, err := compute.FilterRecordBatch(ctx, batch.Record, delMask, filterOpts)
	if err != nil {
		filteredIns.Release()
		return nil, nil, nil, err
	}
	filteredUpd, err := compute.FilterRecordBatch(ctx, batch.Record, updMask, filterOpts)
	if err != nil {
		filteredIns.Release()
		filteredDel.Release()
		return nil, nil, nil, err
	}

	if filteredIns.NumRows() > 0 {
		inserts = &Batch{Table: batch.Table, Record: filteredIns, Watermark: batch.Watermark}
	} else {
		filteredIns.Release()
	}
	if filteredDel.NumRows() > 0 {
		deletes = &Batch{Table: batch.Table, Record: filteredDel, Watermark: batch.Watermark}
	} else {
		filteredDel.Release()
	}
	if filteredUpd.NumRows() > 0 {
		updates = &Batch{Table: batch.Table, Record: filteredUpd, Watermark: batch.Watermark}
	} else {
		filteredUpd.Release()
	}
	return inserts, deletes, updates, nil
}

func colIndex(schema *arrow.Schema, name string) int {
	for i := range schema.NumFields() {
		if schema.Field(i).Name == name {
			return i
		}
	}
	return -1
}
