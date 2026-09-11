package dataplane

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
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
//
// Mask nulls coalesce to false (M-14): a null mask bit never passes a
// row, so an absent predicate value drops the row rather than admitting
// it by accident.
func Filter(ctx context.Context, alloc memory.Allocator, batch *Batch, mask arrow.Array) (inserts, deletes, updates *Batch, err error) {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
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
		return nil, nil, nil, fmt.Errorf("dataplane: filter __op column type %T, want *array.Uint8", batch.Record.Column(opIdx))
	}
	if err := validateOpColumn(opCol); err != nil {
		return nil, nil, nil, err
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
		deletes = &Batch{Table: batch.Table, Record: filteredDelete, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredDelete.Release()
	}
	if filteredInsert.NumRows() > 0 {
		inserts = &Batch{Table: batch.Table, Record: filteredInsert, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredInsert.Release()
	}
	if filteredUpdate.NumRows() > 0 {
		updates = &Batch{Table: batch.Table, Record: filteredUpdate, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredUpdate.Release()
	}
	return inserts, deletes, updates, nil
}

// Side determines which column version a predicate evaluates against.
type Side uint8

const (
	// AfterSide evaluates over the data column (default).
	AfterSide Side = iota
	// BeforeSide evaluates over "__before_" + Column.
	BeforeSide
)

// Predicate is a simple column predicate for EvaluatePredicate and
// TransitionMatrix. Supported ops: "=", "!=".
type Predicate struct {
	Column string
	Op     string
	Value  any
	Side   Side
}

// resolveColumn returns the column index for a predicate, resolving
// the side (AfterSide → Column, BeforeSide → __before_ + Column).
// Returns error if the column is not found — no silent fallback.
func resolveColumn(schema *arrow.Schema, p Predicate) (int, error) {
	name := p.Column
	if p.Side == BeforeSide {
		name = "__before_" + p.Column
	}
	idx := colIndex(schema, name)
	if idx < 0 {
		return -1, fmt.Errorf("dataplane: transition: column %q not found", name)
	}
	return idx, nil
}

// EvaluatePredicate builds a boolean mask from a predicate over the
// batch's columns. Null values are treated as false (coalesce — matches
// the row-oriented path, NOT Kleene semantics).
func EvaluatePredicate(ctx context.Context, alloc memory.Allocator, batch *Batch, pred Predicate) (arrow.Array, error) {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	if batch.Record == nil {
		return nil, fmt.Errorf("dataplane: nil record")
	}
	idx, err := resolveColumn(batch.Record.Schema(), pred)
	if err != nil {
		return nil, err
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
		return invertBool(ctx, eq)
	default:
		return nil, fmt.Errorf("dataplane: unsupported predicate op %q (supported: \"=\", \"!=\")", pred.Op)
	}
}

// compareEqual builds an equality mask via the arrow-go compute kernels
// (compute.CallFunction "equal") instead of a hand-rolled per-type scan.
// Two behaviors the kernel does NOT give us for free, so we still guard
// them explicitly to match this package's documented semantics:
//
//   - Kleene null propagation: "equal" against a null input produces null,
//     not false. coalesceFalse walks the result after the call and forces
//     every null bit to false (M-14 — a null mask bit never passes a row).
//   - Silent numeric overflow: casting an out-of-range int64 into an Int32
//     scalar wraps instead of erroring. The explicit range check is kept
//     for Int32 so an out-of-range predicate value still fails loudly.
//   - Silent NaN acceptance: "equal" against NaN quietly returns false for
//     every row instead of erroring. The explicit NaN check is kept so
//     this stays a documented, loud error instead of a silent no-op.
func compareEqual(ctx context.Context, alloc memory.Allocator, col arrow.Array, val any, n int) (arrow.Array, error) {
	sc, err := scalarFor(col.DataType(), val)
	if err != nil {
		return nil, err
	}
	colDatum := compute.NewDatum(col)
	defer colDatum.Release()
	scDatum := compute.NewDatum(sc)
	defer scDatum.Release()
	res, err := compute.CallFunction(ctx, "equal", nil, colDatum, scDatum)
	if err != nil {
		return nil, fmt.Errorf("dataplane: equal: %w", err)
	}
	defer res.Release()
	out := res.(*compute.ArrayDatum).MakeArray()
	defer out.Release()
	return coalesceFalse(alloc, out.(*array.Boolean)), nil
}

// scalarFor validates val against col's Arrow type and binds it to a
// scalar.Scalar of that exact type, preserving the type-checking this
// package documents (a predicate value type mismatch is a loud error, not
// an implicit cast).
func scalarFor(dt arrow.DataType, val any) (scalar.Scalar, error) {
	switch dt.ID() {
	case arrow.INT32:
		switch v := val.(type) {
		case int32:
			return scalar.MakeScalarParam(v, dt)
		case int64:
			if v < math.MinInt32 || v > math.MaxInt32 {
				return nil, fmt.Errorf("dataplane: predicate value %d out of range for Int32", v)
			}
			return scalar.MakeScalarParam(int32(v), dt)
		default:
			return nil, fmt.Errorf("dataplane: predicate value type %T, want int32/int64 for column of type Int32", val)
		}
	case arrow.INT64:
		v, ok := val.(int64)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want int64 for column of type Int64", val)
		}
		return scalar.MakeScalarParam(v, dt)
	case arrow.UINT64:
		v, ok := val.(uint64)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want uint64 for column of type Uint64", val)
		}
		return scalar.MakeScalarParam(v, dt)
	case arrow.FLOAT64:
		v, ok := val.(float64)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want float64 for column of type Float64", val)
		}
		if math.IsNaN(v) {
			return nil, fmt.Errorf("dataplane: NaN predicate matches nothing (documented)")
		}
		return scalar.MakeScalarParam(v, dt)
	case arrow.STRING:
		v, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want string for column of type String", val)
		}
		return scalar.MakeScalarParam(v, dt)
	case arrow.BOOL:
		v, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want bool for column of type Boolean", val)
		}
		return scalar.MakeScalarParam(v, dt)
	case arrow.DATE32:
		v, ok := val.(int32)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want int32 (days) for column of type Date32", val)
		}
		return scalar.MakeScalarParam(v, dt)
	case arrow.TIME64:
		v, ok := val.(int64)
		if !ok {
			return nil, fmt.Errorf("dataplane: predicate value type %T, want int64 (micros) for column of type Time64", val)
		}
		return scalar.MakeScalarParam(v, dt)
	default:
		return nil, fmt.Errorf("dataplane: unsupported column type %s for equality predicate", dt)
	}
}

// coalesceFalse forces every null bit in a boolean mask to false (M-14):
// a null mask bit never passes a row, so an absent predicate value drops
// the row rather than admitting it by accident. The compute kernels
// propagate null (Kleene logic); this package coalesces instead.
func coalesceFalse(alloc memory.Allocator, b *array.Boolean) arrow.Array {
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range b.Len() {
		bb.Append(b.IsValid(i) && b.Value(i))
	}
	return bb.NewBooleanArray()
}

func invertBool(ctx context.Context, col arrow.Array) (arrow.Array, error) {
	if _, ok := col.(*array.Boolean); !ok {
		return nil, fmt.Errorf("dataplane: invertBool: type %T, want *array.Boolean", col)
	}
	colDatum := compute.NewDatum(col)
	defer colDatum.Release()
	res, err := compute.CallFunction(ctx, "not", nil, colDatum)
	if err != nil {
		return nil, fmt.Errorf("dataplane: invert: %w", err)
	}
	defer res.Release()
	return res.(*compute.ArrayDatum).MakeArray(), nil
}

// buildOpMask creates a boolean mask that is true only where the __op
// column equals the given value AND the input mask is true.
func buildOpMask(alloc memory.Allocator, opCol *array.Uint8, opVal uint8, mask *array.Boolean) arrow.Array {
	n := opCol.Len()
	bb := array.NewBooleanBuilder(alloc)
	defer bb.Release()
	for i := range n {
		opMatch := opCol.Value(i) == opVal
		// Null mask bit → false (M-14): explicit, not a buffer accident.
		maskPass := mask.IsValid(i) && mask.Value(i)
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
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
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
	if err := validateOpColumn(opCol); err != nil {
		return nil, nil, nil, err
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
		inserts = &Batch{Table: batch.Table, Record: filteredIns, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredIns.Release()
	}
	if filteredDel.NumRows() > 0 {
		deletes = &Batch{Table: batch.Table, Record: filteredDel, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredDel.Release()
	}
	if filteredUpd.NumRows() > 0 {
		updates = &Batch{Table: batch.Table, Record: filteredUpd, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
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

// validateOpColumn checks that every value in the __op column is a known
// operation (Insert=0, Update=1, Delete=2). Unknown values cause an
// immediate error — they would silently vanish in SplitByOp/Filter.
func validateOpColumn(opCol *array.Uint8) error {
	for i := range opCol.Len() {
		switch opCol.Value(i) {
		case OpInsert, OpUpdate, OpDelete:
		default:
			return fmt.Errorf("dataplane: invalid __op %d at row %d", opCol.Value(i), i)
		}
	}
	return nil
}

// evalAll evaluates a list of predicates and ANDs their masks into a
// single boolean mask. Empty predicates → all-true mask (pass-through).
func evalAll(ctx context.Context, alloc memory.Allocator, batch *Batch, preds []Predicate) (arrow.Array, error) {
	nrows := int(batch.Record.NumRows())
	if len(preds) == 0 {
		bb := array.NewBooleanBuilder(alloc)
		for range nrows {
			bb.Append(true)
		}
		arr := bb.NewBooleanArray()
		bb.Release() // explicit, not deferred — buffers live in arr now
		return arr, nil
	}

	var combined arrow.Array
	for i, p := range preds {
		mask, err := EvaluatePredicate(ctx, alloc, batch, p)
		if err != nil {
			if combined != nil {
				combined.Release()
			}
			return nil, fmt.Errorf("dataplane: transition: predicate %d: %w", i, err)
		}
		if combined == nil {
			combined = mask
			continue
		}
		// Both masks are already coalesced (no nulls), so plain "and" and
		// "and_kleene" agree here; "and" has the simpler signature.
		combinedDatum := compute.NewDatum(combined)
		maskDatum := compute.NewDatum(mask)
		res, err := compute.CallFunction(ctx, "and", nil, combinedDatum, maskDatum)
		combinedDatum.Release()
		maskDatum.Release()
		mask.Release()
		combined.Release()
		if err != nil {
			return nil, fmt.Errorf("dataplane: transition: predicate %d: and: %w", i, err)
		}
		combined = res.(*compute.ArrayDatum).MakeArray()
		res.Release()
	}
	return combined, nil
}

// TransitionMatrix classifies rows into insert/delete/update based on
// before and after predicate evaluation (CR-069 §3.1).
//
// Quadrants (before_pass × after_pass):
//
//	insert  = !before & after  → row enters (upsert)
//	delete  =  before & !after → row leaves (equality delete)
//	update  =  before & after  → upsert (normal)
//	neither = !before & !after → dropped, not in any output
//
// Null → false (coalesce, same as predicate evaluation).
// Empty predicate lists → pass-through (everything passes).
// __op does NOT participate — quadrants are the write decision;
// op is CDC semantics.
func TransitionMatrix(ctx context.Context, alloc memory.Allocator, batch *Batch,
	before, after []Predicate) (inserts, deletes, updates *Batch, err error) {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		return nil, nil, nil, nil
	}
	nrows := int(batch.Record.NumRows())

	beforePass, err := evalAll(ctx, alloc, batch, before)
	if err != nil {
		return nil, nil, nil, err
	}
	defer beforePass.Release()

	afterPass, err := evalAll(ctx, alloc, batch, after)
	if err != nil {
		return nil, nil, nil, err
	}
	defer afterPass.Release()

	bp := beforePass.(*array.Boolean)
	ap := afterPass.(*array.Boolean)

	// W-2: __op defines existence; predicates define membership.
	opIdx := colIndex(batch.Record.Schema(), "__op")
	if opIdx < 0 {
		return nil, nil, nil, fmt.Errorf("dataplane: transition: __op column not found")
	}
	opCol, ok := batch.Record.Column(opIdx).(*array.Uint8)
	if !ok {
		return nil, nil, nil, fmt.Errorf("dataplane: transition: __op %T, want *array.Uint8", batch.Record.Column(opIdx))
	}
	if err := validateOpColumn(opCol); err != nil {
		return nil, nil, nil, err
	}

	// Build the three quadrant masks.
	insMask := array.NewBooleanBuilder(alloc)
	delMask := array.NewBooleanBuilder(alloc)
	updMask := array.NewBooleanBuilder(alloc)
	defer insMask.Release()
	defer delMask.Release()
	defer updMask.Release()

	for i := range nrows {
		a := ap.Value(i) && opCol.Value(i) != OpDelete // exists after?
		b := bp.Value(i) && opCol.Value(i) != OpInsert // existed before?
		insMask.Append(!b && a)
		delMask.Append(b && !a)
		updMask.Append(b && a)
	}

	insBool := insMask.NewBooleanArray()
	delBool := delMask.NewBooleanArray()
	updBool := updMask.NewBooleanArray()
	defer insBool.Release()
	defer delBool.Release()
	defer updBool.Release()

	filterOpts := compute.DefaultFilterOptions()

	filteredInserts, err := compute.FilterRecordBatch(ctx, batch.Record, insBool, filterOpts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dataplane: transition filter inserts: %w", err)
	}
	filteredDeletes, err := compute.FilterRecordBatch(ctx, batch.Record, delBool, filterOpts)
	if err != nil {
		filteredInserts.Release()
		return nil, nil, nil, fmt.Errorf("dataplane: transition filter deletes: %w", err)
	}
	filteredUpdates, err := compute.FilterRecordBatch(ctx, batch.Record, updBool, filterOpts)
	if err != nil {
		filteredInserts.Release()
		filteredDeletes.Release()
		return nil, nil, nil, fmt.Errorf("dataplane: transition filter updates: %w", err)
	}

	if filteredInserts.NumRows() > 0 {
		inserts = &Batch{Table: batch.Table, Record: filteredInserts, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredInserts.Release()
	}
	if filteredDeletes.NumRows() > 0 {
		deletes = &Batch{Table: batch.Table, Record: filteredDeletes, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredDeletes.Release()
	}
	if filteredUpdates.NumRows() > 0 {
		updates = &Batch{Table: batch.Table, Record: filteredUpdates, Watermark: batch.Watermark, Mode: batch.Mode, SnapshotState: batch.SnapshotState, SnapshotPending: batch.SnapshotPending}
	} else {
		filteredUpdates.Release()
	}
	return inserts, deletes, updates, nil
}
