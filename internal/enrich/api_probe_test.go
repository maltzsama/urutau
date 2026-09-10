package enrich

// P1 of BRIEF-PERF v10 Onda 2 — API probes.
//
// The Onda 2 executor is built on arrow-go compute kernels. The brief was
// drafted assuming compute.IndexIn and compute.IfElse; NEITHER exists in
// arrow-go v18.7.0 (the pinned version). These probes pin exactly which
// kernels ARE available and in which form, so the executor code is written
// against a verified surface. Each probe is a permanent regression: if a
// dependency bump changes a kernel's shape, the probe fails here, not deep
// in ColumnarJoin.
//
// RESULT REGISTRY (filled by running this file; the executor uses ONLY the
// registered form, no runtime flag reads a test):
//
//   P1_TakeRecord     : compute.Take(ctx, opts, *RecordDatum, Int32 *ArrayDatum)
//                       -> *RecordDatum.  PASSES.
//   P2_IsIn           : compute.CallFunction(ctx, "is_in",
//                       &compute.SetOptions{ValueSet: <array datum>,
//                       NullBehavior: compute.NullMatchingSkip}, <values datum>)
//                       -> Boolean; batch null -> false.  PASSES.
//   P3_MakeArrayOfNull: array.MakeArrayOfNull(mem, dt, n) for every type in
//                       the reference value space.  PASSES.
//   P4_ConcatTyped    : array.Concatenate([]arrow.Array{a, b}, mem) with a,b
//                       same type.  PASSES.
//   P5_EqualScalar    : compute.CallFunction(ctx, "equal", nil, <col datum>,
//                       compute.NewDatum(<scalar>)) -> Boolean, scalar RHS
//                       broadcast.  PASSES.

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func probeCtx() context.Context {
	return compute.WithAllocator(context.Background(), memory.DefaultAllocator)
}

func int64Arr(vals ...int64) *array.Int64 {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vals, nil)
	return b.NewInt64Array()
}

func int32Arr(vals ...int32) *array.Int32 {
	b := array.NewInt32Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(vals, nil)
	return b.NewInt32Array()
}

func TestArrowKernelProbes(t *testing.T) {
	ctx := probeCtx()

	t.Run("P1_TakeRecord", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "k", Type: arrow.PrimitiveTypes.Int64},
			{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)
		rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer rb.Release()
		rb.Field(0).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)
		rb.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
		ref := rb.NewRecordBatch()
		defer ref.Release()

		idx := int32Arr(2, 0, 2, 1)
		defer idx.Release()

		out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.RecordDatum{Value: ref}, &compute.ArrayDatum{Value: idx.Data()})
		if err != nil {
			t.Fatalf("compute.Take on RecordDatum: %v", err)
		}
		rd, ok := out.(*compute.RecordDatum)
		if !ok {
			t.Fatalf("Take returned %T, want *compute.RecordDatum", out)
		}
		defer rd.Release()
		got := rd.Value
		if got.NumRows() != 4 {
			t.Fatalf("rows = %d, want 4", got.NumRows())
		}
		ks := got.Column(0).(*array.Int64)
		if ks.Value(0) != 30 || ks.Value(1) != 10 || ks.Value(3) != 20 {
			t.Fatalf("gathered keys wrong: %v", ks)
		}
		vs := got.Column(1).(*array.String)
		if vs.Value(0) != "c" || vs.Value(1) != "a" {
			t.Fatalf("gathered values wrong: %v", vs)
		}
	})

	t.Run("P2_IsIn", func(t *testing.T) {
		// value set: the reference keys.
		valueSet := int64Arr(10, 20, 30)
		defer valueSet.Release()
		// batch keys: 20 hits, 99 misses, null misses.
		bb := array.NewInt64Builder(memory.DefaultAllocator)
		defer bb.Release()
		bb.Append(20)
		bb.Append(99)
		bb.AppendNull()
		bb.Append(10)
		batch := bb.NewInt64Array()
		defer batch.Release()

		out, err := compute.CallFunction(ctx, "is_in",
			&compute.SetOptions{
				ValueSet:     &compute.ArrayDatum{Value: valueSet.Data()},
				NullBehavior: compute.NullMatchingSkip,
			},
			&compute.ArrayDatum{Value: batch.Data()})
		if err != nil {
			t.Fatalf("is_in: %v", err)
		}
		ad, ok := out.(*compute.ArrayDatum)
		if !ok {
			t.Fatalf("is_in returned %T, want *compute.ArrayDatum", out)
		}
		defer ad.Release()
		mask := ad.MakeArray().(*array.Boolean)
		defer mask.Release()
		if mask.Len() != 4 {
			t.Fatalf("mask len = %d, want 4", mask.Len())
		}
		// row 0 (20) hit; row 1 (99) miss; row 2 (null) miss; row 3 (10) hit.
		if !boolAt(mask, 0) || boolAt(mask, 1) || boolAt(mask, 2) || !boolAt(mask, 3) {
			t.Fatalf("is_in mask = [%v %v %v %v], want [true false false true]",
				boolAt(mask, 0), boolAt(mask, 1), boolAt(mask, 2), boolAt(mask, 3))
		}
	})

	t.Run("P3_MakeArrayOfNull", func(t *testing.T) {
		types := []arrow.DataType{
			arrow.BinaryTypes.String,
			arrow.BinaryTypes.Binary,
			arrow.FixedWidthTypes.Boolean,
			arrow.PrimitiveTypes.Int64,
			arrow.PrimitiveTypes.Uint64,
			arrow.PrimitiveTypes.Float64,
			&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
		}
		for _, dt := range types {
			a := array.MakeArrayOfNull(memory.DefaultAllocator, dt, 5)
			if a.Len() != 5 || a.NullN() != 5 {
				a.Release()
				t.Fatalf("MakeArrayOfNull(%s): len=%d nullN=%d, want 5/5", dt, a.Len(), a.NullN())
			}
			if !arrow.TypeEqual(a.DataType(), dt) {
				a.Release()
				t.Fatalf("MakeArrayOfNull(%s): type = %s", dt, a.DataType())
			}
			a.Release()
		}
	})

	t.Run("P4_ConcatTyped", func(t *testing.T) {
		hits := int64Arr(1, 2)
		defer hits.Release()
		nulls := array.MakeArrayOfNull(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, 3)
		defer nulls.Release()
		out, err := array.Concatenate([]arrow.Array{hits, nulls}, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("Concatenate: %v", err)
		}
		defer out.Release()
		got := out.(*array.Int64)
		if got.Len() != 5 || got.Value(0) != 1 || got.Value(1) != 2 || !got.IsNull(2) || !got.IsNull(4) {
			t.Fatalf("Concatenate result wrong: %v", got)
		}
	})

	t.Run("P5_EqualScalar", func(t *testing.T) {
		b := array.NewUint8Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues([]uint8{0, 2, 1, 2}, nil)
		opCol := b.NewUint8Array()
		defer opCol.Release()

		out, err := compute.CallFunction(ctx, "equal", nil,
			&compute.ArrayDatum{Value: opCol.Data()},
			compute.NewDatum(uint8(2)))
		if err != nil {
			t.Fatalf("equal with scalar RHS: %v", err)
		}
		ad := out.(*compute.ArrayDatum)
		defer ad.Release()
		mask := ad.MakeArray().(*array.Boolean)
		defer mask.Release()
		if boolAt(mask, 0) || !boolAt(mask, 1) || boolAt(mask, 2) || !boolAt(mask, 3) {
			t.Fatalf("equal(__op, 2) = [%v %v %v %v], want [false true false true]",
				boolAt(mask, 0), boolAt(mask, 1), boolAt(mask, 2), boolAt(mask, 3))
		}
	})
}

func boolAt(a *array.Boolean, i int) bool {
	return a.IsValid(i) && a.Value(i)
}
