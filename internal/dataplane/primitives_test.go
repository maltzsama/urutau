package dataplane_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// primitives_test.go smoke-tests every compute kernel the CR-069 plan
// uses. If any of these fail, the CR-045 table was wrong — the kernel
// doesn't exist or behaves differently than claimed. This file converts
// folk knowledge into executable assertion.

func TestPrimitiveFilterRecordBatch(t *testing.T) {
	alloc := checkedAlloc(t)
	ctx := context.Background()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	defer bb.Release()
	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(0).(*array.Int64Builder).Append(2)
	bb.Field(0).(*array.Int64Builder).Append(3)
	bb.Field(1).(*array.StringBuilder).Append("a")
	bb.Field(1).(*array.StringBuilder).Append("b")
	bb.Field(1).(*array.StringBuilder).Append("c")
	rec := bb.NewRecordBatch()
	defer rec.Release()

	// Boolean filter: keep rows 0 and 2
	fb := array.NewBooleanBuilder(alloc)
	defer fb.Release()
	fb.Append(true)
	fb.Append(false)
	fb.Append(true)
	filter := fb.NewBooleanArray()
	defer filter.Release()

	opts := compute.DefaultFilterOptions()
	result, err := compute.FilterRecordBatch(ctx, rec, filter, opts)
	if err != nil {
		t.Fatalf("FilterRecordBatch: %v", err)
	}
	defer result.Release()

	if result.NumRows() != 2 {
		t.Errorf("NumRows = %d, want 2", result.NumRows())
	}
}

func TestPrimitiveTakeArray(t *testing.T) {
	alloc := checkedAlloc(t)
	ctx := context.Background()

	// String array to take from
	sb := array.NewStringBuilder(alloc)
	defer sb.Release()
	sb.Append("x")
	sb.Append("y")
	sb.Append("z")
	values := sb.NewStringArray()
	defer values.Release()

	// Indices: take row 2, then row 0
	ib := array.NewInt32Builder(alloc)
	defer ib.Release()
	ib.Append(2)
	ib.Append(0)
	indices := ib.NewInt32Array()
	defer indices.Release()

	result, err := compute.TakeArray(ctx, values, indices)
	if err != nil {
		t.Fatalf("TakeArray: %v", err)
	}
	defer result.Release()

	if result.Len() != 2 {
		t.Fatalf("Len = %d, want 2", result.Len())
	}
	if result.(*array.String).Value(0) != "z" {
		t.Errorf("result[0] = %q, want z", result.(*array.String).Value(0))
	}
	if result.(*array.String).Value(1) != "x" {
		t.Errorf("result[1] = %q, want x", result.(*array.String).Value(1))
	}
}

func TestPrimitiveCastArray(t *testing.T) {
	alloc := checkedAlloc(t)
	ctx := context.Background()

	// Int32 array
	ib := array.NewInt32Builder(alloc)
	defer ib.Release()
	ib.Append(1)
	ib.Append(2)
	vals := ib.NewInt32Array()
	defer vals.Release()

	result, err := compute.CastToType(ctx, vals, arrow.PrimitiveTypes.Int64)
	if err != nil {
		t.Fatalf("CastToType: %v", err)
	}
	defer result.Release()

	if result.DataType().ID() != arrow.INT64 {
		t.Errorf("output type = %v, want INT64", result.DataType())
	}
}

func TestPrimitiveSortIndices(t *testing.T) {
	alloc := checkedAlloc(t)
	ctx := context.Background()

	ib := array.NewInt32Builder(alloc)
	defer ib.Release()
	ib.Append(30)
	ib.Append(10)
	ib.Append(20)
	vals := ib.NewInt32Array()
	defer vals.Release()

	opts := compute.SortOptions{compute.DefaultSortKey()}
	datum := &compute.ArrayDatum{Value: vals.Data()}
	result, err := compute.SortIndices(ctx, datum, opts)
	if err != nil {
		t.Fatalf("SortIndices: %v", err)
	}
	defer result.Release()

	resultArr := result.(*compute.ArrayDatum).MakeArray()
	defer resultArr.Release()

	if resultArr.Len() != 3 {
		t.Fatalf("Len = %d, want 3", resultArr.Len())
	}
	indices := resultArr.(*array.Uint64)
	expected := []uint64{1, 2, 0}
	for i, want := range expected {
		if indices.Value(i) != want {
			t.Errorf("indices[%d] = %d, want %d", i, indices.Value(i), want)
		}
	}
}

func TestPrimitiveRecordBatchAsInterface(t *testing.T) {
	alloc := checkedAlloc(t)
	ctx := context.Background()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	defer bb.Release()
	bb.Field(0).(*array.Int64Builder).Append(42)
	rec := bb.NewRecordBatch()
	defer rec.Release()

	// Verify: RecordBatch is the interface that compute kernels accept.
	// This test proves the Batch.Record field type is correct.
	opts := compute.DefaultFilterOptions()
	fb := array.NewBooleanBuilder(alloc)
	defer fb.Release()
	fb.Append(true)
	filter := fb.NewBooleanArray()
	defer filter.Release()

	result, err := compute.FilterRecordBatch(ctx, rec, filter, opts)
	if err != nil {
		t.Fatalf("FilterRecordBatch with RecordBatch interface: %v", err)
	}
	defer result.Release()

	if result.NumRows() != 1 {
		t.Errorf("NumRows = %d, want 1", result.NumRows())
	}
}
