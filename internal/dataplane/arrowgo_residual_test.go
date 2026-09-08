package dataplane_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestArrowGoKernelResidual is a tripwire. It fires the moment arrow-go
// fixes its kernel-internal allocation leak (exec.(*KernelCtx).Allocate,
// array.(*bufferBuilder).resize, array.(*builder).init — v18.7.0).
//
// GREEN (residual > 0): the limitation still exists. Our tests use
// kernelAlloc (plain GoAllocator) for kernel-calling operators; our
// builders are leak-asserted via checkedAlloc.
//
// RED (residual == 0): arrow-go fixed it. UPGRADE: delete the
// kernelAlloc split, thread checkedAlloc via compute.WithAllocator in
// all tests. The two-tier allocation discipline becomes one tier.
//
// Run always — no env var, no skip. This is a sentinel, not a repro.
func TestArrowGoKernelResidual(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	ctx := compute.WithAllocator(context.Background(), alloc)

	// Minimal kernel call: TakeArray with one element.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	defer bb.Release()
	bb.Field(0).(*array.Int64Builder).Append(1)
	rec := bb.NewRecordBatch()
	defer rec.Release()

	ib := array.NewInt32Builder(alloc)
	defer ib.Release()
	ib.Append(0)
	indices := ib.NewInt32Array()
	defer indices.Release()

	taken, err := compute.TakeArray(ctx, rec.Column(0), indices)
	if err != nil {
		t.Fatalf("TakeArray: %v", err)
	}
	taken.Release()

	runtime.GC()

	if alloc.CurrentAlloc() == 0 {
		t.Error("arrow-go kernel internals no longer leak — UPGRADE TIME: " +
			"delete the kernelAlloc split, thread checkedAlloc via compute.WithAllocator everywhere")
	}
}
