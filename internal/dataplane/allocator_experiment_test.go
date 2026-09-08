package dataplane_test

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/maltzsama/urutau/internal/dataplane"
)

// TestAllocatorThreadingExperiment documents a confirmed behavior in
// arrow-go v18.7.0: compute kernels allocate internal buffers via the
// allocator passed through context (compute.WithAllocator), but those
// internal buffers are never freed by explicit Release inside arrow-go.
//
// Expected: RED with residual bytes from kernel internals.
// Source:   exec.(*KernelCtx).Allocate, array.(*bufferBuilder).resize,
//
//	array.(*builder).init
//
// This test exists to pin the finding. It is NOT a regression test —
// the residual is an arrow-go limitation, not our code's leak.
// Skipped by default. Run explicitly: URUTAU_EXPERIMENT=1 go test -run TestAllocatorThreadingExperiment
func TestAllocatorThreadingExperiment(t *testing.T) {
	if os.Getenv("URUTAU_EXPERIMENT") == "" {
		t.Skip("pinned arrow-go residual — run with URUTAU_EXPERIMENT=1")
	}
	alloc := checkedAlloc(t)
	ctx := compute.WithAllocator(context.Background(), alloc)

	// Collapse: TakeArray + FilterRecordBatch kernel internals
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 10, Allocator: alloc})
	ups, dels, err := dataplane.Collapse(ctx, alloc, b, []string{"id"})
	if err != nil {
		b.Release()
		t.Fatalf("Collapse: %v", err)
	}
	if ups != nil {
		ups.Release()
	}
	if dels != nil {
		dels.Release()
	}
	b.Release()

	// Cast: CastToType kernel internals
	b2 := dataplane.GenerateBatch(2, dataplane.GeneratorOpts{NumRows: 10, Allocator: alloc})
	policy := dataplane.CastPolicy{"id": &arrow.StringType{}}
	out, err := dataplane.Cast(ctx, alloc, b2, policy)
	if err != nil {
		b2.Release()
		t.Fatalf("Cast: %v", err)
	}
	if out != b2 {
		out.Release()
	}
	b2.Release()

	// SplitByOp: FilterRecordBatch kernel internals
	b3 := dataplane.GenerateBatch(3, dataplane.GeneratorOpts{NumRows: 20, Allocator: alloc})
	ins, del, upd, err := dataplane.SplitByOp(ctx, alloc, b3)
	if err != nil {
		b3.Release()
		t.Fatalf("SplitByOp: %v", err)
	}
	if ins != nil {
		ins.Release()
	}
	if del != nil {
		del.Release()
	}
	if upd != nil {
		upd.Release()
	}
	b3.Release()

	// Filter: FilterRecordBatch kernel internals
	b4 := dataplane.GenerateBatch(4, dataplane.GeneratorOpts{NumRows: 10, PKDomain: 10, Allocator: alloc})
	mask, err := dataplane.EvaluatePredicate(ctx, alloc, b4, dataplane.Predicate{
		Column: "id",
		Op:     "=",
		Value:  int64(1),
	})
	if err != nil {
		b4.Release()
		t.Fatalf("EvaluatePredicate: %v", err)
	}
	fi, fd, fu, err := dataplane.Filter(ctx, alloc, b4, mask)
	if err != nil {
		mask.Release()
		b4.Release()
		t.Fatalf("Filter: %v", err)
	}
	mask.Release()
	if fi != nil {
		fi.Release()
	}
	if fd != nil {
		fd.Release()
	}
	if fu != nil {
		fu.Release()
	}
	b4.Release()

	// Direct kernel call — isolates the residual to arrow-go, not our operators.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	defer bb.Release()
	bb.Field(0).(*array.Int64Builder).Append(1)
	rec := bb.NewRecordBatch()
	defer rec.Release()

	fb := array.NewBooleanBuilder(alloc)
	defer fb.Release()
	fb.Append(true)
	filter := fb.NewBooleanArray()
	defer filter.Release()

	result, err := compute.FilterRecordBatch(ctx, rec, filter, compute.DefaultFilterOptions())
	if err != nil {
		t.Fatalf("FilterRecordBatch: %v", err)
	}
	result.Release()

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

	castResult, err := compute.CastToType(ctx, rec.Column(0), arrow.BinaryTypes.String)
	if err != nil {
		t.Fatalf("CastToType: %v", err)
	}
	castResult.Release()
}
