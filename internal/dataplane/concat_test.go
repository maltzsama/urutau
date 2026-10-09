package dataplane_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/internal/dataplane"
)

func int64Batch(alloc memory.Allocator, vals ...int64) *dataplane.Batch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rb := array.NewRecordBuilder(alloc, schema)
	for _, v := range vals {
		rb.Field(0).(*array.Int64Builder).Append(v)
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	return &dataplane.Batch{Table: "t", Record: rec}
}

func stringBatch(alloc memory.Allocator, vals ...string) *dataplane.Batch {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String}}, nil)
	rb := array.NewRecordBuilder(alloc, schema)
	for _, v := range vals {
		rb.Field(0).(*array.StringBuilder).Append(v)
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	return &dataplane.Batch{Table: "t", Record: rec}
}

// #508: one concatenation helper with a single schema check.
func TestConcatBatchesSchemaCheck(t *testing.T) {
	alloc := memory.DefaultAllocator
	a := int64Batch(alloc, 1, 2, 3)
	defer a.Record.Release()
	b := int64Batch(alloc, 4, 5)
	defer b.Record.Release()

	out, err := dataplane.ConcatBatches(nil, []*dataplane.Batch{a, b})
	if err != nil {
		t.Fatalf("ConcatBatches: %v", err)
	}
	if out == nil || out.Record == nil || out.Record.NumRows() != 5 {
		t.Fatalf("concatenated = %v, want 5 rows", out)
	}
	out.Record.Release()

	// A different schema must be rejected, not silently concatenated.
	other := stringBatch(alloc, "x")
	defer other.Record.Release()
	if _, err := dataplane.ConcatBatches(nil, []*dataplane.Batch{a, other}); err == nil {
		t.Fatal("ConcatBatches must reject a schema mismatch")
	}

	// Empty inputs concatenate to nil.
	if out, err := dataplane.ConcatBatches(nil, nil); err != nil || out != nil {
		t.Fatalf("empty concat = %v, %v; want nil, nil", out, err)
	}
}

func TestMergeBatchesNilSides(t *testing.T) {
	alloc := memory.DefaultAllocator
	a := int64Batch(alloc, 1, 2)
	defer a.Record.Release()

	if out, err := dataplane.MergeBatches(nil, nil, nil); err != nil || out != nil {
		t.Fatalf("merge of two nils = %v, %v", out, err)
	}
	out, err := dataplane.MergeBatches(nil, a, nil)
	if err != nil || out == nil || out.Record.NumRows() != 2 {
		t.Fatalf("merge(a, nil) = %v, %v", out, err)
	}
	out.Record.Release()
}
