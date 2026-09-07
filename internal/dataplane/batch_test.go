package dataplane_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/internal/dataplane"
)

func newTestRecord(t *testing.T, alloc memory.Allocator) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	bb := array.NewRecordBuilder(alloc, schema)
	defer bb.Release()

	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.StringBuilder).Append("alice")

	return bb.NewRecordBatch()
}

func TestReleaseNilRecord(t *testing.T) {
	b := &dataplane.Batch{Table: "t", Watermark: "w"}
	b.Release()
}

func TestReleaseFreesRecord(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := newTestRecord(t, alloc)

	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: "w"}
	b.Release()

	if b.Record != nil {
		t.Fatal("Record not nil after Release")
	}
	alloc.AssertSize(t, 0)
}

func TestReleaseIdempotent(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := newTestRecord(t, alloc)

	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: "w"}
	b.Release()
	b.Release()

	alloc.AssertSize(t, 0)
}

func TestBatchTable(t *testing.T) {
	alloc := memory.NewGoAllocator()
	rec := newTestRecord(t, alloc)
	defer rec.Release()

	b := &dataplane.Batch{Table: "orders", Record: rec, Watermark: "offset-42"}
	if b.Table != "orders" {
		t.Errorf("Table = %q, want orders", b.Table)
	}
	if b.Watermark != "offset-42" {
		t.Errorf("Watermark = %v, want offset-42", b.Watermark)
	}
}

func TestBatchWatermarkTypes(t *testing.T) {
	alloc := memory.NewGoAllocator()
	rec := newTestRecord(t, alloc)
	defer rec.Release()

	for _, wm := range []any{"gtid-1", []byte{0x01, 0x02}, int64(999)} {
		b := &dataplane.Batch{Table: "t", Record: rec, Watermark: wm}
		if b.Watermark == nil {
			t.Errorf("Watermark nil for %T", wm)
		}
	}
}

func TestBatchOwnershipTransfer(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := newTestRecord(t, alloc)

	producer := func() *dataplane.Batch {
		return &dataplane.Batch{Table: "t", Record: rec, Watermark: "w"}
	}

	b := producer()
	b.Release()

	alloc.AssertSize(t, 0)
}

func TestBatchRetainBeforeEscape(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	rec := newTestRecord(t, alloc)

	// Simulate: flight reader reuses buffer. Retain before the batch
	// escapes the read loop (CR-069 §1.2).
	rec.Retain()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: "w"}

	// Reader releases its copy.
	rec.Release()

	if b.Record.NumRows() != 1 {
		t.Errorf("NumRows = %d, want 1 after Retain/Release", b.Record.NumRows())
	}

	b.Release()
	alloc.AssertSize(t, 0)
}
