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
	b := &dataplane.Batch{Table: "t", Watermark: []byte("w")}
	b.Release()
}

func TestReleaseFreesRecord(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := newTestRecord(t, alloc)

	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}
	b.Release()

	if b.Record != nil {
		t.Fatal("Record not nil after Release")
	}
}

func TestReleaseIdempotent(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := newTestRecord(t, alloc)

	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}
	b.Release()
	b.Release()
}

func TestBatchTable(t *testing.T) {
	alloc := memory.NewGoAllocator()
	rec := newTestRecord(t, alloc)
	defer rec.Release()

	b := &dataplane.Batch{Table: "orders", Record: rec, Watermark: []byte("offset-42")}
	if b.Table != "orders" {
		t.Errorf("Table = %q, want orders", b.Table)
	}
	if string(b.Watermark) != "offset-42" {
		t.Errorf("Watermark = %q, want offset-42", b.Watermark)
	}
}

func TestBatchOwnershipTransfer(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := newTestRecord(t, alloc)

	producer := func() *dataplane.Batch {
		return &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}
	}

	b := producer()
	b.Release()
}

func TestBatchRetainBeforeEscape(t *testing.T) {
	alloc := checkedAlloc(t)
	rec := newTestRecord(t, alloc)

	rec.Retain()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}

	rec.Release()

	if b.Record.NumRows() != 1 {
		t.Errorf("NumRows = %d, want 1 after Retain/Release", b.Record.NumRows())
	}

	b.Release()
}
