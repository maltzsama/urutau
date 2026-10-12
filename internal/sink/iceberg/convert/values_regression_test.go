package convert

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// appendColumn's Int32Builder branch used to append int/int64 with a bare
// int32(t): an out-of-range key wrapped negative and the equality delete was
// issued for the wrong key. It must reject the overflow instead.
func TestAppendColumnRejectsInt32Overflow(t *testing.T) {
	field := arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32}

	b := array.NewInt32Builder(memory.DefaultAllocator)
	defer b.Release()
	if err := AppendColumn(b, field, []any{int64(3_000_000_000)}); err == nil {
		t.Fatal("appendColumn(int64 3_000_000_000) = nil, want an overflow error")
	}

	// In-range values still append.
	if err := AppendColumn(b, field, []any{int64(42), int(7)}); err != nil {
		t.Fatalf("appendColumn(in range): %v", err)
	}
	arr := b.NewInt32Array()
	defer arr.Release()
	if arr.Value(0) != 42 || arr.Value(1) != 7 {
		t.Fatalf("values = %d, %d; want 42, 7", arr.Value(0), arr.Value(1))
	}
}
