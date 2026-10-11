package convert

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// #201.1: an int64 beyond 2^53 cannot be represented exactly as float64 and
// must be rejected instead of rounded silently.
func TestAppendColumnInt64Float64Precision(t *testing.T) {
	b := array.NewFloat64Builder(memory.DefaultAllocator)
	defer b.Release()
	field := arrow.Field{Name: "f", Type: arrow.PrimitiveTypes.Float64}

	if err := AppendColumn(b, field, []any{maxExactFloat64}); err != nil {
		t.Fatalf("2^53 is exactly representable, got %v", err)
	}
	if err := AppendColumn(b, field, []any{-maxExactFloat64}); err != nil {
		t.Fatalf("-2^53 is exactly representable, got %v", err)
	}
	if err := AppendColumn(b, field, []any{maxExactFloat64 + 1}); err == nil {
		t.Fatal("2^53+1 loses precision as float64, want an error")
	}
	if err := AppendColumn(b, field, []any{-maxExactFloat64 - 1}); err == nil {
		t.Fatal("-2^53-1 loses precision as float64, want an error")
	}
}
