package iceberg

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// #197: appendColumn rejects a fixed(L) value whose length differs from L,
// instead of letting FixedSizeBinaryBuilder.Append panic.
func TestAppendColumnFixedSizeBinaryLength(t *testing.T) {
	dt := &arrow.FixedSizeBinaryType{ByteWidth: 4}
	b := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	field := arrow.Field{Name: "x", Type: dt}

	if err := appendColumn(b, field, []any{[]byte{1, 2, 3}}); err == nil {
		t.Fatal("a wrong-length fixed-size binary value must error")
	}
	if err := appendColumn(b, field, []any{[]byte{1, 2, 3, 4}}); err != nil {
		t.Fatalf("a correct-length value must pass: %v", err)
	}
}
