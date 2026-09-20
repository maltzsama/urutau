package iceberg

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// #188: a NULL primary-key value must not become a zero-value delete key.
func TestScalarValueNullKeyErrors(t *testing.T) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendNull()
	arr := b.NewInt64Array()
	defer arr.Release()
	if _, err := scalarValue(arr, 0); err == nil {
		t.Fatal("a NULL key value must error, not return the zero value")
	}
}
