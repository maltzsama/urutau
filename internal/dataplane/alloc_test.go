package dataplane_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// checkedAlloc returns a CheckedAllocator that automatically asserts
// zero leaked bytes on test teardown. Leak = red test.
func checkedAlloc(t *testing.T) *memory.CheckedAllocator {
	t.Helper()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	return alloc
}
