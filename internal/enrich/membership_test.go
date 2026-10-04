package enrich

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestMembershipMaskMatchesIndexBuilderInt32 guards #640: the reference index
// builder and the batch lookup must read a key column identically. The old
// builder (arrowValueAt) had no Int32 case and read every non-null Int32 key as
// nil, so an Int32 join key never matched. Both now use readArrowValue.
func TestMembershipMaskMatchesIndexBuilderInt32(t *testing.T) {
	b := array.NewInt32Builder(memory.DefaultAllocator)
	b.AppendValues([]int32{1, 2, 3}, nil)
	keys := b.NewInt32Array()
	b.Release()
	defer keys.Release()

	// Build the index exactly as buildImage does.
	ki := make(map[any]int32, 3)
	for i := 0; i < 3; i++ {
		ki[normalizeKey(readArrowValue(keys, i))] = int32(i)
	}
	snap := &snapshot{keyIndex: ki}

	m, err := membershipMask(memory.DefaultAllocator, keys, snap)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Release()
	for i := 0; i < 3; i++ {
		if !m.Value(i) {
			t.Fatalf("row %d: an Int32 key present in the index must be a hit", i)
		}
	}
}
