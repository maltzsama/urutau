package enrich

import (
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func benchInt64(n int) *array.Int64 {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.Reserve(n)
	for i := 0; i < n; i++ {
		b.Append(int64(i))
	}
	a := b.NewInt64Array()
	b.Release()
	return a
}

// BenchmarkMembershipMask measures the per-batch cost of the enrich join's
// membership check after #589. The old is_in kernel rebuilt an Arrow hash set
// of the whole reference key column on every call; on this box that was
// ~7 ms / ~9 MB per 2000-row batch at a 100k-row reference, and ~1.3 s /
// ~557 MB at 5M. membershipMask reuses the snapshot's keyIndex (built once per
// refresh) and only allocates the batch-size boolean mask.
func BenchmarkMembershipMask(b *testing.B) {
	const batchRows = 2000
	batch := benchInt64(batchRows)
	defer batch.Release()
	for _, refRows := range []int{1000, 100000, 5000000} {
		ki := make(map[any]int32, refRows)
		for i := 0; i < refRows; i++ {
			ki[normalizeKey(int64(i))] = int32(i)
		}
		snap := &snapshot{keyIndex: ki}
		b.Run(fmt.Sprintf("ref_%d", refRows), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, err := membershipMask(memory.DefaultAllocator, batch, snap)
				if err != nil {
					b.Fatal(err)
				}
				m.Release()
			}
		})
	}
}
