package transport

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
)

// The live MySQL decoder hands unsigned integers over as uint8/uint16/uint32
// /uint64. An unsigned column cast to int64 or uint64 must encode them.
func TestAppendUnsignedIntegers(t *testing.T) {
	i64 := array.NewInt64Builder(memory.DefaultAllocator)
	defer i64.Release()
	for _, v := range []any{uint8(1), uint16(2), uint32(4000000000), uint64(math.MaxInt64), uint(5)} {
		if err := appendTypedValue(i64, core.ColumnType{Kind: core.KindInt64}, v); err != nil {
			t.Errorf("int64 ← %T %v: %v", v, v, err)
		}
	}
	if err := appendTypedValue(i64, core.ColumnType{Kind: core.KindInt64}, uint64(math.MaxInt64)+1); err == nil {
		t.Error("int64 ← 2^63 must error, not wrap")
	}
	u64 := array.NewUint64Builder(memory.DefaultAllocator)
	defer u64.Release()
	for _, v := range []any{uint8(1), uint16(2), uint32(4000000000), uint(5)} {
		if err := appendTypedValue(u64, core.ColumnType{Kind: core.KindUInt64}, v); err != nil {
			t.Errorf("uint64 ← %T %v: %v", v, v, err)
		}
	}
}
