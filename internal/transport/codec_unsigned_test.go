package transport

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
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

// An unsigned column cast to float64 or decimal is encoded on the wire with
// that kind, so those branches must take the decoder's unsigned types too.
func TestAppendUnsignedToFloatAndDecimal(t *testing.T) {
	f := array.NewFloat64Builder(memory.DefaultAllocator)
	defer f.Release()
	for _, v := range []any{uint8(1), uint16(2), uint32(4000000000), uint(5), uint64(1) << 53} {
		if err := appendTypedValue(f, core.ColumnType{Kind: core.KindFloat64}, v); err != nil {
			t.Errorf("float64 ← %T %v: %v", v, v, err)
		}
	}
	for _, v := range []any{uint64(1)<<53 + 1, uint64(math.MaxUint64)} {
		if err := appendTypedValue(f, core.ColumnType{Kind: core.KindFloat64}, v); err == nil {
			t.Errorf("float64 ← %d must error: it loses precision", v)
		}
	}
	d := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 20, Scale: 0})
	defer d.Release()
	for _, v := range []any{uint8(1), uint32(4000000000), uint64(math.MaxUint64)} {
		if err := appendTypedValue(d, core.ColumnType{Kind: core.KindDecimal, Precision: 20, Scale: 0}, v); err != nil {
			t.Errorf("decimal(20,0) ← %T %v: %v", v, v, err)
		}
	}
	arr := d.NewDecimal128Array()
	defer arr.Release()
	if got := arr.ValueStr(2); got != "18446744073709551615" {
		t.Errorf("decimal(20,0) of MaxUint64 = %s", got)
	}
}
