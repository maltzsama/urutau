package core

import (
	"math"
	"testing"
)

// go-mysql delivers an unsigned MySQL integer as uint8, uint16, uint32 or
// uint64 (canal's handleUnsigned); the snapshot delivers int64 or uint64.
// The source column is KindUnknown, so the declared cast is what lands it.
func TestConvertUnsignedToUInt64(t *testing.T) {
	tgt := CastTarget{Type: ColumnType{Kind: KindUInt64}}
	cases := []struct {
		in   any
		want uint64
	}{
		{uint8(200), 200},
		{uint16(60000), 60000},
		{uint32(4000000000), 4000000000},
		{uint64(math.MaxUint64), math.MaxUint64},
		{int64(42), 42},
		{int32(7), 7},
		{int(9), 9},
	}
	for _, c := range cases {
		got, err := tgt.Convert(KindUnknown, c.in)
		if err != nil || got != c.want {
			t.Errorf("Convert(%T %v → uint64) = %v (%T), %v; want %d", c.in, c.in, got, got, err, c.want)
		}
	}
	if got, err := tgt.Convert(KindUnknown, nil); err != nil || got != nil {
		t.Errorf("Convert(nil → uint64) = %v, %v", got, err)
	}
	if _, err := tgt.Convert(KindUnknown, int64(-1)); err == nil {
		t.Error("Convert(-1 → uint64) must error")
	}
}

func TestConvertUnsignedToInt64(t *testing.T) {
	tgt := CastTarget{Type: ColumnType{Kind: KindInt64}}
	cases := []struct {
		in   any
		want int64
	}{
		{uint8(200), 200},
		{uint16(60000), 60000},
		{uint32(4000000000), 4000000000},
		{uint64(math.MaxInt64), math.MaxInt64},
	}
	for _, c := range cases {
		got, err := tgt.Convert(KindUnknown, c.in)
		if err != nil || got != c.want {
			t.Errorf("Convert(%T %v → int64) = %v, %v; want %d", c.in, c.in, got, err, c.want)
		}
	}
	if _, err := tgt.Convert(KindUnknown, uint64(math.MaxInt64)+1); err == nil {
		t.Error("Convert(2^63 → int64) must error, not wrap")
	}
}

// The KindUnknown bypass must not accept a target Convert cannot produce:
// the cast would pass boot and fail on the first row.
func TestCheckCastUnknownRejectsUnconvertibleTargets(t *testing.T) {
	for _, k := range []Kind{KindBool, KindInt32, KindFloat32, KindDate, KindTime} {
		if err := CheckCast(ColumnType{Kind: KindUnknown}, CastTarget{Type: ColumnType{Kind: k}}); err == nil {
			t.Errorf("CheckCast(unknown → %s) = nil, want an error", k)
		}
	}
	for _, k := range []Kind{KindString, KindInt64, KindUInt64, KindFloat64, KindBinary, KindJSON} {
		if err := CheckCast(ColumnType{Kind: KindUnknown}, CastTarget{Type: ColumnType{Kind: k}}); err != nil {
			t.Errorf("CheckCast(unknown → %s) = %v, want nil", k, err)
		}
	}
}

func TestConvertUnsignedToDecimalStringFloat(t *testing.T) {
	dec := CastTarget{Type: ColumnType{Kind: KindDecimal, Precision: 20, Scale: 0}}
	if got, err := dec.Convert(KindUnknown, uint64(math.MaxUint64)); err != nil || got != "18446744073709551615" {
		t.Errorf("Convert(MaxUint64 → decimal(20,0)) = %v, %v", got, err)
	}
	str := CastTarget{Type: ColumnType{Kind: KindString}}
	if got, err := str.Convert(KindUnknown, uint32(4000000000)); err != nil || got != "4000000000" {
		t.Errorf("Convert(uint32 → string) = %v, %v", got, err)
	}
	f := CastTarget{Type: ColumnType{Kind: KindFloat64}}
	if got, err := f.Convert(KindUnknown, uint16(7)); err != nil || got != float64(7) {
		t.Errorf("Convert(uint16 → float64) = %v, %v", got, err)
	}
}
