package core

import (
	"math"
	"testing"
)

func TestAsInt64(t *testing.T) {
	for _, c := range []struct {
		in   any
		want int64
		ok   bool
	}{
		{int64(-5), -5, true}, {int32(-5), -5, true}, {int16(-5), -5, true}, {int8(-5), -5, true}, {-5, -5, true},
		{uint64(7), 7, true}, {uint32(7), 7, true}, {uint16(7), 7, true}, {uint8(7), 7, true}, {uint(7), 7, true},
		{uint64(math.MaxInt64), math.MaxInt64, true},
		{uint64(math.MaxInt64) + 1, 0, false},
		{float64(1), 0, false}, {"1", 0, false}, {[]byte("1"), 0, false}, {nil, 0, false},
	} {
		got, ok := AsInt64(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("AsInt64(%T %v) = %d, %v; want %d, %v", c.in, c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestAsUint64(t *testing.T) {
	for _, c := range []struct {
		in   any
		want uint64
		ok   bool
	}{
		{uint64(math.MaxUint64), math.MaxUint64, true}, {uint8(3), 3, true},
		{int64(3), 3, true}, {int8(0), 0, true}, {3, 3, true},
		{int64(-1), 0, false}, {int8(-1), 0, false},
		{float64(1), 0, false}, {"1", 0, false}, {nil, 0, false},
	} {
		got, ok := AsUint64(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("AsUint64(%T %v) = %d, %v; want %d, %v", c.in, c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestAsFloat64(t *testing.T) {
	for _, c := range []struct {
		in   any
		want float64
		ok   bool
	}{
		{float64(1.5), 1.5, true}, {float32(1.5), 1.5, true},
		{int64(-2), -2, true}, {int16(-2), -2, true}, {uint64(math.MaxUint64), math.MaxUint64, true},
		{"1", 0, false}, {[]byte("1"), 0, false}, {nil, 0, false},
	} {
		got, ok := AsFloat64(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("AsFloat64(%T %v) = %v, %v; want %v, %v", c.in, c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestParseInt64(t *testing.T) {
	if got, err := ParseInt64([]byte("-42")); err != nil || got != -42 {
		t.Fatalf("ParseInt64(-42) = %d, %v", got, err)
	}
	for _, bad := range []string{"", "3.14", "x", "9223372036854775808"} {
		if _, err := ParseInt64([]byte(bad)); err == nil {
			t.Errorf("ParseInt64(%q): want an error", bad)
		}
	}
}
