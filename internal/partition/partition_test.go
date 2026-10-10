package partition

import (
	"math"
	"testing"

	"github.com/maltzsama/urutau/source"
)

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// TestCompareScalarOrdersKeys pins the ordering for every key type a
// partition bound can carry, including the cross-signedness and cross-width
// integer cases a float64 round-trip would collapse.
func TestCompareScalarOrdersKeys(t *testing.T) {
	const big = uint64(math.MaxInt64) + 1 // beyond int64 range
	cases := []struct {
		name string
		a, b any
		want int
	}{
		{"int32 below", int32(1), int32(2), -1},
		{"int32 equal", int32(2), int32(2), 0},
		{"int32 above", int32(3), int32(2), 1},
		{"int64 below", int64(-5), int64(5), -1},
		{"int64 adjacent above 2^53", int64(1<<53 + 1), int64(1 << 53), 1},
		{"uint64 below", uint64(99), uint64(100), -1},
		{"uint64 above 2^63", big, uint64(math.MaxInt64), 1},
		{"int64 vs uint64 negative", int64(-1), uint64(0), -1},
		{"uint64 vs int64 positive", uint64(100), int64(50), 1},
		{"float below", float64(1.5), float64(2.0), -1},
		{"float equal", float64(2.0), float64(2.0), 0},
		{"float vs int", float64(1.5), int64(2), -1},
		{"string below", "aaa", "aab", -1},
		{"string equal", "x", "x", 0},
		{"bytes below", []byte{0x00}, []byte{0x01}, -1},
		{"string vs bytes", "abc", []byte("abc"), 0},
	}
	for _, c := range cases {
		got, err := compareScalar(c.a, c.b)
		if err != nil {
			t.Fatalf("%s: compareScalar(%v, %v): %v", c.name, c.a, c.b, err)
		}
		if sign(got) != c.want {
			t.Errorf("%s: compareScalar(%v, %v) sign = %d, want %d", c.name, c.a, c.b, sign(got), c.want)
		}
	}
}

// TestCompareScalarRejectsUnorderedPairs: a pair with no common ordering must
// fail, never fall back to a lexical comparison of fmt.Sprint output.
func TestCompareScalarRejectsUnorderedPairs(t *testing.T) {
	cases := []struct{ a, b any }{
		{int64(1), "1"},
		{"100", int64(50)},
		{uint64(1), []byte("1")},
		{float64(1), "1"},
		{struct{}{}, struct{}{}},
	}
	for _, c := range cases {
		if _, err := compareScalar(c.a, c.b); err == nil {
			t.Errorf("compareScalar(%T, %T): want an error", c.a, c.b)
		}
	}
}

func TestComparePKOrdersTuplesLexicographically(t *testing.T) {
	cases := []struct {
		a, b []any
		want int
	}{
		{[]any{int64(1)}, []any{int64(2)}, -1},
		{[]any{int64(5)}, []any{int64(5)}, 0},
		{[]any{int64(9)}, []any{int64(2)}, 1},
		{[]any{int64(1), int64(9)}, []any{int64(1), int64(5)}, 1}, // first column decides
		{[]any{int64(1)}, []any{int64(1), int64(0)}, -1},          // shorter tuple is smaller
	}
	for _, c := range cases {
		got, err := ComparePK(c.a, c.b)
		if err != nil {
			t.Fatalf("ComparePK(%v, %v): %v", c.a, c.b, err)
		}
		if sign(got) != c.want {
			t.Errorf("ComparePK(%v, %v) sign = %d, want %d", c.a, c.b, sign(got), c.want)
		}
	}
}

// TestOwnerUnboundedRanges routes with open Low/High bounds — the shape the
// single-range default and the outermost partitions carry.
func TestOwnerUnboundedRanges(t *testing.T) {
	// [-, 100), [100, 200), [200, -)
	ranges := []source.Chunk{
		{Low: nil, High: []any{int64(100)}},
		{Low: []any{int64(100)}, High: []any{int64(200)}},
		{Low: []any{int64(200)}, High: nil},
	}
	cases := []struct {
		key  int64
		want int
	}{
		{0, 0}, {99, 0}, {100, 1}, {150, 1}, {199, 1}, {200, 2}, {1000, 2},
	}
	for _, c := range cases {
		got, err := Owner(ranges, []any{c.key})
		if err != nil {
			t.Fatalf("Owner(key=%d): %v", c.key, err)
		}
		if got != c.want {
			t.Errorf("Owner(key=%d) = %d, want %d", c.key, got, c.want)
		}
	}
}

func TestOwnerSingleRangeAlwaysZero(t *testing.T) {
	ranges := []source.Chunk{{}}
	for _, key := range []any{int64(12345), uint64(99), "zzz", -7.5} {
		got, err := Owner(ranges, []any{key})
		if err != nil {
			t.Fatalf("Owner(%v): %v", key, err)
		}
		if got != 0 {
			t.Errorf("Owner(%v) with one unbounded range = %d, want 0", key, got)
		}
	}
}

// TestOwnerNoMatchReturnsNegativeOne: a gap in the ranges must not silently
// misroute; the caller turns the -1 into an error.
func TestOwnerNoMatchReturnsNegativeOne(t *testing.T) {
	ranges := []source.Chunk{
		{Low: nil, High: []any{int64(10)}},
		{Low: []any{int64(20)}, High: nil},
	}
	got, err := Owner(ranges, []any{int64(15)})
	if err != nil {
		t.Fatalf("Owner(key=15): %v", err)
	}
	if got != -1 {
		t.Fatalf("Owner(key=15) = %d, want -1 (gap between ranges)", got)
	}
}

// TestOwnerUnsignedKeysPastInt64Bound routes uint64 keys against int64 and
// uint64 bounds, including values at and above 2^63.
func TestOwnerUnsignedKeysPastInt64Bound(t *testing.T) {
	const big = uint64(math.MaxInt64) + 1
	cases := []struct {
		name   string
		ranges []source.Chunk
		key    uint64
		want   int
	}{
		{"int64 bound, key past it", []source.Chunk{{High: []any{int64(50)}}, {Low: []any{int64(50)}}}, 100, 1},
		{"int64 bound, key below it", []source.Chunk{{High: []any{int64(50)}}, {Low: []any{int64(50)}}}, 9, 0},
		{"uint64 bound above 2^63", []source.Chunk{{High: []any{big}}, {Low: []any{big}}}, big - 1, 0},
		{"key at a uint64 bound above 2^63", []source.Chunk{{High: []any{big}}, {Low: []any{big}}}, big, 1},
	}
	for _, c := range cases {
		got, err := Owner(c.ranges, []any{c.key})
		if err != nil {
			t.Fatalf("%s: Owner: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: Owner(key %d) = %d, want %d", c.name, c.key, got, c.want)
		}
	}
}

// TestOwnerPropagatesUnorderedKeyError: a key type with no ordering against the
// bounds fails the route instead of guessing a partition.
func TestOwnerPropagatesUnorderedKeyError(t *testing.T) {
	ranges := []source.Chunk{
		{Low: nil, High: []any{"m"}},
		{Low: []any{"m"}, High: nil},
	}
	if _, err := Owner(ranges, []any{int64(5)}); err == nil {
		t.Fatal("Owner(int key) against string bounds: want an error")
	}
}
