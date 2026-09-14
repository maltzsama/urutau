package mysql

import (
	"context"
	"math/big"
	"testing"
)

func TestPartitionsUnpartitionedReturnsOneOpenRange(t *testing.T) {
	c := &Chunker{pk: []string{"id"}}
	for _, n := range []int{0, 1} {
		got, err := c.Partitions(context.Background(), n)
		if err != nil {
			t.Fatalf("Partitions(%d): %v", n, err)
		}
		if len(got) != 1 || got[0].Low != nil || got[0].High != nil {
			t.Fatalf("Partitions(%d) = %+v, want one unbounded range", n, got)
		}
	}
}

func TestPartitionsRejectsCompositePK(t *testing.T) {
	c := &Chunker{pk: []string{"order_id", "line_no"}, schema: "s", table: "t"}
	_, err := c.Partitions(context.Background(), 3)
	if err == nil {
		t.Fatal("Partitions: want error for a composite primary key")
	}
}

func TestPartitionNumericEvenSplit(t *testing.T) {
	c := &Chunker{}
	ranges, err := c.partitionNumeric(int64(0), int64(99), 4)
	if err != nil {
		t.Fatalf("partitionNumeric: %v", err)
	}
	if len(ranges) != 4 {
		t.Fatalf("got %d ranges, want 4", len(ranges))
	}
	// First range's Low is unbounded (covers anything below, including a
	// late-arriving value under the observed min); last range's High is
	// unbounded (covers anything above the observed max).
	if ranges[0].Low != nil {
		t.Fatalf("first range Low = %v, want nil (unbounded)", ranges[0].Low)
	}
	if ranges[len(ranges)-1].High != nil {
		t.Fatalf("last range High = %v, want nil (unbounded)", ranges[len(ranges)-1].High)
	}
	// Every interior boundary's High must equal the next range's Low —
	// the ranges must be contiguous with no gap and no overlap.
	for i := 0; i < len(ranges)-1; i++ {
		hi := ranges[i].High
		lo := ranges[i+1].Low
		if len(hi) != 1 || len(lo) != 1 || hi[0] != lo[0] {
			t.Fatalf("range %d.High = %v does not match range %d.Low = %v", i, hi, i+1, lo)
		}
	}
}

func TestPartitionNumericCoversFullDomain(t *testing.T) {
	// Every integer in [min, max] must fall in exactly one range — the
	// property that makes bootstrap and live-stream routing agree.
	c := &Chunker{}
	const min, max, n = 0, 1000, 7
	ranges, err := c.partitionNumeric(int64(min), int64(max), n)
	if err != nil {
		t.Fatalf("partitionNumeric: %v", err)
	}
	owner := func(v int64) int {
		for i, r := range ranges {
			if r.Low != nil && v < r.Low[0].(int64) {
				continue
			}
			if r.High != nil && v >= r.High[0].(int64) {
				continue
			}
			return i
		}
		return -1
	}
	for v := int64(min); v <= max; v++ {
		if got := owner(v); got == -1 {
			t.Fatalf("value %d owned by no partition (ranges=%+v)", v, ranges)
		}
	}
}

func TestPartitionNumericRejectsMaxLessThanMin(t *testing.T) {
	c := &Chunker{}
	if _, err := c.partitionNumeric(int64(10), int64(5), 3); err == nil {
		t.Fatal("partitionNumeric: want error when max < min")
	}
}

func TestEncodeDecodeCharsetStringRoundTrips(t *testing.T) {
	for _, s := range []string{"a", "abc", "ZZZ", "hello world", "0123"} {
		enc, err := encodeCharsetString(s)
		if err != nil {
			t.Fatalf("encode(%q): %v", s, err)
		}
		dec := decodeCharsetString(enc)
		if dec != s {
			t.Fatalf("decode(encode(%q)) = %q, want %q", s, dec, s)
		}
	}
}

func TestEncodeCharsetStringOrderingMatchesLexical(t *testing.T) {
	// The whole partitioning scheme depends on the encoded big.Int
	// preserving the same order as MySQL's own string comparison for
	// same-length padded strings — this is the property that makes
	// "split the numeric space evenly" a valid proxy for "split the key
	// space evenly."
	pairs := [][2]string{
		{"aaa", "aab"},
		{"aay", "aba"},
		{"000", "001"},
		{"Az9", "Aza"},
	}
	for _, p := range pairs {
		lo, err := encodeCharsetString(p[0])
		if err != nil {
			t.Fatalf("encode(%q): %v", p[0], err)
		}
		hi, err := encodeCharsetString(p[1])
		if err != nil {
			t.Fatalf("encode(%q): %v", p[1], err)
		}
		if lo.Cmp(hi) >= 0 {
			t.Fatalf("encode(%q)=%v is not < encode(%q)=%v", p[0], lo, p[1], hi)
		}
	}
}

func TestEncodeCharsetStringRejectsUnsupportedRune(t *testing.T) {
	if _, err := encodeCharsetString("héllo"); err == nil {
		t.Fatal("encodeCharsetString: want error for a rune outside the charset (é)")
	}
}

func TestPadRight(t *testing.T) {
	got := padRight("ab", 5)
	if len(got) != 5 || got[:2] != "ab" {
		t.Fatalf("padRight(%q, 5) = %q", "ab", got)
	}
	if got := padRight("abcde", 3); got != "abcde" {
		t.Fatalf("padRight should not truncate: got %q", got)
	}
}

func TestToInt64(t *testing.T) {
	cases := []struct {
		in   any
		want int64
	}{
		{int64(5), 5}, {int32(5), 5}, {int(5), 5}, {[]byte("42"), 42},
	}
	for _, c := range cases {
		got, err := toInt64(c.in)
		if err != nil {
			t.Fatalf("toInt64(%v): %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("toInt64(%v) = %d, want %d", c.in, got, c.want)
		}
	}
	if _, err := toInt64(3.14); err == nil {
		t.Fatal("toInt64: want error for an unsupported type")
	}
}

func TestBoundTuple(t *testing.T) {
	if got := boundTuple(nil); got != nil {
		t.Fatalf("boundTuple(nil) = %v, want nil", got)
	}
	got := boundTuple(int64(7))
	if len(got) != 1 || got[0] != int64(7) {
		t.Fatalf("boundTuple(7) = %v, want [7]", got)
	}
}

func TestCharsetBaseSanity(t *testing.T) {
	if charsetBase.Cmp(big.NewInt(1)) <= 0 {
		t.Fatal("charsetBase must be > 1 for encoding to be meaningful")
	}
}
