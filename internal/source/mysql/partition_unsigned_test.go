package mysql

import (
	"math"
	"testing"
)

// go-sql-driver returns an unsigned BIGINT as uint64 (text protocol, the
// minMax query), including values above math.MaxInt64. The bounds must stay
// uint64, contiguous, and cover the observed domain without wrapping.
func TestPartitionNumericUnsigned(t *testing.T) {
	cases := []struct {
		name     string
		min, max uint64
		n        int
	}{
		{"small", 0, 99, 4},
		{"above MaxInt64", math.MaxInt64 - 10, math.MaxInt64 + 1000, 3},
		{"full domain", 0, math.MaxUint64, 4},
		{"more partitions than values", 5, 6, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ranges, err := (&Chunker{}).partitionNumeric(c.min, c.max, c.n)
			if err != nil {
				t.Fatalf("partitionNumeric: %v", err)
			}
			if len(ranges) != c.n {
				t.Fatalf("got %d ranges, want %d", len(ranges), c.n)
			}
			if ranges[0].Low != nil || ranges[c.n-1].High != nil {
				t.Fatalf("outer bounds must be open: %+v", ranges)
			}
			prev := c.min
			for i := 0; i < c.n-1; i++ {
				hi, ok := ranges[i].High[0].(uint64)
				if !ok {
					t.Fatalf("range %d High = %T, want uint64", i, ranges[i].High[0])
				}
				lo, ok := ranges[i+1].Low[0].(uint64)
				if !ok || lo != hi {
					t.Fatalf("range %d.High = %v does not match range %d.Low = %v", i, hi, i+1, ranges[i+1].Low)
				}
				if hi < prev || hi > c.max {
					t.Fatalf("boundary %d = %d out of order or past max %d (ranges=%+v)", i, hi, c.max, ranges)
				}
				prev = hi
			}
		})
	}
}

// Every value in a small [min, max] must fall in exactly one range, however
// the partition count compares with the domain size.
func TestPartitionNumericUnsignedOwnsEveryValueOnce(t *testing.T) {
	for _, n := range []int{2, 3, 4, 7, 12} {
		for _, dom := range [][2]uint64{{0, 5}, {0, 0}, {3, 4}, {10, 40}} {
			ranges, err := (&Chunker{}).partitionNumeric(dom[0], dom[1], n)
			if err != nil {
				t.Fatalf("n=%d %v: %v", n, dom, err)
			}
			for v := dom[0]; v <= dom[1]+2; v++ {
				owners := 0
				for _, r := range ranges {
					if (r.Low == nil || v >= r.Low[0].(uint64)) && (r.High == nil || v < r.High[0].(uint64)) {
						owners++
					}
				}
				if owners != 1 {
					t.Fatalf("n=%d %v: value %d owned by %d ranges (ranges=%+v)", n, dom, v, owners, ranges)
				}
			}
		}
	}
}

// The signed path had the same flaw before issue #406: with more partitions
// than key values it emitted several open-ended ranges, so the snapshot sent
// one row to several workers.
func TestPartitionNumericSignedOwnsEveryValueOnce(t *testing.T) {
	for _, n := range []int{2, 3, 4, 7, 12} {
		for _, dom := range [][2]int64{{0, 5}, {5, 6}, {-3, 3}, {10, 40}, {-7, -7}} {
			ranges, err := (&Chunker{}).partitionNumeric(dom[0], dom[1], n)
			if err != nil {
				t.Fatalf("n=%d %v: %v", n, dom, err)
			}
			if len(ranges) != n {
				t.Fatalf("n=%d %v: got %d ranges", n, dom, len(ranges))
			}
			for v := dom[0] - 2; v <= dom[1]+2; v++ {
				owners := 0
				for _, r := range ranges {
					if (r.Low == nil || v >= r.Low[0].(int64)) && (r.High == nil || v < r.High[0].(int64)) {
						owners++
					}
				}
				if owners != 1 {
					t.Fatalf("n=%d %v: value %d owned by %d ranges (ranges=%+v)", n, dom, v, owners, ranges)
				}
			}
		}
	}
}

func TestPartitionNumericSignedFullDomain(t *testing.T) {
	ranges, err := (&Chunker{}).partitionNumeric(int64(math.MinInt64), int64(math.MaxInt64), 4)
	if err != nil {
		t.Fatalf("partitionNumeric: %v", err)
	}
	prev := int64(math.MinInt64)
	for i := 0; i < len(ranges)-1; i++ {
		hi := ranges[i].High[0].(int64)
		if hi <= prev {
			t.Fatalf("boundary %d = %d does not advance past %d (ranges=%+v)", i, hi, prev, ranges)
		}
		prev = hi
	}
}

func TestPartitionNumericRejectsMixedSignedness(t *testing.T) {
	if _, err := (&Chunker{}).partitionNumeric(uint64(1), int64(5), 2); err == nil {
		t.Fatal("partitionNumeric: want an error for a uint64 min with an int64 max")
	}
}
