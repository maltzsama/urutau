package mysql

import (
	"testing"
	"time"
)

// go-sql-driver returns an UNSIGNED BIGINT three ways: uint64 over the text
// protocol (a query without args), and over the binary protocol (a query
// with args, as every bounded chunk is) int64 when it fits in 63 bits and
// its decimal text above that. Bounds and snapshot rows must come out as
// uint64 whichever protocol served them, or one chunk's bounds mix int64 with
// uint64 and the codec rejects rows above 2^63.
func TestNormalizeSnapshotUnsignedBigint(t *testing.T) {
	cases := []struct {
		in   any
		want uint64
	}{
		{uint64(18446744073709551615), 18446744073709551615},
		{int64(42), 42},
		{[]byte("9223372036854775808"), 9223372036854775808},
		{"18446744073709551615", 18446744073709551615},
	}
	for _, c := range cases {
		got := normalizeSnapshot(c.in, "UNSIGNED BIGINT", time.UTC)
		if u, ok := got.(uint64); !ok || u != c.want {
			t.Errorf("normalizeSnapshot(%#v) = %#v (%T), want uint64 %d", c.in, got, got, c.want)
		}
	}
	// Signed and other types are untouched.
	if got := normalizeSnapshot(int64(-5), "BIGINT", time.UTC); got != int64(-5) {
		t.Errorf("BIGINT: got %#v", got)
	}
	if got := normalizeSnapshot([]byte("x"), "VARCHAR", time.UTC); got != "x" {
		t.Errorf("VARCHAR: got %#v", got)
	}
	if got := normalizeSnapshot(nil, "UNSIGNED BIGINT", time.UTC); got != nil {
		t.Errorf("NULL: got %#v", got)
	}
}
