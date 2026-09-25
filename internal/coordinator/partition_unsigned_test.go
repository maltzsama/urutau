package coordinator

import (
	"math"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// A BIGINT UNSIGNED key arrives from transport.BatchReader as uint64
// (core.KindUInt64), while partition bounds may be int64 or uint64. Both must
// compare numerically, including values at and above 2^63.
func TestComparePKOrdersUnsignedKeys(t *testing.T) {
	const big = uint64(math.MaxInt64) + 1
	cases := []struct {
		a, b []any
		want int
	}{
		{[]any{uint64(100)}, []any{int64(50)}, 1},
		{[]any{uint64(99)}, []any{int64(100)}, -1},
		{[]any{uint64(50)}, []any{int64(50)}, 0},
		{[]any{int64(50)}, []any{uint64(100)}, -1},
		{[]any{uint64(100)}, []any{uint64(99)}, 1},
		{[]any{uint64(0)}, []any{int64(-1)}, 1},
		{[]any{int64(-1)}, []any{uint64(0)}, -1},
		{[]any{big}, []any{int64(math.MaxInt64)}, 1},
		{[]any{int64(math.MaxInt64)}, []any{big}, -1},
		{[]any{big}, []any{big + 1}, -1},
		{[]any{uint64(math.MaxUint64)}, []any{uint64(math.MaxUint64 - 1)}, 1},
	}
	for _, c := range cases {
		got, err := comparePK(c.a, c.b)
		if err != nil {
			t.Fatalf("comparePK(%v, %v): %v", c.a, c.b, err)
		}
		if sign(got) != c.want {
			t.Errorf("comparePK(%v, %v) sign = %d, want %d", c.a, c.b, sign(got), c.want)
		}
	}
}

func TestPartitionOwnerRoutesUnsignedKeys(t *testing.T) {
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
		got, err := partitionOwner(c.ranges, []any{c.key})
		if err != nil {
			t.Fatalf("%s: partitionOwner: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: partitionOwner(key %d) = %d, want %d", c.name, c.key, got, c.want)
		}
	}
}

// A key type the routing has no ordering for must fail, not fall back to a
// lexical comparison of fmt.Sprint output.
func TestComparePKRejectsUnorderedTypes(t *testing.T) {
	cases := []struct{ a, b any }{
		{int64(1), "1"},
		{"100", int64(50)},
		{uint64(1), []byte("1")},
		{float64(1), "1"},
		{struct{}{}, struct{}{}},
	}
	for _, c := range cases {
		if _, err := comparePK([]any{c.a}, []any{c.b}); err == nil {
			t.Errorf("comparePK(%T, %T): want an error", c.a, c.b)
		}
	}
}

func TestRequireOrderableRanges(t *testing.T) {
	intRanges := []source.Chunk{{High: []any{int64(50)}}, {Low: []any{int64(50)}}}
	uintRanges := []source.Chunk{{High: []any{uint64(50)}}, {Low: []any{uint64(50)}}}
	strRanges := []source.Chunk{{High: []any{"m"}}, {Low: []any{"m"}}}
	schema := func(k core.Kind) core.Schema {
		return core.Schema{Columns: []core.Column{{Name: "id", Type: core.ColumnType{Kind: k}}}, PrimaryKey: []string{"id"}}
	}
	cases := []struct {
		name    string
		kind    core.Kind
		ranges  []source.Chunk
		wantErr bool
	}{
		{"int64 key, int64 bounds", core.KindInt64, intRanges, false},
		{"uint64 key (cast), int64 bounds", core.KindUInt64, intRanges, false},
		{"int64 key (cast), uint64 bounds", core.KindInt64, uintRanges, false},
		{"float key, int64 bounds", core.KindFloat64, intRanges, false},
		{"string key, string bounds", core.KindString, strRanges, false},
		{"string key (cast), int64 bounds", core.KindString, intRanges, true},
		{"decimal key (cast), int64 bounds", core.KindDecimal, intRanges, true},
		{"int64 key, string bounds", core.KindInt64, strRanges, true},
		{"timestamp key, unpartitioned", core.KindTimestamp, []source.Chunk{{}}, false},
	}
	tbl := spec.Table{Target: "t"}
	for _, c := range cases {
		err := requireOrderableRanges(tbl, schema(c.kind), []string{"id"}, c.ranges)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", c.name, err, c.wantErr)
		}
	}
}
