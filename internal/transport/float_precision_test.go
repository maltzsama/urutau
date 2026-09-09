package transport

// RV-08 / C-7: int -> float64 carries the same precision guard as int64.
// In 64-bit builds int IS int64 wide — values above 2^53 silently lose
// precision when the mantissa runs out.

import (
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

func TestCodecIntToFloat64PrecisionGuard(t *testing.T) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "f", Type: core.ColumnType{Kind: core.KindFloat64, Nullable: true}},
		},
		PrimaryKey: []string{},
	}

	cases := []struct {
		name string
		val  int
		want bool // true = must be accepted
	}{
		{"2^53 fits float64 exactly", 1 << 53, true},
		{"2^53+1 loses precision", (1 << 53) + 1, false},
		{"small value", 42, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := []rowchange.Change{
				{Op: rowchange.OpInsert, Table: "t", After: map[string]any{"f": tc.val}, Position: "p1"},
			}
			_, _, err := EncodeBatch(rows, schema, nil, nil)
			if tc.want && err != nil {
				t.Fatalf("value %d must be accepted, got: %v", tc.val, err)
			}
			if !tc.want && err == nil {
				t.Fatalf("value %d loses precision in float64 and must be rejected", tc.val)
			}
		})
	}
}
