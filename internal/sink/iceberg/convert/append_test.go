package convert

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// TestAppendColumnUnsupportedBuilder guards the default arm: a column type the
// writer has no rule for must fail rather than write garbage.
func TestAppendColumnUnsupportedBuilder(t *testing.T) {
	b := array.NewBuilder(memory.DefaultAllocator, arrow.FixedWidthTypes.Duration_s)
	defer b.Release()
	err := AppendColumn(b, arrow.Field{Name: "d", Type: arrow.FixedWidthTypes.Duration_s}, []any{int64(1)})
	if err == nil || !strings.Contains(err.Error(), "unsupported builder") {
		t.Fatalf("err = %v, want unsupported builder", err)
	}
}

func TestSortedKeys(t *testing.T) {
	got := SortedKeys(map[string]any{"c": 1, "a": 2, "b": 3})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if len(SortedKeys(map[string]any(nil))) != 0 {
		t.Fatal("nil map must yield no keys")
	}
}

func TestMapCanonicalTypeMatrix(t *testing.T) {
	cases := []struct {
		name string
		in   core.ColumnType
		want iceberg.Type
	}{
		{"bool", core.ColumnType{Kind: core.KindBool}, iceberg.PrimitiveTypes.Bool},
		{"int32", core.ColumnType{Kind: core.KindInt32}, iceberg.PrimitiveTypes.Int32},
		{"int64", core.ColumnType{Kind: core.KindInt64}, iceberg.PrimitiveTypes.Int64},
		{"uint64", core.ColumnType{Kind: core.KindUInt64}, iceberg.DecimalTypeOf(20, 0)},
		{"float32", core.ColumnType{Kind: core.KindFloat32}, iceberg.PrimitiveTypes.Float32},
		{"float64", core.ColumnType{Kind: core.KindFloat64}, iceberg.PrimitiveTypes.Float64},
		{"decimal", core.ColumnType{Kind: core.KindDecimal, Precision: 12, Scale: 3}, iceberg.DecimalTypeOf(12, 3)},
		{"string", core.ColumnType{Kind: core.KindString}, iceberg.PrimitiveTypes.String},
		{"json", core.ColumnType{Kind: core.KindJSON}, iceberg.PrimitiveTypes.String},
		{"binary", core.ColumnType{Kind: core.KindBinary}, iceberg.PrimitiveTypes.Binary},
		{"fixed", core.ColumnType{Kind: core.KindFixedBinary, FixedSize: 8}, iceberg.FixedTypeOf(8)},
		{"date", core.ColumnType{Kind: core.KindDate}, iceberg.PrimitiveTypes.Date},
		{"time", core.ColumnType{Kind: core.KindTime}, iceberg.PrimitiveTypes.Time},
		{"timestamp", core.ColumnType{Kind: core.KindTimestamp}, iceberg.PrimitiveTypes.Timestamp},
		{"timestamptz", core.ColumnType{Kind: core.KindTimestampTZ}, iceberg.PrimitiveTypes.TimestampTz},
		{"uuid", core.ColumnType{Kind: core.KindUUID}, iceberg.PrimitiveTypes.UUID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mapCanonicalType(tc.in)
			if err != nil {
				t.Fatalf("mapCanonicalType: %v", err)
			}
			if !got.Equals(tc.want) {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestMapCanonicalTypeErrors(t *testing.T) {
	cases := []struct {
		name string
		in   core.ColumnType
	}{
		{"decimal-zero-precision", core.ColumnType{Kind: core.KindDecimal}},
		{"decimal-negative-precision", core.ColumnType{Kind: core.KindDecimal, Precision: -1}},
		{"fixed-zero-size", core.ColumnType{Kind: core.KindFixedBinary}},
		{"unsupported", core.ColumnType{Kind: core.KindUnknown}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := mapCanonicalType(tc.in); err == nil {
				t.Fatalf("mapCanonicalType(%v) must fail", tc.in.Kind)
			}
		})
	}
}

func TestIcebergFromCanonicalTypeCompositeErrors(t *testing.T) {
	next := 1
	if _, err := icebergFromCanonicalType(core.ColumnType{Kind: core.KindList}, &next); err == nil {
		t.Fatal("list without element type must fail")
	}
	if _, err := icebergFromCanonicalType(core.ColumnType{Kind: core.KindMap, KeyType: &core.ColumnType{Kind: core.KindString}}, &next); err == nil {
		t.Fatal("map without value type must fail")
	}
	if _, err := icebergFromCanonicalType(core.ColumnType{Kind: core.KindMap, ValueType: &core.ColumnType{Kind: core.KindString}}, &next); err == nil {
		t.Fatal("map without key type must fail")
	}
	bad := core.ColumnType{
		Kind:   core.KindStruct,
		Fields: []core.Column{{Name: "x", Type: core.ColumnType{Kind: core.KindDecimal}}},
	}
	if _, err := icebergFromCanonicalType(bad, &next); err == nil {
		t.Fatal("bad struct field must propagate")
	}
}

func TestFromCanonicalWrapsColumnError(t *testing.T) {
	_, err := FromCanonical(core.Schema{Columns: []core.Column{
		{Name: "amount", Type: core.ColumnType{Kind: core.KindDecimal}},
	}})
	if err == nil || !strings.Contains(err.Error(), "amount") {
		t.Fatalf("err = %v, want it to name the column", err)
	}
}

func TestSplitByOpErrors(t *testing.T) {
	ctx := context.Background()

	missing := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	missing.Field(0).(*array.Int64Builder).Append(1)
	missingRec := missing.NewRecordBatch()
	missing.Release()
	defer missingRec.Release()
	if _, _, err := SplitByOp(ctx, &dataplane.Batch{Record: missingRec}); err == nil {
		t.Fatal("missing __op must fail")
	}

	wrong := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "__op", Type: arrow.BinaryTypes.String},
	}, nil))
	wrong.Field(0).(*array.StringBuilder).Append("insert")
	wrongRec := wrong.NewRecordBatch()
	wrong.Release()
	defer wrongRec.Release()
	if _, _, err := SplitByOp(ctx, &dataplane.Batch{Record: wrongRec}); err == nil {
		t.Fatal("non-uint8 __op must fail")
	}
}

func TestStringColumnErrors(t *testing.T) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	if _, err := StringColumn(rec, "nope"); err == nil {
		t.Fatal("missing column must fail")
	}
	if _, err := StringColumn(rec, "id"); err == nil {
		t.Fatal("wrong-typed column must fail")
	}
}

func TestConstStringAndNullColumn(t *testing.T) {
	s := ConstString(3, "x")
	defer s.Release()
	str := s.(*array.String)
	for i := range 3 {
		if str.Value(i) != "x" {
			t.Fatalf("constString[%d] = %q, want x", i, str.Value(i))
		}
	}

	n := NullColumn(arrow.FixedWidthTypes.Timestamp_us, 2)
	defer n.Release()
	if n.Len() != 2 || !n.IsNull(0) || !n.IsNull(1) {
		t.Fatalf("nullColumn = %v", n)
	}
	if n.DataType().ID() != arrow.TIMESTAMP {
		t.Fatalf("nullColumn type = %s, want timestamp", n.DataType())
	}
}
