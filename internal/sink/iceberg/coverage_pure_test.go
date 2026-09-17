package iceberg

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/spec"
)

// ── appendColumn: full builder matrix ────────────────────────────────

func decimalType() *arrow.Decimal128Type    { return &arrow.Decimal128Type{Precision: 12, Scale: 3} }
func fixedType() *arrow.FixedSizeBinaryType { return &arrow.FixedSizeBinaryType{ByteWidth: 16} }

// TestAppendColumnAcceptsCanonicalValues drives every builder branch
// appendColumn supports with a value of the shape the wire actually carries,
// including the trailing NULL that marks a nullable column.
func TestAppendColumnAcceptsCanonicalValues(t *testing.T) {
	cases := []struct {
		name string
		dt   arrow.DataType
		vals []any
	}{
		{"int64", arrow.PrimitiveTypes.Int64, []any{int64(1), int(2), int32(3), float64(4), nil}},
		{"int32", arrow.PrimitiveTypes.Int32, []any{int32(1), int(2), int64(3), float64(4), nil}},
		{"float32", arrow.PrimitiveTypes.Float32, []any{float32(1.5), float64(2.5), int64(3), nil}},
		{"string", arrow.BinaryTypes.String, []any{"a", nil}},
		{"float64", arrow.PrimitiveTypes.Float64, []any{float64(1.5), float32(2.5), int64(3), int(4), int32(5), nil}},
		{"bool", arrow.FixedWidthTypes.Boolean, []any{true, false, nil}},
		{"decimal", decimalType(), []any{"12.340", nil}},
		{"date", arrow.FixedWidthTypes.Date32, []any{"2022-01-08", nil}},
		{"time", arrow.FixedWidthTypes.Time64us, []any{"01:01:01.500000", nil}},
		{"timestamp-time", arrow.FixedWidthTypes.Timestamp_us, []any{time.Unix(0, 0).UTC(), nil}},
		{"timestamp-text", arrow.FixedWidthTypes.Timestamp_us, []any{"2022-01-08 01:02:03", nil}},
		{"fixed-bytes", fixedType(), []any{[]byte("0123456789abcdef"), nil}},
		{"fixed-uuid", fixedType(), []any{"550e8400-e29b-41d4-a716-446655440000", nil}},
		{"binary", arrow.BinaryTypes.Binary, []any{[]byte{0xde, 0xad}, nil}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := array.NewBuilder(memory.DefaultAllocator, tc.dt)
			defer b.Release()
			if err := appendColumn(b, arrow.Field{Name: "c", Type: tc.dt}, tc.vals); err != nil {
				t.Fatalf("appendColumn: %v", err)
			}
			arr := b.NewArray()
			defer arr.Release()
			if int(arr.Len()) != len(tc.vals) {
				t.Fatalf("len = %d, want %d", arr.Len(), len(tc.vals))
			}
			if !arr.IsNull(len(tc.vals) - 1) {
				t.Fatalf("trailing value must be NULL")
			}
		})
	}
}

// TestAppendColumnRejectsIncompatibleValues proves a value the column type
// cannot hold fails loudly instead of being coerced or silently dropped.
func TestAppendColumnRejectsIncompatibleValues(t *testing.T) {
	cases := []struct {
		name string
		dt   arrow.DataType
		v    any
	}{
		{"int64-string", arrow.PrimitiveTypes.Int64, "x"},
		{"int64-fractional", arrow.PrimitiveTypes.Int64, float64(1.5)},
		{"int32-string", arrow.PrimitiveTypes.Int32, "x"},
		{"int32-overflow-int64", arrow.PrimitiveTypes.Int32, int64(math.MaxInt32) + 1},
		{"int32-overflow-int", arrow.PrimitiveTypes.Int32, int(math.MaxInt32) + 1},
		{"int32-fractional", arrow.PrimitiveTypes.Int32, float64(1.5)},
		{"float32-string", arrow.PrimitiveTypes.Float32, "x"},
		{"string-int", arrow.BinaryTypes.String, 1},
		{"float64-string", arrow.PrimitiveTypes.Float64, "x"},
		{"bool-int", arrow.FixedWidthTypes.Boolean, 1},
		{"decimal-int", decimalType(), 1},
		{"decimal-bad-string", decimalType(), "not-a-decimal"},
		{"date-int", arrow.FixedWidthTypes.Date32, 1},
		{"date-bad", arrow.FixedWidthTypes.Date32, "not-a-date"},
		{"time-int", arrow.FixedWidthTypes.Time64us, 1},
		{"time-bad", arrow.FixedWidthTypes.Time64us, "nope"},
		{"timestamp-int", arrow.FixedWidthTypes.Timestamp_us, 1},
		{"timestamp-bad", arrow.FixedWidthTypes.Timestamp_us, "nope"},
		{"fixed-int", fixedType(), 1},
		{"fixed-bad-uuid", fixedType(), "not-a-uuid"},
		{"binary-string", arrow.BinaryTypes.Binary, "x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := array.NewBuilder(memory.DefaultAllocator, tc.dt)
			defer b.Release()
			if err := appendColumn(b, arrow.Field{Name: "c", Type: tc.dt}, []any{tc.v}); err == nil {
				t.Fatalf("appendColumn(%v) must be rejected", tc.v)
			}
		})
	}
}

// TestAppendColumnUnsupportedBuilder guards the default arm: a column type the
// writer has no rule for must fail rather than write garbage.
func TestAppendColumnUnsupportedBuilder(t *testing.T) {
	b := array.NewBuilder(memory.DefaultAllocator, arrow.FixedWidthTypes.Duration_s)
	defer b.Release()
	err := appendColumn(b, arrow.Field{Name: "d", Type: arrow.FixedWidthTypes.Duration_s}, []any{int64(1)})
	if err == nil || !strings.Contains(err.Error(), "unsupported builder") {
		t.Fatalf("err = %v, want unsupported builder", err)
	}
}

// ── appendOneComposite: struct / list / map ──────────────────────────

func structTypeOf() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
	)
}
func listTypeOf() *arrow.ListType { return arrow.ListOf(arrow.PrimitiveTypes.Int64) }
func mapTypeOf() *arrow.MapType {
	return arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)
}

func TestAppendOneCompositeStruct(t *testing.T) {
	st := structTypeOf()
	b := array.NewStructBuilder(memory.DefaultAllocator, st)
	defer b.Release()
	field := arrow.Field{Name: "s", Type: st}

	if err := appendOneComposite(b, field, map[string]any{"a": int64(1), "b": "x"}); err != nil {
		t.Fatalf("struct row: %v", err)
	}
	if err := appendOneComposite(b, field, map[string]any{"a": int64(2)}); err != nil {
		t.Fatalf("struct row with missing key: %v", err)
	}
	if err := appendOneComposite(b, field, nil); err != nil {
		t.Fatalf("nil struct row: %v", err)
	}
	arr := b.NewArray()
	defer arr.Release()
	if arr.Len() != 3 {
		t.Fatalf("len = %d, want 3", arr.Len())
	}
	if !arr.IsNull(2) {
		t.Fatal("nil struct row must be null")
	}
}

func TestAppendOneCompositeList(t *testing.T) {
	lt := listTypeOf()
	b := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer b.Release()
	field := arrow.Field{Name: "l", Type: lt}

	if err := appendOneComposite(b, field, []any{int64(1), int64(2)}); err != nil {
		t.Fatalf("list row: %v", err)
	}
	if err := appendOneComposite(b, field, []any{}); err != nil {
		t.Fatalf("empty list: %v", err)
	}
	if err := appendOneComposite(b, field, nil); err != nil {
		t.Fatalf("nil list: %v", err)
	}
	arr := b.NewArray()
	defer arr.Release()
	if arr.Len() != 3 || !arr.IsNull(2) {
		t.Fatalf("list array = %v", arr)
	}
}

func TestAppendOneCompositeMap(t *testing.T) {
	mt := mapTypeOf()
	b := array.NewMapBuilder(memory.DefaultAllocator, mt.KeyField().Type, mt.ItemField().Type, false)
	defer b.Release()
	field := arrow.Field{Name: "m", Type: mt}

	// Keys deliberately out of order: sortedKeys must make the write stable.
	if err := appendOneComposite(b, field, map[string]any{"z": int64(1), "a": int64(2)}); err != nil {
		t.Fatalf("map row: %v", err)
	}
	if err := appendOneComposite(b, field, nil); err != nil {
		t.Fatalf("nil map: %v", err)
	}
	arr := b.NewArray()
	defer arr.Release()
	if arr.Len() != 2 || !arr.IsNull(1) {
		t.Fatalf("map array = %v", arr)
	}
}

func TestAppendOneCompositeRejectsMismatches(t *testing.T) {
	int64Builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer int64Builder.Release()
	if err := appendOneComposite(int64Builder, arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64}, int64(1)); err == nil {
		t.Fatal("non-composite builder must be rejected")
	}

	st := structTypeOf()
	sb := array.NewStructBuilder(memory.DefaultAllocator, st)
	defer sb.Release()
	if err := appendOneComposite(sb, arrow.Field{Name: "s", Type: st}, "not-a-map"); err == nil {
		t.Fatal("non-map value into struct builder must be rejected")
	}
	if err := appendOneComposite(sb, arrow.Field{Name: "s", Type: listTypeOf()}, map[string]any{}); err == nil {
		t.Fatal("struct builder with non-struct field type must be rejected")
	}

	lt := listTypeOf()
	lb := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer lb.Release()
	if err := appendOneComposite(lb, arrow.Field{Name: "l", Type: lt}, "not-a-list"); err == nil {
		t.Fatal("non-list value into list builder must be rejected")
	}
	if err := appendOneComposite(lb, arrow.Field{Name: "l", Type: st}, []any{}); err == nil {
		t.Fatal("list builder with non-list field type must be rejected")
	}

	mt := mapTypeOf()
	mb := array.NewMapBuilder(memory.DefaultAllocator, mt.KeyField().Type, mt.ItemField().Type, false)
	defer mb.Release()
	if err := appendOneComposite(mb, arrow.Field{Name: "m", Type: mt}, "not-a-map"); err == nil {
		t.Fatal("non-map value into map builder must be rejected")
	}
	if err := appendOneComposite(mb, arrow.Field{Name: "m", Type: lt}, map[string]any{}); err == nil {
		t.Fatal("map builder with non-map field type must be rejected")
	}
}

func TestSortedKeys(t *testing.T) {
	got := sortedKeys(map[string]any{"c": 1, "a": 2, "b": 3})
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if len(sortedKeys(nil)) != 0 {
		t.Fatal("nil map must yield no keys")
	}
}

// ── typemap ──────────────────────────────────────────────────────────

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

// ── buildPartitionSpec ───────────────────────────────────────────────

func TestBuildPartitionSpecTransforms(t *testing.T) {
	sch := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "d", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 4, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
	cases := []struct {
		name    string
		exprs   []string
		wantErr bool
		fields  int
	}{
		{"empty-is-unpartitioned", nil, false, 0},
		{"day", []string{"day(d)"}, false, 1},
		{"month", []string{"month(d)"}, false, 1},
		{"year", []string{"year(d)"}, false, 1},
		{"hour", []string{"hour(ts)"}, false, 1},
		{"identity", []string{"identity(name)"}, false, 1},
		{"bucket", []string{"bucket(4, id)"}, false, 1},
		{"truncate", []string{"truncate(2, name)"}, false, 1},
		{"multiple", []string{"day(d)", "bucket(4, id)"}, false, 2},
		{"whitespace-tolerated", []string{"  day(d)  "}, false, 1},
		{"garbage", []string{"nope"}, true, 0},
		{"bucket-zero", []string{"bucket(0, id)"}, true, 0},
		{"truncate-negative", []string{"truncate(-1, name)"}, true, 0},
		{"unknown-column", []string{"day(nope)"}, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec, err := buildPartitionSpec(sch, tc.exprs)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("buildPartitionSpec(%v) must fail", tc.exprs)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildPartitionSpec: %v", err)
			}
			if spec.Len() != tc.fields {
				t.Fatalf("fields = %d, want %d", spec.Len(), tc.fields)
			}
			if tc.fields == 0 && !spec.IsUnpartitioned() {
				t.Fatal("no exprs must yield the unpartitioned spec")
			}
		})
	}
}

// ── splitByOp ────────────────────────────────────────────────────────

func TestSplitByOp(t *testing.T) {
	ctx := context.Background()

	up, del, err := splitByOp(ctx, &dataplane.Batch{})
	if err != nil || up != nil || del != nil {
		t.Fatalf("empty batch = %v, %v, %v; want nil,nil,nil", up, del, err)
	}

	b := wireBatch(t,
		[3]any{int64(1), "a", rowchange.OpInsert},
		[3]any{int64(2), "b", rowchange.OpUpdate},
		[3]any{int64(3), "c", rowchange.OpDelete},
	)
	defer b.Release()
	up, del, err = splitByOp(ctx, b)
	if err != nil {
		t.Fatalf("splitByOp: %v", err)
	}
	defer up.Release()
	defer del.Release()
	if up.Record.NumRows() != 2 {
		t.Fatalf("upserts = %d, want 2", up.Record.NumRows())
	}
	if del.Record.NumRows() != 1 {
		t.Fatalf("deletes = %d, want 1", del.Record.NumRows())
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
	if _, _, err := splitByOp(ctx, &dataplane.Batch{Record: missingRec}); err == nil {
		t.Fatal("missing __op must fail")
	}

	wrong := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "__op", Type: arrow.BinaryTypes.String},
	}, nil))
	wrong.Field(0).(*array.StringBuilder).Append("insert")
	wrongRec := wrong.NewRecordBatch()
	wrong.Release()
	defer wrongRec.Release()
	if _, _, err := splitByOp(ctx, &dataplane.Batch{Record: wrongRec}); err == nil {
		t.Fatal("non-uint8 __op must fail")
	}
}

// ── buildMetaColumn ──────────────────────────────────────────────────

func metaSrcBatch(t *testing.T, withOp, withPos bool) arrow.RecordBatch {
	t.Helper()
	fields := []arrow.Field{
		{Name: "__commit_ts", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "__ingest_ts", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "__phase", Type: arrow.BinaryTypes.String},
	}
	if withOp {
		fields = append([]arrow.Field{{Name: "__op", Type: arrow.PrimitiveTypes.Uint8}}, fields...)
	}
	if withPos {
		fields = append(fields, arrow.Field{Name: "__pos", Type: arrow.BinaryTypes.String})
	}
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(fields, nil))
	defer b.Release()
	for i := range b.Fields() {
		switch fb := b.Field(i).(type) {
		case *array.Uint8Builder:
			fb.Append(uint8(rowchange.OpInsert))
			fb.Append(uint8(rowchange.OpDelete))
		case *array.StringBuilder:
			fb.Append("v")
			fb.Append("w")
		case *array.TimestampBuilder:
			fb.AppendTime(time.Unix(0, 0).UTC())
			fb.AppendTime(time.Unix(1, 0).UTC())
		}
	}
	return b.NewRecordBatch()
}

func TestBuildMetaColumnBranches(t *testing.T) {
	w := &TableWriter{sourceTable: "shop.orders"}
	src := metaSrcBatch(t, true, true)
	defer src.Release()
	n := src.NumRows()

	strField := arrow.Field{Name: "m", Type: arrow.BinaryTypes.String}
	tsField := arrow.Field{Name: "m", Type: arrow.FixedWidthTypes.Timestamp_us}

	cases := []struct {
		name  string
		key   core.MetadataKey
		field arrow.Field
		null  bool
	}{
		{"op", core.MetaOp, strField, false},
		{"position", core.MetaPosition, strField, false},
		{"seq", core.MetaSeq, strField, false},
		{"commit_ts", core.MetaCommitTS, tsField, false},
		{"ingest_ts", core.MetaIngestTS, tsField, false},
		{"phase", core.MetaPhase, strField, false},
		{"source_table", core.MetaSourceTable, strField, false},
		{"stream", core.MetaStream, strField, false},
		{"shard-null", core.MetaShard, strField, true},
		{"msg_ts-null", core.MetaMsgTS, tsField, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr, err := w.buildMetaColumn(src, tc.key, tc.field, n)
			if err != nil {
				t.Fatalf("buildMetaColumn: %v", err)
			}
			defer arr.Release()
			if arr.Len() != int(n) {
				t.Fatalf("len = %d, want %d", arr.Len(), n)
			}
			if tc.null && !arr.IsNull(0) {
				t.Fatal("expected a typed NULL column")
			}
		})
	}
}

func TestBuildMetaColumnErrors(t *testing.T) {
	w := &TableWriter{sourceTable: "shop.orders"}
	src := metaSrcBatch(t, false, false)
	defer src.Release()
	strField := arrow.Field{Name: "m", Type: arrow.BinaryTypes.String}

	if _, err := w.buildMetaColumn(src, core.MetaOp, strField, src.NumRows()); err == nil {
		t.Fatal("MetaOp without __op must fail")
	}
	if _, err := w.buildMetaColumn(src, core.MetaPosition, strField, src.NumRows()); err == nil {
		t.Fatal("MetaPosition without __pos must fail")
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

	if _, err := stringColumn(rec, "nope"); err == nil {
		t.Fatal("missing column must fail")
	}
	if _, err := stringColumn(rec, "id"); err == nil {
		t.Fatal("wrong-typed column must fail")
	}
}

func TestConstStringAndNullColumn(t *testing.T) {
	s := constString(3, "x")
	defer s.Release()
	str := s.(*array.String)
	for i := range 3 {
		if str.Value(i) != "x" {
			t.Fatalf("constString[%d] = %q, want x", i, str.Value(i))
		}
	}

	n := nullColumn(arrow.FixedWidthTypes.Timestamp_us, 2)
	defer n.Release()
	if n.Len() != 2 || !n.IsNull(0) || !n.IsNull(1) {
		t.Fatalf("nullColumn = %v", n)
	}
	if n.DataType().ID() != arrow.TIMESTAMP {
		t.Fatalf("nullColumn type = %s, want timestamp", n.DataType())
	}
}

// ── misc helpers ─────────────────────────────────────────────────────

func TestAddSnapshotProps(t *testing.T) {
	p := iceberg.Properties{}
	addSnapshotProps(p, "", nil)
	if _, ok := p["cdc.snapshot.state"]; ok {
		t.Fatal("empty state must not be written")
	}
	if _, ok := p["cdc.snapshot.pending"]; ok {
		t.Fatal("nil pending must not be written")
	}

	addSnapshotProps(p, "staged", []uint32{1, 2})
	if p["cdc.snapshot.state"] != "staged" {
		t.Fatalf("state = %q", p["cdc.snapshot.state"])
	}
	if p["cdc.snapshot.pending"] == "" {
		t.Fatal("pending must be encoded")
	}

	empty := iceberg.Properties{}
	addSnapshotProps(empty, "", []uint32{})
	if _, ok := empty["cdc.snapshot.pending"]; !ok {
		t.Fatal("non-nil empty pending must still be encoded")
	}
}

func TestSleepCtx(t *testing.T) {
	if err := sleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("sleepCtx: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, time.Hour); err != context.Canceled {
		t.Fatalf("cancelled sleepCtx = %v, want context.Canceled", err)
	}
}

func TestOneBatch(t *testing.T) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil))
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	seen := 0
	for got, err := range oneBatch(rec) {
		if err != nil {
			t.Fatalf("oneBatch err: %v", err)
		}
		if got.NumRows() != 1 {
			t.Fatalf("rows = %d, want 1", got.NumRows())
		}
		seen++
	}
	if seen != 1 {
		t.Fatalf("yields = %d, want 1", seen)
	}
}

func TestNewMaintainerDefaults(t *testing.T) {
	m := NewMaintainer(nil, table.Identifier{"db", "t"}, spec.Maintenance{}, nil, nil, nil)
	if m == nil {
		t.Fatal("NewMaintainer returned nil")
	}
	if m.log == nil {
		t.Fatal("nil logger must default to slog.Default()")
	}
	if len(m.ident) != 2 || m.ident[0] != "db" || m.ident[1] != "t" {
		t.Fatalf("ident = %v", m.ident)
	}
}

func TestSinkIdentResolvesNamespace(t *testing.T) {
	s := &Sink{ns: "db"}
	if got := s.ident("db.orders"); len(got) != 2 || got[0] != "db" || got[1] != "orders" {
		t.Fatalf("qualified ident = %v", got)
	}
	if got := s.ident("orders"); len(got) != 2 || got[0] != "db" || got[1] != "orders" {
		t.Fatalf("bare ident = %v", got)
	}
}

func TestSinkMaintainCloseAndConcurrency(t *testing.T) {
	s := &Sink{ns: "db"}
	if m := s.Maintain(core.TableRef{Target: "orders"}, spec.Maintenance{}, nil, nil, nil); m == nil {
		t.Fatal("Maintain returned nil")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !s.SupportsConcurrentWriters() {
		t.Fatal("iceberg supports concurrent writers")
	}
}
