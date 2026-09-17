package iceberg

// Regression tests for two defects fixed in project_record.go:
//
//	2. declaring a metadata column panicked projectRecord on an Arrow type mismatch
//	3. scalarValue returned nil (a NULL equality-delete key) for decimal/date/time/float32 PKs

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// msg_ts (timestamp) and enrich_miss (bool) used to fall through buildMetaColumn
// to a utf8 null, which panics array.NewRecordBatch. They must now project a
// column whose type equals the field's.
func TestMetadataColumnsMatchDeclaredFieldType(t *testing.T) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "msg_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{Name: "enrich_miss", Type: arrow.FixedWidthTypes.Boolean},
	}
	w := &TableWriter{
		dataSchema: arrow.NewSchema(fields, nil),
		metaByName: map[string]core.MetadataColumn{
			"msg_ts":      {From: core.MetaMsgTS, As: "msg_ts"},
			"enrich_miss": {From: core.MetaEnrichMiss, As: "enrich_miss"},
		},
		cast: core.CastPolicy{},
	}
	b := wireBatch(t, [3]any{int64(1), "x", rowchange.OpUpdate})
	defer b.Release()

	out, err := w.projectRecord(context.Background(), b)
	if err != nil {
		t.Fatalf("projectRecord: %v", err)
	}
	defer out.Release()
	for i, f := range fields {
		if got := out.Column(i).DataType(); !arrow.TypeEqual(got, f.Type) {
			t.Errorf("column %q type = %s, want %s", f.Name, got, f.Type)
		}
	}
}

// timestampColumn must build in the target field's unit: __commit_ts rides the
// wire as ns while an Iceberg Timestamptz field is timestamp[us, UTC]. The
// instant is preserved across the conversion.
func TestCommitTSUsesFieldUnitAndPreservesInstant(t *testing.T) {
	const ns = int64(1_500_000_123_456) // 1500000.123456 s -> 1500000123456 us
	wireNS := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	tsB := array.NewTimestampBuilder(memory.DefaultAllocator, wireNS)
	tsB.Append(arrow.Timestamp(ns))
	tsArr := tsB.NewTimestampArray()
	tsB.Release()
	defer tsArr.Release()

	wire := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "__commit_ts", Type: wireNS}}, nil), []arrow.Array{tsArr}, 1)
	defer wire.Release()

	arr, err := timestampColumn(wire, "__commit_ts", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	if err != nil {
		t.Fatalf("timestampColumn: %v", err)
	}
	defer arr.Release()
	col := arr.(*array.Timestamp)
	if got := col.DataType().(*arrow.TimestampType).Unit; got != arrow.Microsecond {
		t.Fatalf("commit_ts unit = %v, want Microsecond", got)
	}
	if got, want := int64(col.Value(0)), ns/1000; got != want {
		t.Fatalf("commit_ts = %d us, want %d (instant not preserved)", got, want)
	}
}

// scalarValue used to return nil for decimal/date/time/float32, which became a
// NULL equality-delete key that matched no row. Each must now return a value
// appendColumn accepts, and an unhandled type must be an error — never nil.
func TestScalarValueSupportsUnhandledPKTypes(t *testing.T) {
	decB := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 20, Scale: 0})
	if err := decB.AppendValueFromString("42"); err != nil {
		t.Fatalf("decimal append: %v", err)
	}
	dec := decB.NewDecimal128Array()
	decB.Release()
	defer dec.Release()

	dateB := array.NewDate32Builder(memory.DefaultAllocator)
	dateB.Append(arrow.Date32(19000))
	date := dateB.NewDate32Array()
	dateB.Release()
	defer date.Release()

	timeB := array.NewTime64Builder(memory.DefaultAllocator, &arrow.Time64Type{Unit: arrow.Microsecond})
	timeB.Append(arrow.Time64(3_661_500_000)) // 01:01:01.500000
	tm := timeB.NewTime64Array()
	timeB.Release()
	defer tm.Release()

	f32B := array.NewFloat32Builder(memory.DefaultAllocator)
	f32B.Append(1.5)
	f32 := f32B.NewFloat32Array()
	f32B.Release()
	defer f32.Release()

	if v, err := scalarValue(dec, 0); err != nil || v != "42" {
		t.Errorf("scalarValue(decimal) = %v, %v; want \"42\", nil", v, err)
	}
	v, err := scalarValue(date, 0)
	if err != nil {
		t.Errorf("scalarValue(date): %v", err)
	} else if days, derr := dateToDays(v.(string)); derr != nil || days != 19000 {
		t.Errorf("date key %q does not round-trip: days=%d err=%v", v, days, derr)
	}
	v, err = scalarValue(tm, 0)
	if err != nil {
		t.Errorf("scalarValue(time): %v", err)
	} else if micros, terr := timeToMicros(v.(string)); terr != nil || micros != 3_661_500_000 {
		t.Errorf("time key %q does not round-trip: micros=%d err=%v", v, micros, terr)
	}
	if v, err := scalarValue(f32, 0); err != nil || v != float32(1.5) {
		t.Errorf("scalarValue(float32) = %v, %v; want 1.5", v, err)
	}

	i8B := array.NewInt8Builder(memory.DefaultAllocator)
	i8B.Append(7)
	i8 := i8B.NewInt8Array()
	i8B.Release()
	defer i8.Release()
	if v, err := scalarValue(i8, 0); err == nil {
		t.Errorf("scalarValue(int8) = %v, nil; want an error", v)
	}
}

// A Time64 key must be normalized to microseconds regardless of the array's
// unit: formatTimeOfDay is microsecond-based, so a nanosecond value (not on
// today's wire, which is Time64us) would otherwise shift the key.
func TestScalarValueNormalizesTime64Unit(t *testing.T) {
	nsB := array.NewTime64Builder(memory.DefaultAllocator, &arrow.Time64Type{Unit: arrow.Nanosecond})
	nsB.Append(arrow.Time64(3_661_500_000_000)) // 3661.5 s expressed in ns
	ns := nsB.NewTime64Array()
	nsB.Release()
	defer ns.Release()

	v, err := scalarValue(ns, 0)
	if err != nil {
		t.Fatalf("scalarValue(ns Time64): %v", err)
	}
	if micros, terr := timeToMicros(v.(string)); terr != nil || micros != 3_661_500_000 {
		t.Fatalf("ns Time64 key %q: micros=%d err=%v, want 3661500000", v, micros, terr)
	}
}

// extractKeys for a decimal PK must yield the canonical decimal text, not nil.
func TestExtractKeysDecimalPKIsNotNull(t *testing.T) {
	decB := array.NewDecimal128Builder(memory.DefaultAllocator, &arrow.Decimal128Type{Precision: 20, Scale: 0})
	if err := decB.AppendValueFromString("42"); err != nil {
		t.Fatalf("decimal append: %v", err)
	}
	dec := decB.NewDecimal128Array()
	decB.Release()
	defer dec.Release()

	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: &arrow.Decimal128Type{Precision: 20, Scale: 0}},
	}, nil), []arrow.Array{dec}, 1)
	defer rec.Release()

	keys, err := extractKeys([]*dataplane.Batch{{Table: "t", Record: rec}}, []string{"id"})
	if err != nil {
		t.Fatalf("extractKeys: %v", err)
	}
	if len(keys) != 1 || keys[0][0] != "42" {
		t.Fatalf("keys = %v, want [[\"42\"]]", keys)
	}
}
