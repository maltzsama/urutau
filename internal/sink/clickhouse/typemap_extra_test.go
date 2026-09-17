package clickhouse

import (
	"reflect"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/shopspring/decimal"
)

func TestZeroOfCoversAllBases(t *testing.T) {
	kinds := []string{"Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64", "Float32", "Float64", "String", "Date", "DateTime", "UUID"}
	for _, k := range kinds {
		z := zeroOf(k)
		if z == nil {
			t.Errorf("zeroOf(%q) = nil", k)
		}
	}
}

func TestZeroIntCoversAllWidths(t *testing.T) {
	widths := []string{"Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
	for _, w := range widths {
		z := zeroInt(w)
		if z == nil {
			t.Errorf("zeroInt(%q) = nil", w)
		}
	}
}

func TestUintValueCoversAllTypes(t *testing.T) {
	types := []string{"UInt8", "UInt16", "UInt32", "UInt64"}
	for _, typ := range types {
		got, err := uintValue(typ, int(42))
		if err != nil {
			t.Errorf("uintValue(%q): %v", typ, err)
		}
		if got != 42 {
			t.Errorf("uintValue(%q) = %d, want 42", typ, got)
		}
	}
}

func TestIntValueCoversAllTypes(t *testing.T) {
	types := []string{"Int8", "Int16", "Int32", "Int64"}
	for _, typ := range types {
		got, err := intValue(typ, int(42))
		if err != nil {
			t.Errorf("intValue(%q): %v", typ, err)
		}
		if got != 42 {
			t.Errorf("intValue(%q) = %d, want 42", typ, got)
		}
	}
}

func TestToIntOverflowChecks(t *testing.T) {
	// UInt8 overflow.
	if _, err := toInt("UInt8", int(256)); err == nil {
		t.Error("UInt8 overflow: want error")
	}
	// Int8 overflow.
	if _, err := toInt("Int8", int(128)); err == nil {
		t.Error("Int8 overflow: want error")
	}
	// Valid.
	got, err := toInt("Int32", int(42))
	if err != nil {
		t.Errorf("valid toInt: %v", err)
	}
	if got == nil {
		t.Error("valid toInt = nil")
	}
}

func TestCoerceEdgeCases(t *testing.T) {
	// Empty chType falls through to default error.
	if _, err := coerce("", 42); err == nil {
		t.Error("coerce(empty): want error")
	}

	// Bool passthrough.
	got, err := coerce("Bool", true)
	if err != nil || got != true {
		t.Errorf("coerce(Bool) = %v, %v", got, err)
	}

	// String passthrough.
	got, err = coerce("String", "hello")
	if err != nil || got != "hello" {
		t.Errorf("coerce(String) = %v, %v", got, err)
	}

	// Decimal from decimal.Decimal directly.
	d := decimal.RequireFromString("99.99")
	got, err = coerce("Decimal(10,2)", d)
	if err != nil {
		t.Errorf("coerce(Decimal, decimal) = %v", err)
	}

	// Array/Tuple passthrough.
	arr := []any{"a", "b"}
	got, err = coerce("Array(String)", arr)
	if err != nil || !reflect.DeepEqual(got, arr) {
		t.Errorf("coerce(Array, []any) = %v, %v", got, err)
	}
	got, err = coerce("Tuple(String)", arr)
	if err != nil || !reflect.DeepEqual(got, arr) {
		t.Errorf("coerce(Tuple, []any) = %v, %v", got, err)
	}

	// Map passthrough.
	m := map[string]any{"k": "v"}
	got, err = coerce("Map(String, String)", m)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Errorf("coerce(Map, map[string]any) = %v, %v", got, err)
	}
	m2 := map[any]any{"k": "v"}
	got, err = coerce("Map(String, String)", m2)
	if err != nil || !reflect.DeepEqual(got, m2) {
		t.Errorf("coerce(Map, map[any]any) = %v, %v", got, err)
	}
}

func TestFloatValueEdgeCases(t *testing.T) {
	got, err := floatValue(float32(1.5))
	if err != nil || got != 1.5 {
		t.Errorf("floatValue(float32) = %v, %v", got, err)
	}
	got, err = floatValue(int64(42))
	if err != nil || got != 42.0 {
		t.Errorf("floatValue(int64) = %v, %v", got, err)
	}
	if _, err := floatValue("bad"); err == nil {
		t.Error("floatValue(string): want error")
	}
}

func TestToTimeEdgeCases(t *testing.T) {
	// Valid datetime string.
	got, err := toTime("2024-01-02T15:04:05Z", "DateTime")
	if err != nil {
		t.Errorf("toTime(valid): %v", err)
	}
	if got == nil {
		t.Error("toTime(valid) = nil")
	}
	// Unsupported type.
	if _, err := toTime(42, ""); err == nil {
		t.Error("toTime(int): want error")
	}
	// Invalid string.
	if _, err := toTime("not-a-time", ""); err == nil {
		t.Error("toTime(invalid): want error")
	}
}

func TestMetaValueClickHouse(t *testing.T) {
	now := time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC)
	c := chRowMeta{
		Op:       rowchange.OpInsert,
		Position: "gtid:1",
		CommitTS: now,
		IngestTS: now,
		Snapshot: false,
		Phase:    "stream",
	}

	cases := []struct {
		key core.MetadataKey
	}{
		{core.MetaOp},
		{core.MetaCommitTS},
		{core.MetaIngestTS},
		{core.MetaPosition},
		{core.MetaSourceTable},
		{core.MetaPhase},
	}
	for _, tc := range cases {
		_, err := metaValue(tc.key, c, "src.t")
		if err != nil {
			t.Errorf("metaValue(%q): %v", tc.key, err)
		}
	}

	// Unknown key errors.
	if _, err := metaValue("unknown", c, "t"); err == nil {
		t.Error("unknown metadata key: want error")
	}

	// Zero commit TS returns nil.
	zero := chRowMeta{CommitTS: time.Time{}}
	got, err := metaValue(core.MetaCommitTS, zero, "t")
	if err != nil {
		t.Fatalf("zero commitTS: %v", err)
	}
	if got != nil {
		t.Errorf("zero commitTS = %v, want nil", got)
	}
}
