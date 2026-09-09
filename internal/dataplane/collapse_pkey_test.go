package dataplane_test

// T-9 (real): Collapse with composite multi-type PKs — including UUID
// (FixedSizeBinary), Decimal128 and Binary — plus EncodeKey anti-collision
// guarantees between types.

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/internal/dataplane"
)

// wireBatch builds a wire-schema record from data fields + rows, with the
// 5 trailing metadata columns filled from row ops/positions.
type wireRow struct {
	op   uint8
	pos  string
	data map[string]any
}

func buildWireBatch(t *testing.T, alloc *memory.CheckedAllocator, dataFields []arrow.Field, rows []wireRow) *dataplane.Batch {
	t.Helper()

	fields := make([]arrow.Field, 0, len(dataFields)+5)
	fields = append(fields, dataFields...)
	fields = append(fields,
		arrow.Field{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		arrow.Field{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		arrow.Field{Name: "__ingest_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		arrow.Field{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	)
	schema := arrow.NewSchema(fields, nil)
	bld := array.NewRecordBuilder(alloc, schema)
	defer bld.Release()

	for _, r := range rows {
		for j := range fields {
			f := &fields[j]
			switch f.Name {
			case "__op":
				bld.Field(j).(*array.Uint8Builder).Append(r.op)
			case "__pos":
				bld.Field(j).(*array.StringBuilder).Append(r.pos)
			case "__commit_ts":
				bld.Field(j).(*array.TimestampBuilder).Append(0)
			case "__ingest_ts":
				bld.Field(j).(*array.TimestampBuilder).Append(0)
			case "__snapshot":
				bld.Field(j).(*array.BooleanBuilder).Append(false)
			default:
				switch v := r.data[f.Name].(type) {
				case int32:
					bld.Field(j).(*array.Int32Builder).Append(v)
				case int64:
					bld.Field(j).(*array.Int64Builder).Append(v)
				case float32:
					bld.Field(j).(*array.Float32Builder).Append(v)
				case float64:
					bld.Field(j).(*array.Float64Builder).Append(v)
				case string:
					bld.Field(j).(*array.StringBuilder).Append(v)
				case []byte:
					if fb, ok := bld.Field(j).(*array.FixedSizeBinaryBuilder); ok {
						fb.Append(v)
					} else {
						bld.Field(j).(*array.BinaryBuilder).Append(v)
					}
				case decimal128.Num:
					bld.Field(j).(*array.Decimal128Builder).Append(v)
				default:
					t.Fatalf("unsupported value %T for column %q", r.data[f.Name], f.Name)
				}
			}
		}
	}
	rec := bld.NewRecordBatch()
	return &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w")}
}

func TestCollapseCompositeMultiTypePK(t *testing.T) {
	alloc := checkedAlloc(t)

	fields := []arrow.Field{
		{Name: "pk1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "pk2", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	b := buildWireBatch(t, alloc, fields, []wireRow{
		{op: uint8(dataplane.OpInsert), pos: "p1", data: map[string]any{"pk1": int32(1), "pk2": "a", "v": "v1"}},
		{op: uint8(dataplane.OpUpdate), pos: "p2", data: map[string]any{"pk1": int32(1), "pk2": "a", "v": "v2"}},
		{op: uint8(dataplane.OpInsert), pos: "p3", data: map[string]any{"pk1": int32(2), "pk2": "b", "v": "v3"}},
	})
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), alloc, b, []string{"pk1", "pk2"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()
	if dels != nil {
		t.Fatalf("no deletes expected, got %d", dels.Record.NumRows())
	}
	if ups == nil || ups.Record.NumRows() != 2 {
		t.Fatalf("upserts = %v, want 2 rows", ups)
	}
	// Last occurrence wins: pk (1,"a") must carry v2.
	vArr, ok := ups.Record.Column(2).(*array.String)
	if !ok {
		t.Fatalf("v column type %T", ups.Record.Column(2))
	}
	if vArr.Value(0) != "v2" || vArr.Value(1) != "v3" {
		t.Errorf("collapse winner wrong: got %q,%q want v2,v3", vArr.Value(0), vArr.Value(1))
	}
}

func TestCollapseUUIDDecimalBinaryPK(t *testing.T) {
	alloc := checkedAlloc(t)

	uuidA := bytes.Repeat([]byte{0xAB}, 16)
	uuidB := bytes.Repeat([]byte{0xCD}, 16)
	dec1, _ := decimal128.FromString("10.50", 10, 2)
	dec2, _ := decimal128.FromString("99.99", 10, 2)

	fields := []arrow.Field{
		{Name: "pk_uuid", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: false},
		{Name: "pk_dec", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: false},
		{Name: "pk_bin", Type: arrow.BinaryTypes.Binary, Nullable: false},
		{Name: "v", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	b := buildWireBatch(t, alloc, fields, []wireRow{
		{op: uint8(dataplane.OpInsert), pos: "p1", data: map[string]any{"pk_uuid": uuidA, "pk_dec": dec1, "pk_bin": []byte{1, 2}, "v": "first"}},
		{op: uint8(dataplane.OpUpdate), pos: "p2", data: map[string]any{"pk_uuid": uuidA, "pk_dec": dec1, "pk_bin": []byte{1, 2}, "v": "second"}},
		{op: uint8(dataplane.OpInsert), pos: "p3", data: map[string]any{"pk_uuid": uuidB, "pk_dec": dec2, "pk_bin": []byte{3}, "v": "other"}},
	})
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), alloc, b, []string{"pk_uuid", "pk_dec", "pk_bin"})
	if err != nil {
		t.Fatalf("Collapse (uuid/decimal/binary keys): %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()
	if dels != nil {
		t.Fatalf("no deletes expected, got %d", dels.Record.NumRows())
	}
	if ups == nil || ups.Record.NumRows() != 2 {
		t.Fatalf("upserts = %v, want 2 rows (uuid/dec/bin rows 0+1 collapse, row 2 distinct)", ups)
	}
	vArr := ups.Record.Column(3).(*array.String)
	if vArr.Value(0) != "second" {
		t.Errorf("winner = %q, want %q", vArr.Value(0), "second")
	}
}

func TestEncodeKeyTypeAntiCollision(t *testing.T) {
	alloc := checkedAlloc(t)

	// Same numeric value in different types must produce DIFFERENT keys
	// (type-tagged encoding); equal-type equal-value must produce the SAME
	// key; different bytes / values must differ.
	fields := []arrow.Field{
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "i64", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "f32", Type: arrow.PrimitiveTypes.Float32, Nullable: false},
		{Name: "f64", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "uuid", Type: &arrow.FixedSizeBinaryType{ByteWidth: 16}, Nullable: false},
		{Name: "bin", Type: arrow.BinaryTypes.Binary, Nullable: false},
	}
	b := buildWireBatch(t, alloc, fields, []wireRow{
		{op: 0, pos: "p1", data: map[string]any{
			"i32": int32(5), "i64": int64(5), "f32": float32(0.1), "f64": float64(0.1),
			"uuid": bytes.Repeat([]byte{0x01}, 16), "bin": bytes.Repeat([]byte{0x01}, 16),
		}},
		{op: 0, pos: "p2", data: map[string]any{
			"i32": int32(6), "i64": int64(5), "f32": float32(0.1), "f64": float64(0.1),
			"uuid": bytes.Repeat([]byte{0x01}, 16), "bin": bytes.Repeat([]byte{0x01}, 16),
		}},
	})
	defer b.Release()
	rec := b.Record

	keyOf := func(row int, cols []string) []byte {
		t.Helper()
		k, err := dataplane.EncodeKey(rec, row, cols)
		if err != nil {
			t.Fatalf("EncodeKey: %v", err)
		}
		return k
	}

	// T-9 revision: widening stability is GONE by design — int32(5) and
	// int64(5) carry different type tags → different keys.
	if bytes.Equal(keyOf(0, []string{"i32"}), keyOf(0, []string{"i64"})) {
		t.Error("int32(5) and int64(5) must NOT share a key")
	}
	if !bytes.Equal(keyOf(0, []string{"i64"}), keyOf(1, []string{"i64"})) {
		t.Error("int64(5) across rows must share a key")
	}
	// float32(0.1) vs float64(0.1): different bit patterns → different keys.
	if bytes.Equal(keyOf(0, []string{"f32"}), keyOf(0, []string{"f64"})) {
		t.Error("float32(0.1) and float64(0.1) must NOT share a key")
	}
	// UUID (FixedSizeBinary) vs Binary with IDENTICAL bytes: the type tag
	// must keep them apart.
	if bytes.Equal(keyOf(0, []string{"uuid"}), keyOf(0, []string{"bin"})) {
		t.Error("uuid(16×0x01) and binary(16×0x01) must NOT share a key")
	}
}
