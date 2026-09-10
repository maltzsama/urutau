package transport

// RV-03: the decode side is a closed world. arrowTypeToCore only maps
// types the encoder produces; everything else must ERROR. The old
// "defensive widenings" (uint32 -> int64, float16 -> float32, ...) let a
// mismatched array reach a hard type assertion in readTypedValue — a
// remote-triggerable panic on the wire boundary.

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func wireRecordWithColumnType(t *testing.T, alloc memory.Allocator, name string, dt arrow.DataType) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema(append([]arrow.Field{
		{Name: name, Type: dt, Nullable: true},
	}, WireMetadataFields()...), nil)
	bld := array.NewRecordBuilder(alloc, schema)
	defer bld.Release()
	bld.Field(0).AppendNull()
	bld.Field(1).(*array.Uint8Builder).Append(0)
	bld.Field(2).(*array.StringBuilder).Append("p1")
	bld.Field(3).(*array.TimestampBuilder).Append(0)
	bld.Field(4).(*array.TimestampBuilder).Append(0)
	bld.Field(5).(*array.BooleanBuilder).Append(false)
	bld.Field(6).(*array.StringBuilder).Append("stream")
	return bld.NewRecordBatch()
}

func TestDecodeRejectsNonProducedArrowTypes(t *testing.T) {
	alloc := memory.NewGoAllocator()
	cases := []struct {
		name string
		dt   arrow.DataType
	}{
		{"uint32", arrow.PrimitiveTypes.Uint32},
		{"uint16", arrow.PrimitiveTypes.Uint16},
		{"uint8", arrow.PrimitiveTypes.Uint8},
		{"int8", arrow.PrimitiveTypes.Int8},
		{"int16", arrow.PrimitiveTypes.Int16},
		{"float16", arrow.FixedWidthTypes.Float16},
		{"large_string", arrow.BinaryTypes.LargeString},
		{"large_binary", arrow.BinaryTypes.LargeBinary},
		{"date64", arrow.FixedWidthTypes.Date64},
		{"time32", arrow.FixedWidthTypes.Time32s},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := wireRecordWithColumnType(t, alloc, "c_"+tc.name, tc.dt)
			defer rec.Release()

			_, err := DecodeBatch(rec, "t", nil)
			if err == nil {
				t.Fatalf("DecodeBatch must reject %s", tc.name)
			}
			if !strings.Contains(err.Error(), "not produced by the encoder") &&
				!strings.Contains(err.Error(), "has no canonical mapping") {
				t.Fatalf("rejection must cite the closed-world mapping, got: %v", err)
			}
		})
	}
}
