package plugin

// Coverage for the plugin adapter's Arrow→core conversion helpers, which
// translate a plugin's Flight schema and records into the canonical shapes.

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
)

func TestArrowTypeToCoreMatrix(t *testing.T) {
	cases := []struct {
		name string
		in   arrow.DataType
		want core.Kind
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, core.KindBool},
		{"int8", arrow.PrimitiveTypes.Int8, core.KindInt32},
		{"int16", arrow.PrimitiveTypes.Int16, core.KindInt32},
		{"int32", arrow.PrimitiveTypes.Int32, core.KindInt32},
		{"int64", arrow.PrimitiveTypes.Int64, core.KindInt64},
		{"uint64", arrow.PrimitiveTypes.Uint64, core.KindUInt64},
		{"float32", arrow.PrimitiveTypes.Float32, core.KindFloat32},
		{"float64", arrow.PrimitiveTypes.Float64, core.KindFloat64},
		{"string", arrow.BinaryTypes.String, core.KindString},
		{"large_string", arrow.BinaryTypes.LargeString, core.KindString},
		{"binary", arrow.BinaryTypes.Binary, core.KindBinary},
		{"large_binary", arrow.BinaryTypes.LargeBinary, core.KindBinary},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_us, core.KindTimestampTZ},
		{"unmapped", arrow.FixedWidthTypes.Duration_s, core.KindUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := arrowTypeToCore(tc.in, true)
			if got.Kind != tc.want {
				t.Fatalf("kind = %v, want %v", got.Kind, tc.want)
			}
			if !got.Nullable {
				t.Fatal("nullable must be preserved")
			}
		})
	}
}

func TestReadValueMatrix(t *testing.T) {

	boolB := array.NewBooleanBuilder(memory.DefaultAllocator)
	boolB.Append(true)
	defer boolB.Release()
	if readValue(boolB.NewBooleanArray(), 0) != true {
		t.Fatal("bool")
	}

	int8B := array.NewInt8Builder(memory.DefaultAllocator)
	int8B.Append(8)
	defer int8B.Release()
	if readValue(int8B.NewInt8Array(), 0) != int32(8) {
		t.Fatal("int8")
	}

	int16B := array.NewInt16Builder(memory.DefaultAllocator)
	int16B.Append(16)
	defer int16B.Release()
	if readValue(int16B.NewInt16Array(), 0) != int32(16) {
		t.Fatal("int16")
	}

	int32B := array.NewInt32Builder(memory.DefaultAllocator)
	int32B.Append(32)
	defer int32B.Release()
	if readValue(int32B.NewInt32Array(), 0) != int32(32) {
		t.Fatal("int32")
	}

	int64B := array.NewInt64Builder(memory.DefaultAllocator)
	int64B.Append(64)
	defer int64B.Release()
	if readValue(int64B.NewInt64Array(), 0) != int64(64) {
		t.Fatal("int64")
	}

	uint8B := array.NewUint8Builder(memory.DefaultAllocator)
	uint8B.Append(8)
	defer uint8B.Release()
	if readValue(uint8B.NewUint8Array(), 0) != uint8(8) {
		t.Fatal("uint8")
	}

	uint16B := array.NewUint16Builder(memory.DefaultAllocator)
	uint16B.Append(16)
	defer uint16B.Release()
	if readValue(uint16B.NewUint16Array(), 0) != uint16(16) {
		t.Fatal("uint16")
	}

	uint32B := array.NewUint32Builder(memory.DefaultAllocator)
	uint32B.Append(32)
	defer uint32B.Release()
	if readValue(uint32B.NewUint32Array(), 0) != uint32(32) {
		t.Fatal("uint32")
	}

	uint64B := array.NewUint64Builder(memory.DefaultAllocator)
	uint64B.Append(64)
	defer uint64B.Release()
	if readValue(uint64B.NewUint64Array(), 0) != uint64(64) {
		t.Fatal("uint64")
	}

	f32B := array.NewFloat32Builder(memory.DefaultAllocator)
	f32B.Append(1.5)
	defer f32B.Release()
	if readValue(f32B.NewFloat32Array(), 0) != float32(1.5) {
		t.Fatal("float32")
	}

	f64B := array.NewFloat64Builder(memory.DefaultAllocator)
	f64B.Append(2.5)
	defer f64B.Release()
	if readValue(f64B.NewFloat64Array(), 0) != float64(2.5) {
		t.Fatal("float64")
	}

	strB := array.NewStringBuilder(memory.DefaultAllocator)
	strB.Append("x")
	defer strB.Release()
	if readValue(strB.NewStringArray(), 0) != "x" {
		t.Fatal("string")
	}

	binB := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	binB.Append([]byte{0x1})
	defer binB.Release()
	if string(readValue(binB.NewBinaryArray(), 0).([]byte)) != "\x01" {
		t.Fatal("binary")
	}

	tsB := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond})
	tsB.Append(arrow.Timestamp(time.Unix(1, 0).UnixMicro()))
	defer tsB.Release()
	if _, ok := readValue(tsB.NewTimestampArray(), 0).(time.Time); !ok {
		t.Fatal("timestamp")
	}

	durB := array.NewDurationBuilder(memory.DefaultAllocator, arrow.FixedWidthTypes.Duration_s.(*arrow.DurationType))
	durB.Append(1)
	defer durB.Release()
	if readValue(durB.NewDurationArray(), 0) != nil {
		t.Fatal("unmapped types must read as nil")
	}

	nullB := array.NewInt64Builder(memory.DefaultAllocator)
	nullB.AppendNull()
	defer nullB.Release()
	if readValue(nullB.NewInt64Array(), 0) != nil {
		t.Fatal("null must read as nil")
	}
}

func TestReadStringAndBinaryCol(t *testing.T) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "b", Type: arrow.BinaryTypes.Binary},
	}, nil))
	b.Field(0).(*array.StringBuilder).Append("hello")
	b.Field(0).(*array.StringBuilder).AppendNull()
	b.Field(1).(*array.BinaryBuilder).Append([]byte{0xde, 0xad})
	b.Field(1).(*array.BinaryBuilder).Append([]byte{0xbe, 0xef})
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()

	if got := readStringCol(rec, 0, 0); got != "hello" {
		t.Fatalf("readStringCol = %q", got)
	}
	if got := readStringCol(rec, 0, 1); got != "" {
		t.Fatalf("readStringCol(null) = %q", got)
	}
	if got := readStringCol(rec, -1, 0); got != "" {
		t.Fatalf("readStringCol(neg) = %q", got)
	}
	if got := readBinaryCol(rec, 1, 0); got != "3q0=" {
		t.Fatalf("readBinaryCol = %q", got)
	}
	if got := readBinaryCol(rec, -1, 0); got != "" {
		t.Fatalf("readBinaryCol(neg) = %q", got)
	}
}

func TestStructToMap(t *testing.T) {
	if structToMap(nil, 0) != nil {
		t.Fatal("nil column must map to nil")
	}

	st := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "b", Type: arrow.BinaryTypes.String},
	)
	sb := array.NewStructBuilder(memory.DefaultAllocator, st)
	defer sb.Release()
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(7)
	sb.FieldBuilder(1).(*array.StringBuilder).AppendNull()
	arr := sb.NewStructArray()
	defer arr.Release()

	m := structToMap(arr, 0)
	if m["a"] != int64(7) {
		t.Fatalf("struct map = %v", m)
	}
	if _, ok := m["b"]; ok {
		t.Fatalf("a null field must be omitted: %v", m)
	}

	// A non-struct column maps to nil.
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	ib.Append(1)
	defer ib.Release()
	if structToMap(ib.NewInt64Array(), 0) != nil {
		t.Fatal("non-struct must map to nil")
	}
}
