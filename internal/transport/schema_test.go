package transport

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/core"
)

func TestKindToArrow(t *testing.T) {
	tests := []struct {
		name string
		ct   core.ColumnType
		want arrow.DataType
		err  bool
	}{
		{"bool", core.ColumnType{Kind: core.KindBool}, arrow.FixedWidthTypes.Boolean, false},
		{"int32", core.ColumnType{Kind: core.KindInt32}, arrow.PrimitiveTypes.Int32, false},
		{"int64", core.ColumnType{Kind: core.KindInt64}, arrow.PrimitiveTypes.Int64, false},
		{"uint64", core.ColumnType{Kind: core.KindUInt64}, arrow.PrimitiveTypes.Uint64, false},
		{"float32", core.ColumnType{Kind: core.KindFloat32}, arrow.PrimitiveTypes.Float32, false},
		{"float64", core.ColumnType{Kind: core.KindFloat64}, arrow.PrimitiveTypes.Float64, false},
		{"string", core.ColumnType{Kind: core.KindString}, arrow.BinaryTypes.String, false},
		{"json", core.ColumnType{Kind: core.KindJSON}, arrow.BinaryTypes.String, false},
		{"binary", core.ColumnType{Kind: core.KindBinary}, arrow.BinaryTypes.Binary, false},
		{"date", core.ColumnType{Kind: core.KindDate}, arrow.FixedWidthTypes.Date32, false},
		{"time", core.ColumnType{Kind: core.KindTime}, arrow.FixedWidthTypes.Time64us, false},
		{"timestamp", core.ColumnType{Kind: core.KindTimestamp}, &arrow.TimestampType{Unit: arrow.Microsecond}, false},
		{"timestamptz", core.ColumnType{Kind: core.KindTimestampTZ}, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, false},
		{"uuid", core.ColumnType{Kind: core.KindUUID}, &arrow.FixedSizeBinaryType{ByteWidth: 16}, false},
		{"decimal", core.ColumnType{Kind: core.KindDecimal, Precision: 10, Scale: 2}, &arrow.Decimal128Type{Precision: 10, Scale: 2}, false},
		{"decimal no precision", core.ColumnType{Kind: core.KindDecimal}, nil, true},
		{"fixed binary", core.ColumnType{Kind: core.KindFixedBinary, FixedSize: 16}, &arrow.FixedSizeBinaryType{ByteWidth: 16}, false},
		{"fixed no size", core.ColumnType{Kind: core.KindFixedBinary}, nil, true},
		{"struct", core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "a", Type: core.ColumnType{Kind: core.KindInt64}},
		}}, arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64}), false},
		{"struct no fields", core.ColumnType{Kind: core.KindStruct}, nil, true},
		{"list", core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindInt64}}, arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), false},
		{"list no elem", core.ColumnType{Kind: core.KindList}, nil, true},
		{"map", core.ColumnType{Kind: core.KindMap, KeyType: &core.ColumnType{Kind: core.KindString}, ValueType: &core.ColumnType{Kind: core.KindInt64, Nullable: true}}, nil, false},
		{"map no key", core.ColumnType{Kind: core.KindMap}, nil, true},
		{"unsupported", core.ColumnType{Kind: core.KindUnknown}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := KindToArrow(tt.ct)
			if (err != nil) != tt.err {
				t.Errorf("KindToArrow(%+v) error = %v, wantErr %v", tt.ct, err, tt.err)
				return
			}
			if tt.err {
				return
			}
			if tt.want == nil {
				return // complex type, just check no error
			}
			if got.ID() != tt.want.ID() {
				t.Errorf("KindToArrow(%+v) = %v, want %v", tt.ct, got, tt.want)
			}
		})
	}
}

func TestIsMetadataColumn(t *testing.T) {
	metadata := []string{"__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot", "__phase"}
	for _, name := range metadata {
		if !isMetadataColumn(name) {
			t.Errorf("isMetadataColumn(%q) = false, want true", name)
		}
	}
	nonMetadata := []string{"id", "name", "__op_extra", "op"}
	for _, name := range nonMetadata {
		if isMetadataColumn(name) {
			t.Errorf("isMetadataColumn(%q) = true, want false", name)
		}
	}
}

func TestIsReservedColumnName(t *testing.T) {
	reserved := []string{"__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot", "__phase"}
	for _, name := range reserved {
		if !isReservedColumnName(name) {
			t.Errorf("isReservedColumnName(%q) = false, want true", name)
		}
	}
	nonReserved := []string{"id", "name", "__op_extra", "op"}
	for _, name := range nonReserved {
		if isReservedColumnName(name) {
			t.Errorf("isReservedColumnName(%q) = true, want false", name)
		}
	}
}

func TestCoreSchemaToArrowRejectsReserved(t *testing.T) {
	cs := core.Schema{Columns: []core.Column{
		{Name: "__op", Type: core.ColumnType{Kind: core.KindString}},
	}}
	_, err := CoreSchemaToArrow(cs)
	if err == nil {
		t.Error("CoreSchemaToArrow should reject reserved column names")
	}
}

func TestWireMetadataFields(t *testing.T) {
	fields := WireMetadataFields()
	if len(fields) != 6 {
		t.Fatalf("WireMetadataFields() returned %d fields, want 6", len(fields))
	}
	names := []string{"__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot", "__phase"}
	for i, name := range names {
		if fields[i].Name != name {
			t.Errorf("WireMetadataFields()[%d].Name = %q, want %q", i, fields[i].Name, name)
		}
	}
}
