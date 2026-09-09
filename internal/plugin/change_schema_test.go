package plugin

// Audit §3: the source side accepts third-party code in any language, so a
// malformed change-record schema must be REJECTED at the stream gate
// (contract §8.1 validated once, before any record), not panic deep in the
// row readers (op read as Int32 used to hit a hard *array.String assertion).

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// changeSchema builds a §8.1 change-record schema from field specs,
// defaulting to the valid shape when fields is nil.
func changeSchema(t *testing.T, fields ...arrow.Field) *arrow.Schema {
	t.Helper()
	if fields == nil {
		structType := arrow.StructOf(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64})
		fields = []arrow.Field{
			{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "before", Type: structType, Nullable: true},
			{Name: "after", Type: structType, Nullable: true},
			{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
			{Name: "ts_source", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
		}
	}
	return arrow.NewSchema(fields, nil)
}

func TestValidateChangeSchemaAcceptsValidShapes(t *testing.T) {
	if err := validateChangeSchema(changeSchema(t)); err != nil {
		t.Fatalf("full schema must pass: %v", err)
	}

	// before MAY be omitted entirely (§8.1).
	structType := arrow.StructOf(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64})
	noBefore := changeSchema(t,
		arrow.Field{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "after", Type: structType, Nullable: true},
		arrow.Field{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
		arrow.Field{Name: "ts_source", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}, Nullable: true},
	)
	if err := validateChangeSchema(noBefore); err != nil {
		t.Fatalf("before-omitted schema must pass: %v", err)
	}
}

func TestValidateChangeSchemaRejectsMisbehavingPlugins(t *testing.T) {
	structType := arrow.StructOf(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64})
	tsNS := &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}

	cases := []struct {
		name   string
		fields []arrow.Field
		wantIn string // error must mention this
	}{
		{
			// THE case from the audit: op as Int32 used to panic
			// readStringCol's hard *array.String assertion.
			name: "op as int32",
			fields: []arrow.Field{
				{Name: "op", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				{Name: "before", Type: structType, Nullable: true},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
				{Name: "ts_source", Type: tsNS, Nullable: true},
			},
			wantIn: "Utf8",
		},
		{
			// offset as Utf8 — the most natural plugin mistake.
			name: "offset as utf8",
			fields: []arrow.Field{
				{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "before", Type: structType, Nullable: true},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "ts_source", Type: tsNS, Nullable: true},
			},
			wantIn: "Binary",
		},
		{
			name: "before as list, not struct",
			fields: []arrow.Field{
				{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "before", Type: arrow.ListOf(structType), Nullable: true},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
				{Name: "ts_source", Type: tsNS, Nullable: true},
			},
			wantIn: "Struct",
		},
		{
			name: "ts_source in microseconds, not ns",
			fields: []arrow.Field{
				{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "before", Type: structType, Nullable: true},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
				{Name: "ts_source", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
			},
			wantIn: "Timestamp(ns",
		},
		{
			name: "ts_source without timezone",
			fields: []arrow.Field{
				{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "before", Type: structType, Nullable: true},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
				{Name: "ts_source", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}, Nullable: true},
			},
			wantIn: "UTC",
		},
		{
			name: "op nullable",
			fields: []arrow.Field{
				{Name: "op", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "before", Type: structType, Nullable: true},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
				{Name: "ts_source", Type: tsNS, Nullable: true},
			},
			wantIn: "nullability",
		},
		{
			name: "field out of order",
			fields: []arrow.Field{
				{Name: "before", Type: structType, Nullable: true},
				{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
				{Name: "ts_source", Type: tsNS, Nullable: true},
			},
			wantIn: `field 0 is "before"`,
		},
		{
			name: "extra field",
			fields: []arrow.Field{
				{Name: "op", Type: arrow.BinaryTypes.String, Nullable: false},
				{Name: "before", Type: structType, Nullable: true},
				{Name: "after", Type: structType, Nullable: true},
				{Name: "offset", Type: arrow.BinaryTypes.Binary, Nullable: false},
				{Name: "ts_source", Type: tsNS, Nullable: true},
				{Name: "extra", Type: arrow.BinaryTypes.String, Nullable: true},
			},
			wantIn: "want 4 (before omitted) or 5",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateChangeSchema(changeSchema(t, tc.fields...))
			if err == nil {
				t.Fatal("malformed schema must be rejected at the gate")
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Fatalf("error %q must mention %q", err, tc.wantIn)
			}
		})
	}
}

func TestRecordMatchesAnnouncedSchema(t *testing.T) {
	announced := changeSchema(t)
	if err := recordMatchesAnnouncedSchema(announced, announced); err != nil {
		t.Fatalf("identical schemas must match: %v", err)
	}

	// Same fields, different order — the drift that shifts every column read.
	drifted := arrow.NewSchema([]arrow.Field{
		announced.Field(1), announced.Field(0), announced.Field(2),
		announced.Field(3), announced.Field(4),
	}, nil)
	if err := recordMatchesAnnouncedSchema(drifted, announced); err == nil {
		t.Fatal("field-order drift must be rejected")
	}

	// Same names, different type.
	wrongType := changeSchema(t,
		announced.Field(0),
		announced.Field(1),
		announced.Field(2),
		arrow.Field{Name: "offset", Type: arrow.BinaryTypes.String, Nullable: false},
		announced.Field(4),
	)
	if err := recordMatchesAnnouncedSchema(wrongType, announced); err == nil {
		t.Fatal("type drift must be rejected")
	}
}
