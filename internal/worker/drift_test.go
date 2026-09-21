package worker

// Nested schema-drift detection, against the columnar batch the worker
// actually sees. These cases were previously covered against raw row maps
// (the removed checkDrift), which could not catch the real gap: the live
// columnar check never descended into struct columns, so a field added
// inside a declared struct was silently dropped while an equivalent
// top-level column stopped the table.

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// Declared: id, address{city, geo{lat}}.
func nestedSchema() core.Schema {
	return core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "address", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "geo", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
				{Name: "lat", Type: core.ColumnType{Kind: core.KindFloat64}},
			}}},
		}}},
	}}
}

// driftBatch builds a one-row batch from a schema and per-column builders.
// Callers describe the ARRIVING shape, which may differ from the declared
// one — that difference is what the test is about.
func driftBatch(t *testing.T, fields []arrow.Field, fill func(*array.RecordBuilder)) *dataplane.Batch {
	t.Helper()
	schema := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer rb.Release()
	fill(rb)
	rec := rb.NewRecordBatch()
	t.Cleanup(rec.Release)
	return &dataplane.Batch{Table: "t", Record: rec}
}

func geoOf(extra ...arrow.Field) arrow.Field {
	f := []arrow.Field{{Name: "lat", Type: arrow.PrimitiveTypes.Float64}}
	return arrow.Field{Name: "geo", Type: arrow.StructOf(append(f, extra...)...)}
}

func addressOf(geo arrow.Field, extra ...arrow.Field) arrow.Field {
	f := []arrow.Field{{Name: "city", Type: arrow.BinaryTypes.String}}
	f = append(f, extra...)
	return arrow.Field{Name: "address", Type: arrow.StructOf(append(f, geo)...)}
}

var idField = arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64}

// A field added one level down must be reported by its dotted path. This is
// the case the live check missed entirely.
func TestSchemaDriftNestedTopLevelStructField(t *testing.T) {
	b := driftBatch(t,
		[]arrow.Field{idField, addressOf(geoOf(), arrow.Field{Name: "complement", Type: arrow.BinaryTypes.String})},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			addr := rb.Field(1).(*array.StructBuilder)
			addr.Append(true)
			addr.FieldBuilder(0).(*array.StringBuilder).Append("sp")
			addr.FieldBuilder(1).(*array.StringBuilder).Append("apto 4")
			geo := addr.FieldBuilder(2).(*array.StructBuilder)
			geo.Append(true)
			geo.FieldBuilder(0).(*array.Float64Builder).Append(-23.5)
		})

	d, hit, err := schemaDrift(b, nestedSchema())
	if err != nil {
		t.Fatalf("schemaDrift: %v", err)
	}
	if !hit {
		t.Fatal("drift must be detected")
	}
	if d.Column != "address.complement" || d.Kind != "added" {
		t.Fatalf("drift = %+v, want address.complement/added", d)
	}
}

// Two levels down: the path must accumulate through every struct.
func TestSchemaDriftNestedReportsDeepPath(t *testing.T) {
	b := driftBatch(t,
		[]arrow.Field{idField, addressOf(geoOf(arrow.Field{Name: "extra", Type: arrow.FixedWidthTypes.Boolean}))},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			addr := rb.Field(1).(*array.StructBuilder)
			addr.Append(true)
			addr.FieldBuilder(0).(*array.StringBuilder).Append("sp")
			geo := addr.FieldBuilder(1).(*array.StructBuilder)
			geo.Append(true)
			geo.FieldBuilder(0).(*array.Float64Builder).Append(-23.5)
			geo.FieldBuilder(1).(*array.BooleanBuilder).Append(true)
		})

	d, hit, err := schemaDrift(b, nestedSchema())
	if err != nil {
		t.Fatalf("schemaDrift: %v", err)
	}
	if !hit {
		t.Fatal("deep drift must be detected")
	}
	if d.Column != "address.geo.extra" {
		t.Fatalf("column = %q, want the full dotted path address.geo.extra", d.Column)
	}
}

// The mirror: a batch matching the declaration must pass, or every nested
// pipeline would stop on its first batch.
func TestSchemaDriftNestedConformingValuePasses(t *testing.T) {
	b := driftBatch(t,
		[]arrow.Field{idField, addressOf(geoOf())},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			addr := rb.Field(1).(*array.StructBuilder)
			addr.Append(true)
			addr.FieldBuilder(0).(*array.StringBuilder).Append("sp")
			geo := addr.FieldBuilder(1).(*array.StructBuilder)
			geo.Append(true)
			geo.FieldBuilder(0).(*array.Float64Builder).Append(-23.5)
		})

	if _, hit, err := schemaDrift(b, nestedSchema()); err != nil || hit {
		t.Fatalf("a conforming nested value must not report drift (hit=%v err=%v)", hit, err)
	}
}

// Top-level detection must keep working unchanged.
func TestSchemaDriftTopLevelAddedColumn(t *testing.T) {
	b := driftBatch(t,
		[]arrow.Field{idField, {Name: "surprise", Type: arrow.BinaryTypes.String}},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			rb.Field(1).(*array.StringBuilder).Append("new")
		})

	d, hit, err := schemaDrift(b, nestedSchema())
	if err != nil {
		t.Fatalf("schemaDrift: %v", err)
	}
	if !hit || d.Column != "surprise" || d.Kind != "added" {
		t.Fatalf("drift = %+v, want top-level surprise/added", d)
	}
}

// The null rule applies at depth exactly as it does at the top: an all-null
// extra field is a padding artifact of schema merging, not a source value.
func TestSchemaDriftNestedNullExtraIsPadding(t *testing.T) {
	b := driftBatch(t,
		[]arrow.Field{idField, addressOf(geoOf(), arrow.Field{Name: "padding", Type: arrow.BinaryTypes.String, Nullable: true})},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			addr := rb.Field(1).(*array.StructBuilder)
			addr.Append(true)
			addr.FieldBuilder(0).(*array.StringBuilder).Append("sp")
			addr.FieldBuilder(1).(*array.StringBuilder).AppendNull()
			geo := addr.FieldBuilder(2).(*array.StructBuilder)
			geo.Append(true)
			geo.FieldBuilder(0).(*array.Float64Builder).Append(-23.5)
		})

	if _, hit, err := schemaDrift(b, nestedSchema()); err != nil || hit {
		t.Fatalf("an all-null nested extra is padding, not drift (hit=%v err=%v)", hit, err)
	}
}

// A null struct carries no nested values to judge; descending into it would
// read field values that belong to no row.
func TestSchemaDriftNullStructIsNotDrift(t *testing.T) {
	b := driftBatch(t,
		[]arrow.Field{idField, addressOf(geoOf(), arrow.Field{Name: "complement", Type: arrow.BinaryTypes.String, Nullable: true})},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			addr := rb.Field(1).(*array.StructBuilder)
			addr.AppendNull()
		})

	if _, hit, err := schemaDrift(b, nestedSchema()); err != nil || hit {
		t.Fatalf("a null struct must not report drift (hit=%v err=%v)", hit, err)
	}
}

// A declared struct field that conforms must not stop the scan: the drifted
// field sits AFTER it in field order, so a check that returned on the first
// declared struct instead of continuing would miss it.
func TestSchemaDriftContinuesPastConformingStruct(t *testing.T) {
	// address{city, geo{lat}, late} — geo conforms, late is the addition.
	b := driftBatch(t,
		[]arrow.Field{idField, {Name: "address", Type: arrow.StructOf(
			arrow.Field{Name: "city", Type: arrow.BinaryTypes.String},
			arrow.Field{Name: "geo", Type: arrow.StructOf(arrow.Field{Name: "lat", Type: arrow.PrimitiveTypes.Float64})},
			arrow.Field{Name: "late", Type: arrow.BinaryTypes.String},
		)}},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			addr := rb.Field(1).(*array.StructBuilder)
			addr.Append(true)
			addr.FieldBuilder(0).(*array.StringBuilder).Append("sp")
			geo := addr.FieldBuilder(1).(*array.StructBuilder)
			geo.Append(true)
			geo.FieldBuilder(0).(*array.Float64Builder).Append(-23.5)
			addr.FieldBuilder(2).(*array.StringBuilder).Append("arrived late")
		})

	d, hit, err := schemaDrift(b, nestedSchema())
	if err != nil {
		t.Fatalf("schemaDrift: %v", err)
	}
	if !hit || d.Column != "address.late" {
		t.Fatalf("drift = %+v, want address.late — the scan stopped at the conforming struct", d)
	}
}

// #261: a column null in row 0 but populated later is drift, not padding.
func TestSchemaDriftNullFirstRowIsStillDrift(t *testing.T) {
	b := driftBatch(t,
		[]arrow.Field{idField, {Name: "late", Type: arrow.BinaryTypes.String, Nullable: true}},
		func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.Int64Builder).Append(1)
			rb.Field(1).(*array.StringBuilder).AppendNull()
			rb.Field(0).(*array.Int64Builder).Append(2)
			rb.Field(1).(*array.StringBuilder).Append("present")
		})
	d, hit, err := schemaDrift(b, testSchema()) // testSchema has no "late"
	if err != nil {
		t.Fatalf("schemaDrift: %v", err)
	}
	if !hit || d.Column != "late" {
		t.Fatalf("drift = %+v, want late (null first row, value later)", d)
	}
}
