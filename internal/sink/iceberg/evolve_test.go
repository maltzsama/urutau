package iceberg

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// withExtra returns a canonical schema with an extra nullable column.
func withExtra() core.Schema {
	cs := canonicalSchema()
	cs.Columns = append(cs.Columns, core.Column{Name: "extra", Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}})
	return cs
}

func ensure(t *testing.T, s *Sink, ref core.TableRef, schema core.Schema) {
	t.Helper()
	if err := s.EnsureTable(context.Background(), ref, schema, nil, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
}

func TestEvolveSchemaFromOption(t *testing.T) {
	if !evolveSchemaFrom("true") {
		t.Fatal("the exact \"true\" must enable evolution")
	}
	for _, s := range []string{"", "false", "1", "TRUE"} {
		if evolveSchemaFrom(s) {
			t.Fatalf("evolveSchemaFrom(%q) must stay fail-closed", s)
		}
	}
}

func TestEvolveAddsColumn(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	s.evolveSchema = true
	ensure(t, s, ref, withExtra())

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	f, ok := tbl.Schema().FindFieldByName("extra")
	if !ok {
		t.Fatal("extra column was not added")
	}
	if f.Required {
		t.Fatal("an added column must be nullable")
	}
}

func TestEvolvePromotesIntAndFloat(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := core.TableRef{Target: "orders", PrimaryKey: []string{"id"}}

	base := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt32}},
		{Name: "f", Type: core.ColumnType{Kind: core.KindFloat32, Nullable: true}},
	}}
	ensure(t, s, ref, base)

	s.evolveSchema = true
	evolved := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "f", Type: core.ColumnType{Kind: core.KindFloat64, Nullable: true}},
	}}
	ensure(t, s, ref, evolved)

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	id, _ := tbl.Schema().FindFieldByName("id")
	if !id.Type.Equals(iceberg.PrimitiveTypes.Int64) {
		t.Fatalf("id type = %s, want long", id.Type)
	}
	f, _ := tbl.Schema().FindFieldByName("f")
	if !f.Type.Equals(iceberg.PrimitiveTypes.Float64) {
		t.Fatalf("f type = %s, want double", f.Type)
	}
}

func TestEvolveRejectsNarrowing(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s) // id is int64

	s.evolveSchema = true
	narrowed := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt32}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	if err := s.EnsureTable(context.Background(), ref, narrowed, nil, core.CastPolicy{}, dataplane.UpsertMode); err == nil {
		t.Fatal("a narrowing promotion must be rejected")
	}
}

func TestEvolveRejectsRequiredAddedColumn(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)

	s.evolveSchema = true
	required := canonicalSchema()
	required.Columns = append(required.Columns, core.Column{
		Name: "extra", Type: core.ColumnType{Kind: core.KindInt64}, // not nullable
	})
	if err := s.EnsureTable(context.Background(), ref, required, nil, core.CastPolicy{}, dataplane.UpsertMode); err == nil {
		t.Fatal("adding a required column with no default must be rejected")
	}
}

func TestEvolveAddsNestedField(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := core.TableRef{Target: "orders", PrimaryKey: []string{"id"}}

	base := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "s", Type: core.ColumnType{Kind: core.KindStruct, Nullable: true, Fields: []core.Column{
			{Name: "a", Type: core.ColumnType{Kind: core.KindInt64}},
		}}},
	}}
	ensure(t, s, ref, base)

	s.evolveSchema = true
	evolved := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "s", Type: core.ColumnType{Kind: core.KindStruct, Nullable: true, Fields: []core.Column{
			{Name: "a", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "b", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		}}},
	}}
	ensure(t, s, ref, evolved)

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	sf, ok := tbl.Schema().FindFieldByName("s")
	if !ok {
		t.Fatal("struct column missing")
	}
	st, ok := sf.Type.(*iceberg.StructType)
	if !ok {
		t.Fatalf("s is %T, want struct", sf.Type)
	}
	if _, ok := findNestedField(st.FieldList, "b"); !ok {
		t.Fatalf("nested field b not added: %+v", st.FieldList)
	}
}

// An unchanged schema must not write table metadata: the evolution
// transaction no-ops, so a steady-state boot leaves the metadata location
// untouched.
func TestEvolveNoOpWritesNoMetadata(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	before, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	loc := before.MetadataLocation()

	s.evolveSchema = true
	ensure(t, s, ref, canonicalSchema()) // identical

	after, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if after.MetadataLocation() != loc {
		t.Fatalf("no-op evolution wrote metadata: %q -> %q", loc, after.MetadataLocation())
	}
}

// With evolution off, an added column is not applied to the existing table
// (the sink stays fail-closed; the worker's drift check is what rejects the
// batch).
func TestEvolveOffDoesNotAddColumn(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	// evolveSchema defaults to false.
	ensure(t, s, ref, withExtra())

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if _, ok := tbl.Schema().FindFieldByName("extra"); ok {
		t.Fatal("with evolution off the column must not be added")
	}
}
