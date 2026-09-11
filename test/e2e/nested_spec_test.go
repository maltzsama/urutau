// Nested-column spec spike (issue #17, Decision 1): proves the spec's
// nested-type declaration (spec.ColumnDecl: struct/list/map) is not just a
// parser in isolation — it feeds the same Introspect → Iceberg → Trino path
// TestNestedSpike already proved works, starting from the YAML/JSON shape
// an operator would actually write, not a core.Schema built by hand.
package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	urutauiceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
	kafkasource "github.com/maltzsama/urutau/internal/source/kafka"
	"github.com/maltzsama/urutau/spec"
)

func TestNestedSpecSpike(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	nsName := "nested_spec"
	tableName := "orders"

	// The operator-facing declaration: nested columns as YAML-native
	// structural objects, not a textual type grammar.
	tbl := spec.Table{
		Source:     "shop.orders",
		Target:     nsName + "." + tableName,
		PrimaryKey: []string{"id"},
		Columns: map[string]spec.ColumnDecl{
			"id": {Scalar: "int64"},
			"cust": {Struct: map[string]spec.ColumnDecl{
				"name": {Scalar: "string"},
				"age":  {Scalar: "int64"},
			}},
			"tags": {List: &spec.ColumnDecl{Scalar: "string"}},
		},
	}

	_, cs, warns, err := (kafkasource.Source{}).Introspect(ctx, tbl)
	if err != nil {
		t.Fatalf("introspect: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}

	ischema, err := urutauiceberg.FromCanonical(cs)
	if err != nil {
		t.Fatalf("from canonical: %v", err)
	}

	cat, err := urutauiceberg.NewCatalog(ctx, sinkConfig())
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := urutauiceberg.EnsureNamespace(ctx, cat, table.Identifier{nsName}); err != nil {
		t.Fatalf("namespace: %v", err)
	}
	ident := table.Identifier{nsName, tableName}
	_ = cat.DropTable(ctx, ident)
	icebergTbl, err := cat.CreateTable(ctx, ident, ischema,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	dataSchema, err := table.SchemaToArrowSchema(ischema, nil, false, false)
	if err != nil {
		t.Fatalf("arrow schema: %v", err)
	}
	// Introspect sorts columns alphabetically (cust, id, tags) — this
	// builder follows that order, not declaration order.
	rec := nestedSpecOrdersRecord(dataSchema)
	defer rec.Release()

	if err := urutauiceberg.Append(ctx, icebergTbl, rec, props("n1")); err != nil {
		t.Fatalf("append: %v", err)
	}

	tblQ := nsName + `."` + tableName + `"`
	assertTrino(t, ctx, `SELECT cust.name FROM `+tblQ+` WHERE id = 1`, "ana")
	assertTrino(t, ctx, `SELECT cust.age FROM `+tblQ+` WHERE id = 1`, int64(30))
	assertTrino(t, ctx, `SELECT cardinality(tags) FROM `+tblQ+` WHERE id = 1`, int64(2))
	assertTrino(t, ctx, `SELECT tags[1] FROM `+tblQ+` WHERE id = 1`, "a")
	assertTrino(t, ctx, `SELECT cust.name FROM `+tblQ+` WHERE id = 2`, "bob")
}

// nestedSpecOrdersRecord builds the same two rows as nestedOrdersRecord,
// but in the column/field order Resolve/Introspect actually produce:
// alphabetical at every level (top-level columns AND struct fields), since
// the spec declares both as unordered maps. cust = {age, name} (not
// name, age); top level = {cust, id, tags} (not id, cust, tags).
func nestedSpecOrdersRecord(schema *arrow.Schema) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	cust := b.Field(0).(*array.StructBuilder)
	id := b.Field(1).(*array.Int64Builder)
	tags := b.Field(2).(*array.ListBuilder)
	custAge := cust.FieldBuilder(0).(*array.Int64Builder)
	custName := cust.FieldBuilder(1).(*array.StringBuilder)

	// Row 1: id=1, cust={ana,30}, tags=[a,b].
	id.Append(1)
	cust.Append(true)
	custAge.Append(30)
	custName.Append("ana")
	tags.Append(true)
	tags.ValueBuilder().(*array.StringBuilder).Append("a")
	tags.ValueBuilder().(*array.StringBuilder).Append("b")

	// Row 2: id=2, cust={bob,40}, tags=[].
	id.Append(2)
	cust.Append(true)
	custAge.Append(40)
	custName.Append("bob")
	tags.Append(true)

	return b.NewRecordBatch()
}
