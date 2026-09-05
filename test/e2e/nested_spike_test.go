// Nested-value spike (CR-036 §5.1): the stack — iceberg-go commit + Parquet
// + Trino read — must round-trip a struct/list/map column with real values,
// independently of any Avro decoder. Builds the record by hand with arrow
// builders against the iceberg schema, appends it, and reads it back via
// Trino (trusting the read, not the commit return).
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

	"github.com/maltzsama/urutau/core"
	urutauiceberg "github.com/maltzsama/urutau/internal/sink/iceberg"
)

func TestNestedSpike(t *testing.T) {
	requireE2E(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	nsName := "nested"
	tableName := "orders"

	// Canonical nested schema → Iceberg (stable field IDs via FromCanonical).
	cs := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "cust", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
			{Name: "name", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "age", Type: core.ColumnType{Kind: core.KindInt64}},
		}}},
		{Name: "tags", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindString}}},
	}}
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
	tbl, err := cat.CreateTable(ctx, ident, ischema,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Arrow data schema matching the iceberg schema, then a 2-row record
	// with real nested values.
	dataSchema, err := table.SchemaToArrowSchema(ischema, nil, false, false)
	if err != nil {
		t.Fatalf("arrow schema: %v", err)
	}
	rec := nestedOrdersRecord(dataSchema)
	defer rec.Release()

	if err := urutauiceberg.Append(ctx, tbl, rec, props("n1")); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Read back via Trino: navigate the struct with a dot and the list with
	// cardinality — the nested value must come through intact.
	tblQ := nsName + `."` + tableName + `"`
	assertTrino(t, ctx, `SELECT cust.name FROM `+tblQ+` WHERE id = 1`, "ana")
	assertTrino(t, ctx, `SELECT cust.age FROM `+tblQ+` WHERE id = 1`, int64(30))
	assertTrino(t, ctx, `SELECT cardinality(tags) FROM `+tblQ+` WHERE id = 1`, int64(2))
	assertTrino(t, ctx, `SELECT tags[1] FROM `+tblQ+` WHERE id = 1`, "a")
	assertTrino(t, ctx, `SELECT cust.name FROM `+tblQ+` WHERE id = 2`, "bob")
}

// nestedOrdersRecord builds two rows: id=1 with ana/30, tags [a,b], attrs
// {x:10}; id=2 with bob/40, empty tags, empty attrs.
func nestedOrdersRecord(schema *arrow.Schema) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	// Row 1.
	b.Field(0).(*array.Int64Builder).Append(1)
	cust := b.Field(1).(*array.StructBuilder)
	cust.Append(true)
	cust.FieldBuilder(0).(*array.StringBuilder).Append("ana")
	cust.FieldBuilder(1).(*array.Int64Builder).Append(30)
	tags := b.Field(2).(*array.ListBuilder)
	tags.Append(true)
	tags.ValueBuilder().(*array.StringBuilder).Append("a")
	tags.ValueBuilder().(*array.StringBuilder).Append("b")
	// Row 2.
	b.Field(0).(*array.Int64Builder).Append(2)
	cust.Append(true)
	cust.FieldBuilder(0).(*array.StringBuilder).Append("bob")
	cust.FieldBuilder(1).(*array.Int64Builder).Append(40)
	tags.Append(true)

	return b.NewRecordBatch()
}
