package iceberg

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// sortSchema is a two-column iceberg schema with deterministic field IDs.
func sortSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

// sortFields collects a sort order's fields in order (iceberg-go v0.6.0
// exposes them through Fields(), not an indexed accessor).
func sortFields(order table.SortOrder) []table.SortField {
	var out []table.SortField
	for _, f := range order.Fields() {
		out = append(out, f)
	}
	return out
}

func TestSortOrderForBuildsIdentityFields(t *testing.T) {
	order, err := sortOrderFor(sortSchema(), []string{"id"})
	if err != nil {
		t.Fatalf("sortOrderFor: %v", err)
	}
	if order.IsUnsorted() || order.Len() != 1 {
		t.Fatalf("order = %+v, want one field", order)
	}
	f := sortFields(order)[0]
	if f.SourceID() != 1 {
		t.Fatalf("source id = %d, want 1", f.SourceID())
	}
	if f.Direction != table.SortASC {
		t.Fatalf("direction = %q, want asc", f.Direction)
	}
	if f.NullOrder != table.NullsFirst {
		t.Fatalf("null order = %q, want nulls-first", f.NullOrder)
	}
	if f.Transform.String() != "identity" {
		t.Fatalf("transform = %q, want identity", f.Transform)
	}
}

func TestSortOrderForMultipleKeys(t *testing.T) {
	order, err := sortOrderFor(sortSchema(), []string{"id", "v"})
	if err != nil {
		t.Fatalf("sortOrderFor: %v", err)
	}
	if order.Len() != 2 {
		t.Fatalf("len = %d, want 2", order.Len())
	}
	fields := sortFields(order)
	if fields[0].SourceID() != 1 || fields[1].SourceID() != 2 {
		t.Fatalf("source ids = %d, %d; want 1, 2", fields[0].SourceID(), fields[1].SourceID())
	}
}

func TestSortOrderForRejectsUnknownColumn(t *testing.T) {
	if _, err := sortOrderFor(sortSchema(), []string{"nope"}); err == nil {
		t.Fatal("an unknown primary-key column must be rejected")
	}
}

// A table created with a primary key carries that key as its default sort
// order; one created without a primary key stays unsorted.
func TestEnsureTableSetsSortOrderFromPrimaryKey(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	order := tbl.SortOrder()
	if order.IsUnsorted() || order.Len() != 1 {
		t.Fatalf("sort order = %+v, want the id column", order)
	}
	idField, ok := tbl.Schema().FindFieldByName("id")
	if !ok {
		t.Fatal("id column missing")
	}
	if got := sortFields(order)[0].SourceID(); got != idField.ID {
		t.Fatalf("sort source id = %d, want %d", got, idField.ID)
	}
}

func TestEnsureTableWithoutPrimaryKeyIsUnsorted(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := core.TableRef{Target: "orders"} // no PrimaryKey
	if err := s.EnsureTable(ctx, ref, canonicalSchema(), nil, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if !tbl.SortOrder().IsUnsorted() {
		t.Fatalf("sort order = %+v, want unsorted", tbl.SortOrder())
	}
}
