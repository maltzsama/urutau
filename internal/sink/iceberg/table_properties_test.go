package iceberg

// Issue #463: the metadata/manifest housekeeping properties must be on every
// table urutau touches, on create and on an existing table's next boot.

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
)

// housekeepingTrue asserts both #463 properties are "true" on tbl.
func housekeepingTrue(t *testing.T, tbl *table.Table) {
	t.Helper()
	for k := range housekeepingProperties {
		if got := tbl.Properties().Get(k, ""); got != "true" {
			t.Fatalf("%s = %q, want \"true\"", k, got)
		}
	}
}

// A table urutau creates carries the housekeeping properties from the first
// commit, so metadata.json files and manifests stay bounded under CDC.
func TestCreateSetsHousekeepingProperties(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	housekeepingTrue(t, tbl)
}

// A table created before urutau set the properties converges on its next boot.
// The test creates it the old way (format-version only), then runs EnsureTable
// and asserts the properties were added.
func TestEnsureHousekeepingConvergesAnExistingTable(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	sch, err := FromCanonical(canonicalSchema())
	if err != nil {
		t.Fatalf("FromCanonical: %v", err)
	}
	if _, err := s.cat.CreateTable(ctx, s.ident("orders"), sch,
		catalog.WithProperties(iceberg.Properties{"format-version": "2"})); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	before, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if before.Properties().Get(table.ManifestMergeEnabledKey, "") == "true" {
		t.Fatal("precondition: the old table must not carry housekeeping")
	}

	if err := s.EnsureTable(ctx, core.TableRef{Target: "orders"}, canonicalSchema(), nil, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	after, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	housekeepingTrue(t, after)
}
