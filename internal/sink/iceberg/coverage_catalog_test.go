package iceberg

// Batch 4b: the catalog-facing half of the sink, exercised against a real
// in-process Iceberg catalog (hadoop) rooted in a t.TempDir(). No network and
// no Polaris: hadoop.NewCatalog is a file-backed catalog.Catalog, so the
// create/load/commit/transaction paths run for real, and the error-injection
// stubs cover the arms a working catalog cannot reach.

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// ── harness ──────────────────────────────────────────────────────────

func hadoopSink(t *testing.T) *Sink {
	t.Helper()
	cat, err := hadoop.NewCatalog("hadoop", t.TempDir(), iceberg.Properties{})
	if err != nil {
		t.Fatalf("hadoop.NewCatalog: %v", err)
	}
	if err := EnsureNamespace(context.Background(), cat, table.Identifier{"raw"}); err != nil {
		t.Fatalf("EnsureNamespace: %v", err)
	}
	return &Sink{cat: cat, ns: "raw"}
}

func canonicalSchema() core.Schema {
	return core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
}

func createOrders(t *testing.T, s *Sink) core.TableRef {
	t.Helper()
	ref := core.TableRef{Target: "orders", PrimaryKey: []string{"id"}}
	if err := s.EnsureTable(context.Background(), ref, canonicalSchema(), nil, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	return ref
}

// nsErrCatalog fails CreateNamespace with a non-already-exists error.
type nsErrCatalog struct {
	catalog.Catalog
	err error
}

func (c nsErrCatalog) CreateNamespace(context.Context, table.Identifier, iceberg.Properties) error {
	return c.err
}

// raceCatalog loses the create race: the first LoadTable reports the table
// absent, CreateTable reports a concurrent creator won, and the reload finds
// the winner.
type raceCatalog struct {
	catalog.Catalog
	tbl   *table.Table
	loads int
}

func (c *raceCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	c.loads++
	if c.loads == 1 {
		return nil, catalog.ErrNoSuchTable
	}
	return c.tbl, nil
}

func (c *raceCatalog) CreateTable(context.Context, table.Identifier, *iceberg.Schema, ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, catalog.ErrTableAlreadyExists
}

// ── client.go ────────────────────────────────────────────────────────

func TestEnsureNamespaceToleratesExisting(t *testing.T) {
	cat, err := hadoop.NewCatalog("hadoop", t.TempDir(), iceberg.Properties{})
	if err != nil {
		t.Fatalf("hadoop.NewCatalog: %v", err)
	}
	ctx := context.Background()
	ns := table.Identifier{"raw"}
	if err := EnsureNamespace(ctx, cat, ns); err != nil {
		t.Fatalf("EnsureNamespace(create): %v", err)
	}
	if err := EnsureNamespace(ctx, cat, ns); err != nil {
		t.Fatalf("EnsureNamespace(existing) must be tolerated: %v", err)
	}

	boom := errors.New("polaris 503")
	if err := EnsureNamespace(ctx, nsErrCatalog{err: boom}, ns); !errors.Is(err, boom) {
		t.Fatalf("EnsureNamespace(boom) = %v, want boom", err)
	}
}

func TestNewCatalogBuildsClient(t *testing.T) {
	cat, err := NewCatalog(context.Background(), Config{
		URI: "http://127.0.0.1:0/api/catalog", Warehouse: "wh",
		ClientID: "id", ClientSecret: "sec", Scope: "s",
	})
	if err != nil {
		// No server on the port: the constructor's error arm is what ran.
		return
	}
	if cat == nil {
		t.Fatal("NewCatalog returned a nil catalog and no error")
	}
}

// ── EnsureTable / createTable ────────────────────────────────────────

func TestEnsureTableCreatesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	if _, err := s.cat.LoadTable(ctx, s.ident("orders")); err != nil {
		t.Fatalf("table must exist after EnsureTable: %v", err)
	}
	if err := s.EnsureTable(ctx, ref, canonicalSchema(), nil, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable(existing): %v", err)
	}
}

func TestEnsureTableRejectsCastDivergence(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	cast, err := core.ParseCastPolicy(map[string]string{"id": "string"})
	if err != nil {
		t.Fatalf("ParseCastPolicy: %v", err)
	}
	widened := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindString}},
	}}
	if err := s.EnsureTable(ctx, ref, widened, nil, cast, dataplane.UpsertMode); err == nil {
		t.Fatal("cast type divergence must be rejected")
	}
}

func TestEnsureTableRejectsCastColumnMissingFromTable(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	cast, err := core.ParseCastPolicy(map[string]string{"extra": "string"})
	if err != nil {
		t.Fatalf("ParseCastPolicy: %v", err)
	}
	widened := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		{Name: "extra", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	if err := s.EnsureTable(ctx, ref, widened, nil, cast, dataplane.UpsertMode); err == nil {
		t.Fatal("cast column absent from the existing table must be rejected")
	}
}

func TestEnsureTablePartitionSpecDivergence(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := core.TableRef{Target: "orders", PrimaryKey: []string{"id"}}
	if err := s.EnsureTable(ctx, ref, canonicalSchema(), []string{"identity(id)"}, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable(partitioned create): %v", err)
	}
	// Same spec is compatible.
	if err := s.EnsureTable(ctx, ref, canonicalSchema(), []string{"identity(id)"}, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable(matching spec): %v", err)
	}
	// A different transform is not.
	if err := s.EnsureTable(ctx, ref, canonicalSchema(), []string{"bucket(4, id)"}, core.CastPolicy{}, dataplane.UpsertMode); err == nil {
		t.Fatal("partition spec divergence must be rejected")
	}
}

func TestEnsureTablePropagatesCatalogError(t *testing.T) {
	boom := errors.New("auth failure")
	err := EnsureTable(context.Background(), &loadErrCatalog{err: boom},
		table.Identifier{"raw", "orders"}, iceberg.NewSchema(0), nil, nil, core.CastPolicy{}, false)
	if !errors.Is(err, boom) {
		t.Fatalf("EnsureTable = %v, want boom", err)
	}
}

func TestEnsureTableCreateRaceReloadsWinner(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}

	cat := &raceCatalog{tbl: tbl}
	ischema, err := FromCanonical(canonicalSchema())
	if err != nil {
		t.Fatalf("FromCanonical: %v", err)
	}
	if err := EnsureTable(ctx, cat, s.ident("orders"), ischema, nil, nil, core.CastPolicy{}, false); err != nil {
		t.Fatalf("EnsureTable(create race) must validate the winner: %v", err)
	}
	if cat.loads != 2 {
		t.Fatalf("LoadTable calls = %d, want 2 (initial miss + reload)", cat.loads)
	}
}

// ── NewTableWriter / Sink.Writer ─────────────────────────────────────

func TestNewTableWriterBuildsFromTable(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)

	w, err := NewTableWriter(ctx, s.cat, s.ident("orders"), []string{"id"}, core.CastPolicy{}, nil, "src.orders", 0)
	if err != nil {
		t.Fatalf("NewTableWriter: %v", err)
	}
	if len(w.eqIDs) != 1 || w.eqIDs[0] != 1 {
		t.Fatalf("eqIDs = %v, want [1]", w.eqIDs)
	}
	if w.delSchema == nil || w.dataSchema == nil {
		t.Fatal("schemas must be built")
	}
	if w.sourceTable != "src.orders" {
		t.Fatalf("sourceTable = %q", w.sourceTable)
	}
}

func TestNewTableWriterErrors(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)

	if _, err := NewTableWriter(ctx, s.cat, s.ident("orders"), []string{"nope"}, core.CastPolicy{}, nil, "s", 0); err == nil {
		t.Fatal("unknown primary key column must be rejected")
	}
	boom := errors.New("catalog down")
	if _, err := NewTableWriter(ctx, &loadErrCatalog{err: boom}, s.ident("orders"), []string{"id"}, core.CastPolicy{}, nil, "s", 0); !errors.Is(err, boom) {
		t.Fatalf("NewTableWriter(load error) = %v, want boom", err)
	}
}

func TestSinkWriterAndMetadataColumns(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	meta := []core.MetadataColumn{{From: core.MetaOp, As: "_op"}}
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, meta)
	if err != nil {
		t.Fatalf("Sink.Writer: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// ── properties / position ────────────────────────────────────────────

func TestSinkPropertiesRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	props, err := s.Properties(ctx, ref)
	if err != nil {
		t.Fatalf("Properties: %v", err)
	}
	if len(props) != 0 {
		t.Fatalf("fresh table properties = %v, want empty", props)
	}

	if err := s.SetProperties(ctx, ref, map[string]string{"owner": "alice"}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	props, err = s.Properties(ctx, ref)
	if err != nil {
		t.Fatalf("Properties after set: %v", err)
	}
	if props["owner"] != "alice" {
		t.Fatalf("owner = %q, want alice", props["owner"])
	}

	if err := s.SetProperties(ctx, ref, nil); err != nil {
		t.Fatalf("SetProperties(nil) must be a no-op: %v", err)
	}
}

func TestSinkPropertiesMissingTableIsEmpty(t *testing.T) {
	s := hadoopSink(t)
	props, err := s.Properties(context.Background(), core.TableRef{Target: "nope"})
	if err != nil {
		t.Fatalf("Properties(missing) = %v, want nil error", err)
	}
	if len(props) != 0 {
		t.Fatalf("Properties(missing) = %v, want empty", props)
	}
}

func TestPositionReadsCommittedProperty(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)

	pos, err := s.Position(ctx, ref)
	if err != nil || pos != "" {
		t.Fatalf("Position(fresh) = %q, %v; want empty, nil", pos, err)
	}

	if err := s.SetProperties(ctx, ref, map[string]string{"cdc.position": "42"}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	pos, err = s.Position(ctx, ref)
	if err != nil || pos != "42" {
		t.Fatalf("Position = %q, %v; want 42, nil", pos, err)
	}

	if _, err := s.Position(ctx, core.TableRef{Target: "nope"}); err == nil {
		t.Fatal("Position(missing table) must fail")
	}
}

func TestCommittedPositionAndSetTablePropertiesErrors(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)

	boom := errors.New("catalog down")
	if _, err := CommittedPosition(ctx, &loadErrCatalog{err: boom}, s.ident("orders")); !errors.Is(err, boom) {
		t.Fatalf("CommittedPosition = %v, want boom", err)
	}
	if err := SetTableProperties(ctx, &loadErrCatalog{err: boom}, s.ident("orders"), iceberg.Properties{"k": "v"}); !errors.Is(err, boom) {
		t.Fatalf("SetTableProperties = %v, want boom", err)
	}
	if err := SetTableProperties(ctx, s.cat, s.ident("orders"), nil); err != nil {
		t.Fatalf("SetTableProperties(empty) must be a no-op: %v", err)
	}
}

// ── full write paths against a live table ────────────────────────────

func arrowRecord(t *testing.T, sch *arrow.Schema, fill func(*array.RecordBuilder)) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer b.Release()
	fill(b)
	return b.NewRecordBatch()
}

func TestTableWriterCommitUpsertEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}

	b := wireBatch(t,
		[3]any{int64(1), "a", rowchange.OpInsert},
		[3]any{int64(2), "b", rowchange.OpDelete},
	)
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit(upsert): %v", err)
	}
	pos, err := s.Position(ctx, ref)
	if err != nil {
		t.Fatalf("Position: %v", err)
	}
	if pos != "p" {
		t.Fatalf("position = %q, want p (the batch watermark)", pos)
	}
}

func TestTableWriterCommitAppendModeEndToEnd(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}

	b := wireBatch(t, [3]any{int64(9), "x", rowchange.OpInsert})
	defer b.Release()
	b.Mode = dataplane.AppendMode
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit(append): %v", err)
	}
	pos, _ := s.Position(ctx, ref)
	if pos != "p" {
		t.Fatalf("position = %q, want p", pos)
	}
}

func TestTableWriterCommitEmptyAppendIsNoop(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if err := w.Commit(ctx, &dataplane.Batch{Mode: dataplane.AppendMode}); err != nil {
		t.Fatalf("Commit(empty append): %v", err)
	}
}

func TestAppendAndDeleteOnlyPaths(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}

	dataSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatalf("data arrow schema: %v", err)
	}
	data := arrowRecord(t, dataSchema, func(rb *array.RecordBuilder) {
		rb.Field(0).(*array.Int64Builder).Append(1)
		rb.Field(1).(*array.StringBuilder).Append("a")
	})
	defer data.Release()
	if err := Append(ctx, tbl, data, iceberg.Properties{"cdc.position": "10"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	tbl, err = s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := committedPosition(tbl); got != "10" {
		t.Fatalf("position after Append = %q, want 10", got)
	}

	delISchema, err := tbl.Schema().Select(true, "id")
	if err != nil {
		t.Fatalf("select delete schema: %v", err)
	}
	delSchema, err := table.SchemaToArrowSchema(delISchema, nil, true, false)
	if err != nil {
		t.Fatalf("delete arrow schema: %v", err)
	}
	deletes := arrowRecord(t, delSchema, func(rb *array.RecordBuilder) {
		rb.Field(0).(*array.Int64Builder).Append(1)
	})
	defer deletes.Release()

	if err := DeleteOnly(ctx, tbl, []int{1}, deletes, iceberg.Properties{"cdc.position": "11"}); err != nil {
		t.Fatalf("DeleteOnly: %v", err)
	}
	tbl, _ = s.cat.LoadTable(ctx, s.ident("orders"))
	if got := committedPosition(tbl); got != "11" {
		t.Fatalf("position after DeleteOnly = %q, want 11", got)
	}

	if err := AppendAndDelete(ctx, tbl, data, []int{1}, deletes, iceberg.Properties{"cdc.position": "12"}); err != nil {
		t.Fatalf("AppendAndDelete: %v", err)
	}
}

func TestSinkEnsureTableRejectsBadSchema(t *testing.T) {
	s := hadoopSink(t)
	bad := core.Schema{Columns: []core.Column{
		{Name: "amount", Type: core.ColumnType{Kind: core.KindDecimal}},
	}}
	if err := s.EnsureTable(context.Background(), core.TableRef{Target: "x"}, bad, nil, core.CastPolicy{}, dataplane.UpsertMode); err == nil {
		t.Fatal("an unmappable schema must be rejected before touching the catalog")
	}
}
