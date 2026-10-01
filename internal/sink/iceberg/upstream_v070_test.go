package iceberg

// Issue #464: behaviour of apache/iceberg-go v0.7.0 that differs from v0.6.0
// on the paths the sink uses. Each test pins what the sink gets from upstream
// today, so a change between the release candidate and the final shows up
// here and not in a pipeline.

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// scanIDs reads the id column of every row the table's current snapshot
// shows, in scan order.
func scanIDs(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()
	_, recs, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	var ids []int64
	for rec, err := range recs {
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, rec.Column(0).(*array.Int64).Int64Values()...)
		rec.Release()
	}
	return ids
}

// dataFilePaths lists the data files of the table's current snapshot as local
// paths (the hadoop catalog of the tests writes under a t.TempDir()).
func dataFilePaths(t *testing.T, tbl *table.Table) []string {
	t.Helper()
	tasks, err := tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatalf("plan files: %v", err)
	}
	var paths []string
	for _, task := range tasks {
		paths = append(paths, strings.TrimPrefix(task.File.FilePath(), "file://"))
	}
	return paths
}

// A table created with a primary key has that key as its default sort order
// (sortOrderFor). v0.6.0 recorded the order and wrote rows as they arrived;
// v0.7.0 sorts every batch by it before writing (apache/iceberg-go#1157). The
// sort is per batch, so it costs the worker one extra copy of each batch it
// writes.
func TestUpstreamSortsEachBatchByThePrimaryKeyOnWrite(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	b := wireBatch(t,
		[3]any{int64(3), "c", rowchange.OpInsert},
		[3]any{int64(1), "a", rowchange.OpInsert},
		[3]any{int64(2), "b", rowchange.OpInsert},
	)
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if got, want := scanIDs(t, tbl), []int64{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("ids in file order = %v, want %v (sorted by the primary key)", got, want)
	}
}

// A table without a primary key is unsorted, and its rows keep the order they
// were delivered in.
func TestUpstreamKeepsArrivalOrderOnAnUnsortedTable(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := core.TableRef{Target: "orders"}
	if err := s.EnsureTable(ctx, ref, canonicalSchema(), nil, core.CastPolicy{}, dataplane.AppendMode); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	b := wireBatch(t,
		[3]any{int64(3), "c", rowchange.OpInsert},
		[3]any{int64(1), "a", rowchange.OpInsert},
		[3]any{int64(2), "b", rowchange.OpInsert},
	)
	defer b.Release()
	b.Mode = dataplane.AppendMode
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if got, want := scanIDs(t, tbl), []int64{3, 1, 2}; !slices.Equal(got, want) {
		t.Fatalf("ids in file order = %v, want %v (arrival order)", got, want)
	}
}

// v0.7.0 turns Parquet dictionary encoding on by default
// (apache/iceberg-go#931), with a cost-based fallback to plain for a column a
// dictionary does not pay for; v0.6.0 wrote every column plain. The sink sets
// no write.parquet.* property, so its data files change encoding with the
// bump: a low-cardinality column gets a dictionary, a unique key stays plain.
func TestUpstreamWritesDictionaryEncodedDataFilesByDefault(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	statuses := []string{"created", "paid", "shipped", "returned"}
	rows := make([][3]any, 0, 4096)
	for i := range 4096 {
		rows = append(rows, [3]any{int64(i), statuses[i%len(statuses)], rowchange.OpInsert})
	}
	b := wireBatch(t, rows...)
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	paths := dataFilePaths(t, tbl)
	if len(paths) != 1 {
		t.Fatalf("data files = %v, want one", paths)
	}
	rdr, err := file.OpenParquetFile(paths[0], false)
	if err != nil {
		t.Fatalf("open %s: %v", paths[0], err)
	}
	defer func() { _ = rdr.Close() }()
	dict := map[string]bool{}
	rg := rdr.MetaData().RowGroup(0)
	for i := 0; i < rg.NumColumns(); i++ {
		col, err := rg.ColumnChunk(i)
		if err != nil {
			t.Fatalf("column chunk %d: %v", i, err)
		}
		dict[col.PathInSchema().String()] = col.HasDictionaryPage() && slices.Contains(col.Encodings(), parquet.Encodings.RLEDict)
	}
	if !dict["v"] {
		t.Fatalf("column v (4 distinct values in 4096 rows) is not dictionary encoded: %v", dict)
	}
	if dict["id"] {
		t.Fatalf("column id (all distinct) kept a dictionary; upstream's cost fallback should write it plain: %v", dict)
	}
}

// floatKeyBatch is wireBatch for a table whose primary key is a DOUBLE.
func floatKeyBatch(t *testing.T, sch core.Schema, rows ...[3]any) *dataplane.Batch {
	t.Helper()
	data, err := transport.CoreSchemaToArrow(sch)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, data)
	defer bld.Release()
	for _, r := range rows {
		bld.Field(0).(*array.Float64Builder).Append(r[0].(float64))
		bld.Field(1).(*array.StringBuilder).Append(r[1].(string))
		bld.Field(2).(*array.Uint8Builder).Append(uint8(r[2].(rowchange.Op)))
		bld.Field(3).(*array.StringBuilder).Append("p")
		bld.Field(4).AppendNull()
		bld.Field(5).AppendNull()
		bld.Field(6).(*array.BooleanBuilder).Append(false)
		bld.Field(7).(*array.StringBuilder).Append("stream")
	}
	return &dataplane.Batch{Table: "t", Record: bld.NewRecordBatch(), Watermark: []byte("p"), Mode: dataplane.UpsertMode}
}

// v0.7.0 refuses a floating-point column as an equality-delete key
// (apache/iceberg-go#1347); v0.6.0 wrote the delete file. The sink still
// creates and opens a table whose primary key is a FLOAT or DOUBLE, so on
// v0.7.0 such a table fails at its first upsert, with a terminal error,
// instead of at boot (#515 moves the rejection to boot).
func TestUpstreamRejectsAFloatPrimaryKeyAtTheFirstUpsert(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	sch := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindFloat64}},
		{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}}
	ref := core.TableRef{Target: "measures", PrimaryKey: []string{"id"}}
	if err := s.EnsureTable(ctx, ref, sch, nil, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	b := floatKeyBatch(t, sch, [3]any{1.5, "a", rowchange.OpInsert})
	defer b.Release()
	err = w.Commit(ctx, b)
	if err == nil {
		t.Fatal("Commit on a DOUBLE primary key succeeded; v0.7.0 rejects floating-point equality keys")
	}
	if !strings.Contains(err.Error(), "floating-point columns cannot be used as equality delete keys") {
		t.Fatalf("Commit = %v, want upstream's floating-point equality key rejection", err)
	}
	if isRetryableError(err) {
		t.Fatalf("the rejection is classified retryable: %v", err)
	}
}

// The sink creates every table as format-version 2 (createTable), and v0.7.0
// keeps honouring it: deletion vectors and the public position-delete writer
// need version 3 (apache/iceberg-go#2004), so #134 stays out of reach until
// the tables move to it.
func TestUpstreamStillCreatesFormatVersion2Tables(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if v := tbl.Metadata().Version(); v != 2 {
		t.Fatalf("format version = %d, want 2", v)
	}
	if got := tbl.Properties().GetBool(table.ManifestMergeEnabledKey, false); got {
		t.Fatalf("%s is on by default; #463 assumes it is off", table.ManifestMergeEnabledKey)
	}
	if got := tbl.Properties().GetBool(table.MetadataDeleteAfterCommitEnabledKey, false); got {
		t.Fatalf("%s is on by default; #463 assumes it is off", table.MetadataDeleteAfterCommitEnabledKey)
	}
}

// commitOrders commits n single-row upserts, one snapshot pair each, and
// returns the table.
func commitOrders(t *testing.T, s *Sink, ref core.TableRef, n int) *table.Table {
	t.Helper()
	ctx := context.Background()
	w, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	for i := range n {
		b := wireBatch(t, [3]any{int64(i), "x", rowchange.OpInsert})
		if err := w.Commit(ctx, b); err != nil {
			t.Fatalf("Commit %d: %v", i, err)
		}
		b.Release()
	}
	tbl, err := s.cat.LoadTable(ctx, s.ident(ref.Target))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	return tbl
}

// v0.7.0 reworked snapshot expiry (apache/iceberg-go#1736, #1453, #1842):
// standard history.expire.* property names and defaults, a separate age for
// refs, and every ref's head kept. The Maintainer passes retainLast and
// maxAge explicitly, so on a table with only the main branch the outcome is
// the one v0.6.0 gave: past maxAge, exactly the newest retainLast snapshots
// of the branch remain, and the table still reads every row and its position.
func TestUpstreamExpiryKeepsExactlyRetainLastPastMaxAge(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	tbl := commitOrders(t, s, ref, 6)
	before := len(tbl.Metadata().Snapshots())
	if before < 6 {
		t.Fatalf("snapshots before expiry = %d, want at least one per commit", before)
	}
	time.Sleep(20 * time.Millisecond) // every snapshot is now older than maxAge

	cfg := spec.Maintenance{Enabled: true, SnapshotExpiry: &spec.SnapshotExpiryConfig{RetainLast: 2, MaxAge: "1ms"}}
	m := NewMaintainer(s.cat, s.ident("orders"), cfg, nil, nil, nil)
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceSnapshotExpiry}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if got := len(tbl.Metadata().Snapshots()); got != 2 {
		t.Fatalf("snapshots after expiry = %d (of %d), want retainLast = 2", got, before)
	}
	if got := len(scanIDs(t, tbl)); got != 6 {
		t.Fatalf("rows after expiry = %d, want 6", got)
	}
	if pos, err := s.Position(ctx, ref); err != nil || pos != "p" {
		t.Fatalf("position after expiry = %q, %v; want p", pos, err)
	}
}

// maxAge is the operator's safety window: a snapshot younger than it is never
// expired, however small retainLast is.
func TestUpstreamExpiryNeverRemovesASnapshotYoungerThanMaxAge(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	before := len(commitOrders(t, s, ref, 4).Metadata().Snapshots())

	cfg := spec.Maintenance{Enabled: true, SnapshotExpiry: &spec.SnapshotExpiryConfig{RetainLast: 1, MaxAge: "1h"}}
	m := NewMaintainer(s.cat, s.ident("orders"), cfg, nil, nil, nil)
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceSnapshotExpiry}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if got := len(tbl.Metadata().Snapshots()); got != before {
		t.Fatalf("snapshots after expiry = %d, want all %d kept inside maxAge", got, before)
	}
}

// manifestCount counts the manifests of the table's current snapshot.
func manifestCount(t *testing.T, tbl *table.Table) int {
	t.Helper()
	fsys, err := tbl.FS(context.Background())
	if err != nil {
		t.Fatalf("FS: %v", err)
	}
	manifests, err := tbl.CurrentSnapshot().Manifests(fsys)
	if err != nil {
		t.Fatalf("manifests: %v", err)
	}
	return len(manifests)
}

// v0.7.0 adds the maintenance action #463 is missing: Transaction.RewriteManifests
// (apache/iceberg-go#1283). A CDC table gains manifests with every commit; the
// rewrite merges the data manifests into one, in a replace snapshot that
// changes no row. That snapshot takes no caller properties, so it carries no
// cdc.position: the position is still read from the table property, which the
// rewrite leaves alone.
func TestUpstreamRewriteManifestsMergesACDCTablesManifests(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	tbl := commitOrders(t, s, ref, 8)
	before := manifestCount(t, tbl)

	txn := tbl.NewTransaction()
	res, err := txn.RewriteManifests(ctx)
	if err != nil {
		t.Fatalf("RewriteManifests: %v", err)
	}
	if res.IsNoOp() {
		t.Fatalf("RewriteManifests was a no-op (%s) on a table with %d manifests", res.NoOpReason, before)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tbl, err = s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	if after := manifestCount(t, tbl); after >= before {
		t.Fatalf("manifests after rewrite = %d, want fewer than %d", after, before)
	}
	if op := tbl.CurrentSnapshot().Summary.Operation; op != table.OpReplace {
		t.Fatalf("rewrite snapshot operation = %s, want replace", op)
	}
	if pos := tbl.CurrentSnapshot().Summary.Properties[propPosition]; pos != "" {
		t.Fatalf("rewrite snapshot carries %s = %q; upstream takes no snapshot properties here", propPosition, pos)
	}
	if got := len(scanIDs(t, tbl)); got != 8 {
		t.Fatalf("rows after rewrite = %d, want 8", got)
	}
	if pos, err := s.Position(ctx, ref); err != nil || pos != "p" {
		t.Fatalf("position after rewrite = %q, %v; want p", pos, err)
	}
}
