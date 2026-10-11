package iceberg

// Format-version 3 and deletion vectors, as apache/iceberg-go v0.7.0 gives
// them to the sink. The sink creates every table as v3 and still writes
// equality deletes; these tests pin that the equality path is unchanged on
// v3, and what the upstream merge-on-read delete does on such a table, which
// is what a positional-delete mode would build on.

import (
	"context"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// deleteCounts is what a scan plan attaches to the table's data files.
type deleteCounts struct {
	dataFiles, equality, vectors int
	// maxVectorsPerFile is the largest number of deletion vectors any single
	// data file carries; the spec allows one.
	maxVectorsPerFile int
}

func planDeletes(t *testing.T, tbl *table.Table) deleteCounts {
	t.Helper()
	tasks, err := tbl.Scan().PlanFiles(context.Background())
	if err != nil {
		t.Fatalf("PlanFiles: %v", err)
	}
	var c deleteCounts
	eq, dv := map[string]bool{}, map[string]bool{}
	for _, task := range tasks {
		c.dataFiles++
		for _, f := range task.EqualityDeleteFiles {
			eq[f.FilePath()] = true
		}
		for _, f := range task.DeletionVectorFiles {
			dv[f.FilePath()] = true
		}
		c.maxVectorsPerFile = max(c.maxVectorsPerFile, len(task.DeletionVectorFiles))
	}
	c.equality, c.vectors = len(eq), len(dv)
	return c
}

func sortedIDs(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()
	ids := scanIDs(t, tbl)
	slices.Sort(ids)
	return ids
}

func reload(t *testing.T, s *Sink, ref core.TableRef) *table.Table {
	t.Helper()
	tbl, err := s.cat.LoadTable(context.Background(), s.ident(ref.Target))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	return tbl
}

// deleteByID runs upstream's row-level delete for one id in merge-on-read mode.
func deleteByID(t *testing.T, s *Sink, ref core.TableRef, id int64) {
	t.Helper()
	ctx := context.Background()
	txn := reload(t, s, ref).NewTransaction()
	if err := txn.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeMergeOnRead}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	if err := txn.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), id), nil); err != nil {
		t.Fatalf("Delete id=%d: %v", id, err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatalf("commit delete id=%d: %v", id, err)
	}
}

// oneFileOfOrders commits ids 1..n as a single batch, so they share one data file.
func oneFileOfOrders(t *testing.T, s *Sink, ref core.TableRef, n int) sink.TableWriter {
	t.Helper()
	w, err := s.Writer(context.Background(), ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	commitInserts(t, w, 1, n)
	return w
}

// commitInserts commits ids from..from+n-1 as one batch (one data file).
func commitInserts(t *testing.T, w sink.TableWriter, from, n int) {
	t.Helper()
	rows := make([][3]any, n)
	for i := range rows {
		rows[i] = [3]any{int64(from + i), "x", rowchange.OpInsert}
	}
	b := wireBatch(t, rows...)
	defer b.Release()
	if err := w.Commit(context.Background(), b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestTablesAreCreatedAsFormatVersion3(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)
	if v := reload(t, s, ref).Metadata().Version(); v != 3 {
		t.Fatalf("format version = %d, want 3", v)
	}
}

// The sink's own upsert path is unchanged on v3: an update and a delete are
// equality deletes, and a scan returns the surviving rows.
func TestEqualityDeleteUpsertWorksOnFormatVersion3(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w := oneFileOfOrders(t, s, ref, 3)

	b := wireBatch(t, [3]any{int64(1), "y", rowchange.OpUpdate}, [3]any{int64(2), nil, rowchange.OpDelete})
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit update+delete: %v", err)
	}

	tbl := reload(t, s, ref)
	if got, want := sortedIDs(t, tbl), []int64{1, 3}; !slices.Equal(got, want) {
		t.Fatalf("ids = %v, want %v", got, want)
	}
	c := planDeletes(t, tbl)
	if c.equality == 0 || c.vectors != 0 {
		t.Errorf("plan has %d equality delete files and %d deletion vectors, want equality deletes only", c.equality, c.vectors)
	}
}

// Upstream's merge-on-read delete on a v3 table writes a deletion vector, and
// a second delete on the same data file replaces it with a merged one: the
// file never carries two.
func TestMergeOnReadDeleteKeepsOneDeletionVectorPerDataFile(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)
	oneFileOfOrders(t, s, ref, 4)

	deleteByID(t, s, ref, 1)
	tbl := reload(t, s, ref)
	if got, want := sortedIDs(t, tbl), []int64{2, 3, 4}; !slices.Equal(got, want) {
		t.Fatalf("after the first delete ids = %v, want %v", got, want)
	}
	if c := planDeletes(t, tbl); c.vectors != 1 || c.equality != 0 {
		t.Fatalf("after the first delete: %d deletion vectors, %d equality delete files; want 1 and 0", c.vectors, c.equality)
	}

	deleteByID(t, s, ref, 2)
	tbl = reload(t, s, ref)
	if got, want := sortedIDs(t, tbl), []int64{3, 4}; !slices.Equal(got, want) {
		t.Fatalf("after the second delete ids = %v, want %v", got, want)
	}
	if c := planDeletes(t, tbl); c.maxVectorsPerFile != 1 || c.vectors != 1 {
		t.Fatalf("after the second delete: %d deletion vectors, up to %d on one data file; want one merged vector", c.vectors, c.maxVectorsPerFile)
	}
}

// A deletion vector and the sink's equality deletes coexist on one table.
func TestDeletionVectorsAndEqualityDeletesCoexist(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w := oneFileOfOrders(t, s, ref, 4)

	deleteByID(t, s, ref, 1)
	b := wireBatch(t, [3]any{int64(2), nil, rowchange.OpDelete}, [3]any{int64(3), "y", rowchange.OpUpdate})
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit after a deletion vector: %v", err)
	}

	tbl := reload(t, s, ref)
	if got, want := sortedIDs(t, tbl), []int64{3, 4}; !slices.Equal(got, want) {
		t.Fatalf("ids = %v, want %v", got, want)
	}
	if c := planDeletes(t, tbl); c.vectors == 0 || c.equality == 0 {
		t.Errorf("plan has %d deletion vectors and %d equality delete files, want both kinds", c.vectors, c.equality)
	}
}

// Compaction applies the deletion vectors of the files it rewrites and leaves
// none behind: no stale position stays reachable.
func TestCompactionAppliesAndRemovesDeletionVectors(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := createOrders(t, s)
	w := oneFileOfOrders(t, s, ref, 3) // ids 1..3 in one file
	commitInserts(t, w, 4, 3)          // ids 4..6 in another
	deleteByID(t, s, ref, 1)
	deleteByID(t, s, ref, 4)
	if c := planDeletes(t, reload(t, s, ref)); c.vectors != 2 || c.dataFiles != 2 {
		t.Fatalf("setup: want 2 data files with a deletion vector each, got %+v", c)
	}

	cfg := fullMaintenance()
	cfg.Compaction = &spec.CompactionConfig{MinInputFiles: 2}
	m := NewMaintainer(s.cat, s.ident(ref.Target), cfg, nil, func() string { return "p" }, &recordingMetrics{})
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceCompaction}); err != nil {
		t.Fatalf("RunOnce(compaction): %v", err)
	}

	after := reload(t, s, ref)
	if got, want := sortedIDs(t, after), []int64{2, 3, 5, 6}; !slices.Equal(got, want) {
		t.Fatalf("ids after compaction = %v, want %v", got, want)
	}
	if c := planDeletes(t, after); c.vectors != 0 {
		t.Errorf("%d deletion vectors survive compaction, want 0 (%+v)", c.vectors, c)
	}
}

// A table an older release created as format-version 2 is upgraded in place
// the next time the pipeline ensures it: its rows and its equality deletes
// stay readable, and the sink keeps writing to it.
func TestEnsureTableUpgradesAFormatVersion2Table(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref := core.TableRef{Target: "orders", PrimaryKey: []string{"id"}}
	is, err := FromCanonical(canonicalSchema())
	if err != nil {
		t.Fatalf("FromCanonical: %v", err)
	}
	if _, err := s.cat.CreateTable(ctx, s.ident(ref.Target), is,
		catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: "2"})); err != nil {
		t.Fatalf("CreateTable v2: %v", err)
	}
	w := oneFileOfOrders(t, s, ref, 3)
	b := wireBatch(t, [3]any{int64(2), nil, rowchange.OpDelete})
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit delete on v2: %v", err)
	}
	b.Release()
	if v := reload(t, s, ref).Metadata().Version(); v != 2 {
		t.Fatalf("setup: format version = %d, want 2", v)
	}

	if err := s.EnsureTable(ctx, ref, canonicalSchema(), nil, core.CastPolicy{}, dataplane.UpsertMode); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}

	tbl := reload(t, s, ref)
	if v := tbl.Metadata().Version(); v != 3 {
		t.Fatalf("format version after EnsureTable = %d, want 3", v)
	}
	if got, want := sortedIDs(t, tbl), []int64{1, 3}; !slices.Equal(got, want) {
		t.Fatalf("ids after the upgrade = %v, want %v", got, want)
	}

	w2, err := s.Writer(ctx, ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	b = wireBatch(t, [3]any{int64(1), "y", rowchange.OpUpdate}, [3]any{int64(4), "x", rowchange.OpInsert})
	defer b.Release()
	if err := w2.Commit(ctx, b); err != nil {
		t.Fatalf("Commit after the upgrade: %v", err)
	}
	if got, want := sortedIDs(t, reload(t, s, ref)), []int64{1, 3, 4}; !slices.Equal(got, want) {
		t.Fatalf("ids after a commit on the upgraded table = %v, want %v", got, want)
	}
}
