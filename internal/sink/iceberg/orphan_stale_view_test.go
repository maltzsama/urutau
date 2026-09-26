package iceberg

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// appendOrder commits one row to raw.orders.
func appendOrder(t *testing.T, s *Sink, id int64) {
	t.Helper()
	ctx := context.Background()
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	dataSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	if err != nil {
		t.Fatalf("arrow schema: %v", err)
	}
	rec := arrowRecord(t, dataSchema, func(rb *array.RecordBuilder) {
		rb.Field(0).(*array.Int64Builder).Append(id)
		rb.Field(1).(*array.StringBuilder).Append("x")
	})
	defer rec.Release()
	if err := Append(ctx, tbl, rec, iceberg.Properties{"cdc.position": "p"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

// localDir turns a table location into a filesystem path.
func localDir(loc string) string {
	return strings.TrimPrefix(loc, "file://")
}

// backdateFilesSince sets the mtime of every file under dir modified at or
// after since to at.
func backdateFilesSince(t *testing.T, dir string, since, at time.Time) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.ModTime().Before(since) {
			n++
			return os.Chtimes(path, at, at)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("backdate: %v", err)
	}
	return n
}

// Orphan cleanup computes the referenced files from the table version it
// loaded, then deletes every unreferenced file older than olderThan. A
// commit that lands after that load is unreferenced in the cleanup's view;
// if computing the references takes longer than olderThan (a table with many
// manifests, a slow object store), that commit's files already look old when
// the directory is listed, and they were deleted: the table's current
// metadata file with them, leaving the table unloadable. Seen in the #386
// matrix: ~400 commits' metadata deleted, "Location does not exist".
//
// The hook commits right after the cleanup's load and then makes time pass:
// the new files are backdated past olderThan and the clock advanced, which is
// what a slow reference scan does in production.
func TestOrphanCleanupSparesCommitsAfterItsLoad(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)
	appendOrder(t, s, 1)

	const olderThan = time.Hour
	cfg := spec.Maintenance{Enabled: true, OrphanCleanup: &spec.OrphanCleanupConfig{OlderThan: olderThan.String()}}
	m := NewMaintainer(s.cat, s.ident("orders"), cfg, nil, nil, nil)
	clock := time.Now()
	m.now = func() time.Time { return clock }
	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	dir := localDir(tbl.Location())
	m.afterOrphanLoad = func() {
		loaded := time.Now()
		appendOrder(t, s, 2) // a CDC commit lands mid-cleanup
		old := time.Now().Add(-2 * olderThan)
		if n := backdateFilesSince(t, dir, loaded, old); n == 0 {
			t.Fatal("the concurrent commit wrote no file")
		}
		clock = clock.Add(2 * olderThan) // the reference scan took that long
	}

	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceOrphanCleanup}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	after, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("the table no longer loads after orphan cleanup: %v", err)
	}
	if snap := after.CurrentSnapshot(); snap == nil || len(after.Metadata().Snapshots()) < 2 {
		t.Fatalf("the concurrent commit's snapshot is gone")
	}
}

// A run that finishes within its window removes a known orphan and nothing
// else: the table still loads with every commit and row.
func TestOrphanCleanupRemovesAKnownOrphanOnly(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	createOrders(t, s)
	appendOrder(t, s, 1)
	appendOrder(t, s, 2)

	tbl, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	dir := localDir(tbl.Location())
	orphan := filepath.Join(dir, "data", "urutau-orphan.parquet")
	if err := os.WriteFile(orphan, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-3 * time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}

	cfg := spec.Maintenance{Enabled: true, OrphanCleanup: &spec.OrphanCleanupConfig{OlderThan: "1h"}}
	if err := NewMaintainer(s.cat, s.ident("orders"), cfg, nil, nil, nil).RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceOrphanCleanup}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("the orphan survived (stat err %v)", err)
	}
	after, err := s.cat.LoadTable(ctx, s.ident("orders"))
	if err != nil {
		t.Fatalf("LoadTable after cleanup: %v", err)
	}
	if n := len(after.Metadata().Snapshots()); n != 2 {
		t.Fatalf("%d snapshots after cleanup, want 2", n)
	}
	rows := 0
	for _, f := range scanDataFiles(t, after) {
		if _, err := os.Stat(localDir(f)); err != nil {
			t.Fatalf("live file %s removed: %v", f, err)
		}
		rows++
	}
	if rows == 0 {
		t.Fatal("no live data file found")
	}
}

// scanDataFiles lists the data files of the table's current snapshot.
func scanDataFiles(t *testing.T, tbl *table.Table) []string {
	t.Helper()
	fsys, err := tbl.FS(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manifests, err := tbl.CurrentSnapshot().Manifests(fsys)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, m := range manifests {
		for e, err := range m.Entries(fsys, true) {
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, e.DataFile().FilePath())
		}
	}
	return out
}
