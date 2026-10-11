package iceberg

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// scanValues reads id → v for every row the table's current snapshot shows.
// A key seen twice fails the test: an upsert table never shows two versions.
func scanValues(t *testing.T, tbl *table.Table) map[int64]string {
	t.Helper()
	_, recs, err := tbl.Scan(table.WithSelectedFields("id", "v")).ToArrowRecords(context.Background())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	out := map[int64]string{}
	for rec, err := range recs {
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids := rec.Column(0).(*array.Int64)
		vs := rec.Column(1).(*array.String)
		for i := range int(rec.NumRows()) {
			if _, dup := out[ids.Value(i)]; dup {
				t.Fatalf("id %d is visible twice", ids.Value(i))
			}
			out[ids.Value(i)] = vs.Value(i)
		}
		rec.Release()
	}
	return out
}

// positionalWriter returns a writer on a fresh orders table in positional mode.
func positionalWriter(t *testing.T, s *Sink) (core.TableRef, *TableWriter) {
	t.Helper()
	ref := createOrders(t, s)
	w, err := s.Writer(context.Background(), ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	tw := w.(*TableWriter)
	tw.positional = true
	tw.backoff = 0
	return ref, tw
}

func commitRows(t *testing.T, w sink.TableWriter, rows ...[3]any) {
	t.Helper()
	b := wireBatch(t, rows...)
	defer b.Release()
	if err := w.Commit(context.Background(), b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func expectValues(t *testing.T, s *Sink, ref core.TableRef, want map[int64]string) *table.Table {
	t.Helper()
	tbl := reload(t, s, ref)
	if got := scanValues(t, tbl); !maps.Equal(got, want) {
		t.Fatalf("table = %v, want %v", got, want)
	}
	return tbl
}

// An update and a delete in positional mode remove the old row versions with
// deletion vectors and write no equality delete.
func TestPositionalUpsertWritesDeletionVectors(t *testing.T) {
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 4)
	if c := planDeletes(t, reload(t, s, ref)); c.vectors != 0 || c.equality != 0 {
		t.Fatalf("inserts of new keys wrote deletes: %+v", c)
	}

	commitRows(t, w, [3]any{int64(2), "y", rowchange.OpUpdate}, [3]any{int64(3), nil, rowchange.OpDelete})

	tbl := expectValues(t, s, ref, map[int64]string{1: "x", 2: "y", 4: "x"})
	if c := planDeletes(t, tbl); c.equality != 0 || c.vectors == 0 {
		t.Fatalf("plan has %d equality delete files and %d deletion vectors, want deletion vectors only", c.equality, c.vectors)
	}
}

// A second change to rows of the same data file replaces its deletion vector
// with a merged one, and a key updated twice keeps exactly its last version.
func TestPositionalUpsertMergesTheVectorOfAFile(t *testing.T) {
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 4)
	commitRows(t, w, [3]any{int64(1), "a", rowchange.OpUpdate})
	commitRows(t, w, [3]any{int64(2), "b", rowchange.OpUpdate})
	commitRows(t, w, [3]any{int64(1), "c", rowchange.OpUpdate})

	tbl := expectValues(t, s, ref, map[int64]string{1: "c", 2: "b", 3: "x", 4: "x"})
	if c := planDeletes(t, tbl); c.maxVectorsPerFile != 1 || c.equality != 0 {
		t.Fatalf("plan = %+v, want at most one deletion vector per data file and no equality delete", c)
	}
}

// A batch that only deletes commits its deletion vectors and its position.
func TestPositionalDeleteOnlyBatch(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 3)

	b := wireBatch(t, [3]any{int64(1), nil, rowchange.OpDelete}, [3]any{int64(3), nil, rowchange.OpDelete})
	b.Watermark = []byte("pos-2")
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tbl := expectValues(t, s, ref, map[int64]string{2: "x"})
	if got := committedPosition(tbl); got != "pos-2" {
		t.Errorf("committed position = %q, want pos-2", got)
	}
	if c := planDeletes(t, tbl); c.equality != 0 || c.vectors != 1 {
		t.Fatalf("plan = %+v, want one deletion vector and no equality delete", c)
	}
}

// A delete of a key the table does not hold commits nothing wrong: no vector,
// the rows untouched, the position advanced.
func TestPositionalDeleteOfAnAbsentKey(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 2)

	b := wireBatch(t, [3]any{int64(99), nil, rowchange.OpDelete})
	b.Watermark = []byte("pos-9")
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	tbl := expectValues(t, s, ref, map[int64]string{1: "x", 2: "x"})
	if got := committedPosition(tbl); got != "pos-9" {
		t.Errorf("committed position = %q, want pos-9", got)
	}
	if c := planDeletes(t, tbl); c.vectors != 0 {
		t.Fatalf("a delete of an absent key wrote %d deletion vectors", c.vectors)
	}
}

// A table that already carries equality deletes keeps them valid when the
// mode is switched on: both kinds apply, and new changes write vectors.
func TestPositionalModeOnATableWithEqualityDeletes(t *testing.T) {
	s := hadoopSink(t)
	ref := createOrders(t, s)
	eq := oneFileOfOrders(t, s, ref, 4)
	commitRows(t, eq, [3]any{int64(1), "a", rowchange.OpUpdate}) // equality delete of the old id=1

	w, err := s.Writer(context.Background(), ref, core.CastPolicy{}, nil)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	w.(*TableWriter).positional = true
	commitRows(t, w, [3]any{int64(1), "b", rowchange.OpUpdate}, [3]any{int64(2), nil, rowchange.OpDelete})

	expectValues(t, s, ref, map[int64]string{1: "b", 3: "x", 4: "x"})
}

// A commit the catalog applied but answered with an unknown outcome ends as
// one commit in positional mode too, although its vectors are rewritten on
// the retry.
func TestPositionalCommitWithUnknownOutcomeIsAppliedOnce(t *testing.T) {
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 3)

	lossy := &lostResponseCatalog{Catalog: s.cat, lose: 1}
	s.cat, w.cat = lossy, lossy
	commitRows(t, w, [3]any{int64(2), "y", rowchange.OpUpdate}, [3]any{int64(4), "x", rowchange.OpInsert})
	if lossy.applied != 1 {
		t.Errorf("catalog applied %d commits for one batch, want 1", lossy.applied)
	}
	expectValues(t, s, ref, map[int64]string{1: "x", 2: "y", 3: "x", 4: "x"})
}

// Compaction rewrites the data files the vectors point into. The next
// positional commit resolves positions against the rewritten files.
func TestPositionalCommitAfterCompaction(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 3)
	commitInserts(t, w, 4, 3)
	commitRows(t, w, [3]any{int64(2), "y", rowchange.OpUpdate}, [3]any{int64(5), nil, rowchange.OpDelete})

	cfg := fullMaintenance()
	cfg.Compaction = &spec.CompactionConfig{MinInputFiles: 2}
	m := NewMaintainer(s.cat, s.ident(ref.Target), cfg, nil, func() string { return "p" }, &recordingMetrics{})
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceCompaction}); err != nil {
		t.Fatalf("RunOnce(compaction): %v", err)
	}
	expectValues(t, s, ref, map[int64]string{1: "x", 2: "y", 3: "x", 4: "x", 6: "x"})

	commitRows(t, w, [3]any{int64(2), "z", rowchange.OpUpdate}, [3]any{int64(6), nil, rowchange.OpDelete})
	expectValues(t, s, ref, map[int64]string{1: "x", 2: "z", 3: "x", 4: "x"})
}

// Append mode never removes a row, so the mode changes nothing there.
func TestPositionalModeLeavesAppendTablesAlone(t *testing.T) {
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
	w.(*TableWriter).positional = true
	b := wireBatch(t, [3]any{int64(1), "x", rowchange.OpInsert}, [3]any{int64(1), "y", rowchange.OpUpdate})
	b.Mode = dataplane.AppendMode
	defer b.Release()
	if err := w.Commit(ctx, b); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if ids := scanIDs(t, reload(t, s, ref)); len(ids) != 2 {
		t.Fatalf("append table holds %d rows, want 2", len(ids))
	}
}

// The scan plan skips data files whose key range cannot hold a key of the
// batch: resolving one key reads one file, not the table.
func TestPositionalCommitReadsOnlyFilesThatCanHoldTheKeys(t *testing.T) {
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 3)   // file A: 1..3
	commitInserts(t, w, 100, 3) // file B: 100..102
	commitInserts(t, w, 200, 3) // file C: 200..202

	before := w.filesMatched
	commitRows(t, w, [3]any{int64(101), "y", rowchange.OpUpdate})
	if got := w.filesMatched - before; got != 1 {
		t.Errorf("an update of one key read %d data files, want 1", got)
	}
	expectValues(t, s, ref, map[int64]string{1: "x", 2: "x", 3: "x", 100: "x", 101: "y", 102: "x", 200: "x", 201: "x", 202: "x"})
}

// The same key must encode to the same bytes whether it comes from the batch
// or from a data file, whatever integer width or string flavour each side
// uses, and two different tuples must never collide.
func TestRowKeyEncoding(t *testing.T) {
	mem := memory.DefaultAllocator
	i64 := array.NewInt64Builder(mem)
	i64.AppendValues([]int64{12, 1}, nil)
	i32 := array.NewInt32Builder(mem)
	i32.AppendValues([]int32{12, 1}, nil)
	str := array.NewStringBuilder(mem)
	str.AppendValues([]string{"3", "23"}, nil)
	large := array.NewLargeStringBuilder(mem)
	large.AppendValues([]string{"3", "23"}, nil)
	a64, a32, aStr, aLarge := i64.NewArray(), i32.NewArray(), str.NewArray(), large.NewArray()
	defer a64.Release()
	defer a32.Release()
	defer aStr.Release()
	defer aLarge.Release()

	key := func(row int, cols ...arrow.Array) string {
		t.Helper()
		b, err := appendRowKey(nil, cols, row)
		if err != nil {
			t.Fatalf("appendRowKey: %v", err)
		}
		return string(b)
	}
	if key(0, a64, aStr) != key(0, a32, aLarge) {
		t.Error("the same tuple encodes differently across integer widths and string flavours")
	}
	// (12, "3") and (1, "23") concatenate to the same text; they must differ.
	if key(0, a64, aStr) == key(1, a64, aStr) {
		t.Error("two different tuples encode to the same key")
	}

	nulls := array.NewInt64Builder(mem)
	nulls.AppendNull()
	aNull := nulls.NewArray()
	defer aNull.Release()
	if _, err := appendRowKey(nil, []arrow.Array{aNull}, 0); err == nil {
		t.Error("a null key encoded without error")
	}
	floats := array.NewFloat64Builder(mem)
	floats.Append(1.5)
	aFloat := floats.NewArray()
	defer aFloat.Release()
	if _, err := appendRowKey(nil, []arrow.Array{aFloat}, 0); err == nil {
		t.Error("a floating-point key encoded without error")
	}
}

// sink.deleteMode reaches the writer through the sink's options.
func TestSinkDeleteModeSelectsPositionalWriters(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want bool
	}{{"", false}, {"equality", false}, {"positional", true}} {
		s := hadoopSink(t)
		s.positional = tc.mode == string(spec.DeleteModePositional)
		ref := createOrders(t, s)
		w, err := s.Writer(context.Background(), ref, core.CastPolicy{}, nil)
		if err != nil {
			t.Fatalf("Writer: %v", err)
		}
		if got := w.(*TableWriter).positional; got != tc.want {
			t.Errorf("deleteMode %q: writer positional = %v, want %v", tc.mode, got, tc.want)
		}
	}
}

// Snapshot expiry and orphan cleanup run over a table with deletion vectors,
// including vectors an earlier commit superseded. Whatever they delete, the
// live vectors must survive: the table reads the same before and after, and
// the next positional commit still works.
func TestMaintenanceKeepsLiveDeletionVectors(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 4)
	commitRows(t, w, [3]any{int64(1), "a", rowchange.OpUpdate}) // a vector on the first file
	commitRows(t, w, [3]any{int64(2), nil, rowchange.OpDelete}) // superseded by a merged one
	commitRows(t, w, [3]any{int64(1), "b", rowchange.OpUpdate}) // a vector on the file of "a"
	want := map[int64]string{1: "b", 3: "x", 4: "x"}
	tbl := expectValues(t, s, ref, want)

	// Age every file past the safety windows, as a long-lived table's would be.
	old := time.Now().Add(-3 * time.Hour)
	err := filepath.WalkDir(localDir(tbl.Location()), func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		return os.Chtimes(path, old, old)
	})
	if err != nil {
		t.Fatalf("backdate: %v", err)
	}

	cfg := spec.Maintenance{
		Enabled:        true,
		SnapshotExpiry: &spec.SnapshotExpiryConfig{RetainLast: 1, MaxAge: "1ms"},
		OrphanCleanup:  &spec.OrphanCleanupConfig{OlderThan: "1h"},
	}
	m := NewMaintainer(s.cat, s.ident(ref.Target), cfg, nil, func() string { return "p" }, &recordingMetrics{})
	if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceSnapshotExpiry, sink.MaintenanceOrphanCleanup}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	after := expectValues(t, s, ref, want)
	if c := planDeletes(t, after); c.vectors == 0 {
		t.Fatalf("no deletion vector left after maintenance: %+v", c)
	}
	commitRows(t, w, [3]any{int64(3), "c", rowchange.OpUpdate})
	expectValues(t, s, ref, map[int64]string{1: "b", 3: "c", 4: "x"})
}

// interleavingCatalog runs a step right before the first commit it sees, as a
// concurrent writer that wins the race would. The commit that lost then fails
// its requirements; the hadoop catalog of the tests reports that as a plain
// error, so it is translated to what the REST catalog the pipelines use
// returns for it (HTTP 409, rest.ErrCommitFailed).
type interleavingCatalog struct {
	catalog.Catalog
	before func()
	done   bool
}

func (c *interleavingCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, err := c.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	return table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, c), nil
}

func (c *interleavingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if !c.done {
		c.done = true
		c.before()
	}
	meta, loc, err := c.Catalog.CommitTable(ctx, ident, reqs, updates)
	if err != nil && strings.Contains(err.Error(), "requirement failed") {
		return nil, "", fmt.Errorf("%w: %v", rest.ErrCommitFailed, err)
	}
	return meta, loc, err
}

// A compaction that lands between the moment a positional commit resolved its
// positions and the moment it commits has rewritten the files those positions
// point into. The commit must not publish them: it loses the race, resolves
// again against the rewritten files, and the table ends exact.
func TestPositionalCommitLosesTheRaceToACompaction(t *testing.T) {
	ctx := context.Background()
	s := hadoopSink(t)
	ref, w := positionalWriter(t, s)
	commitInserts(t, w, 1, 3)
	commitInserts(t, w, 4, 3)
	commitRows(t, w, [3]any{int64(2), "y", rowchange.OpUpdate}) // a vector on the first file

	inner := s.cat
	compact := func() {
		cfg := fullMaintenance()
		cfg.Compaction = &spec.CompactionConfig{MinInputFiles: 2}
		m := NewMaintainer(inner, s.ident(ref.Target), cfg, nil, func() string { return "p" }, &recordingMetrics{})
		if err := m.RunOnce(ctx, []sink.MaintenanceOp{sink.MaintenanceCompaction}); err != nil {
			t.Errorf("concurrent compaction: %v", err)
		}
	}
	racing := &interleavingCatalog{Catalog: inner, before: compact}
	w.cat = racing

	// Touches a row with an existing vector (a superseding commit) and one
	// without (a plain new vector), and deletes a third.
	commitRows(t, w,
		[3]any{int64(1), "z", rowchange.OpUpdate},
		[3]any{int64(5), "z", rowchange.OpUpdate},
		[3]any{int64(6), nil, rowchange.OpDelete})
	if !racing.done {
		t.Fatal("the compaction never interleaved")
	}

	s.cat = inner
	expectValues(t, s, ref, map[int64]string{1: "z", 2: "y", 3: "x", 4: "x", 5: "z"})
}
