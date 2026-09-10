package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
)

// snapChange builds a snapshot row (chunk SELECT result).
func snapChange(table string, id int64, v, pos string) rowchange.Change {
	return rowchange.Change{
		Op: rowchange.OpInsert, Table: table, Key: []any{id}, Position: pos,
		After:    map[string]any{"id": id, "v": v},
		Snapshot: true,
	}
}

// runSnapshotWorker boots a worker with the given snapshot state and feeds
// the changes, returning the committed batches.
func runSnapshotWorker(t *testing.T, state string, pending []uint32, changes []rowchange.Change) ([]rowchange.Batch, error) {
	t.Helper()
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.UpsertMode)
	w.SetSnapshotState("t", state, pending)
	ing := make(chan Ingest, len(changes)+1)
	for _, in := range ingestFromChanges(t, changes) {
		ing <- in
	}
	close(ing)
	err := w.Run(context.Background(), ing)
	return fc.batches, err
}

// The partitioned snapshot flush must advance the position only on the last
// commit: when live events follow the snapshot rows, the append batch carries
// no position — a crash between the two commits would otherwise resume past
// the never-committed live events.
func TestSnapshotPartitionPositionOnlyOnLastCommit(t *testing.T) {
	batches, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0, 1}, []rowchange.Change{
		snapChange("t", 1, "s1", "low"),
		chg("t", rowchange.OpInsert, 2, "live", "p2"),
		chg("t", rowchange.OpInsert, 3, "live", "p3"),
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want append + upsert", len(batches))
	}
	ab, ub := batches[0], batches[1]
	if ab.Mode != rowchange.AppendMode {
		t.Fatalf("batch 0 mode = %v, want append", ab.Mode)
	}
	if ab.Position != "" {
		t.Fatalf("append batch position = %q, want empty (upsert batch commits last)", ab.Position)
	}
	if len(batchUpserts(ab)) != 1 || batchUpserts(ab)[0].Key[0] != int64(1) {
		t.Fatalf("append batch rows = %+v, want snapshot id=1 only", batchUpserts(ab))
	}
	if ub.Mode != rowchange.UpsertMode || ub.Position != "p3" {
		t.Fatalf("upsert batch = mode %v pos %q, want upsert at p3", ub.Mode, ub.Position)
	}
}

// With no live events in the buffer, the append batch IS the last commit and
// must carry the position.
func TestSnapshotAppendOnlyCarriesPosition(t *testing.T) {
	batches, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0}, []rowchange.Change{
		snapChange("t", 1, "s1", "low"),
		snapChange("t", 2, "s2", "low"),
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("batches = %d, want single append", len(batches))
	}
	if batches[0].Position != "low" {
		t.Fatalf("position = %q, want low", batches[0].Position)
	}
}

// A live event marks its PK as touched: the snapshot row for that key must
// take the upsert path (equality delete), never a pure append — otherwise a
// key updated before its chunk was read ends up duplicated.
func TestBootstrapGuardTracksLiveKeys(t *testing.T) {
	batches, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0}, []rowchange.Change{
		chg("t", rowchange.OpUpdate, 5, "live5", "p1"), // live: touches key 5
		snapChange("t", 5, "snap5", "low"),             // snapshot re-reads key 5
		snapChange("t", 6, "snap6", "low"),             // key 6 was never touched
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want append + upsert", len(batches))
	}
	ab, ub := batches[0], batches[1]
	if len(batchUpserts(ab)) != 1 || batchUpserts(ab)[0].Key[0] != int64(6) {
		t.Fatalf("append batch = %+v, want only the untouched key 6", batchUpserts(ab))
	}
	// Key 5 must take the upsert path (collapse keeps the last version —
	// the snapshot re-read the updated row — and it carries an equality
	// delete). The critical assertion is that it is NOT in the append
	// batch above.
	found := false
	for _, u := range batchUpserts(ub) {
		if u.Key[0] == int64(5) {
			found = true
		}
	}
	if !found {
		t.Fatal("key 5 vanished: neither append nor upsert carried it")
	}
}

// Completing a snapshot releases the filter; a second snapshot run in the
// same process must not panic on a nil guard.
func TestSetSnapshotStateRecreatesGuard(t *testing.T) {
	if _, err := runSnapshotWorker(t, string(snapshot.StateComplete), nil, []rowchange.Change{
		snapChange("t", 1, "s1", "low"),
		chg("t", rowchange.OpInsert, 2, "live", "p1"),
	}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if _, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0}, []rowchange.Change{
		snapChange("t", 1, "s1", "low"),
	}); err != nil {
		t.Fatalf("second run after complete: %v", err)
	}
}

// An unknown column in the data is terminal: the change is refused, the
// drift callback fires once, and nothing with a divergent schema is written.
func TestSchemaDriftIsTerminal(t *testing.T) {
	var drifts []SchemaDrift
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.UpsertMode)
	w.SetKnownSchema("t", core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}})
	w.OnSchemaDrift(func(d SchemaDrift) { drifts = append(drifts, d) })

	err := w.Run(context.Background(), chanIngest(t, ingestFromChanges(t, []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "t", Key: []any{1},
			After: map[string]any{"id": int64(1), "extra": "x"}, Position: "p1"},
		{Op: rowchange.OpInsert, Table: "t", Key: []any{2},
			After: map[string]any{"id": int64(2), "extra": "y", "other": "z"}, Position: "p2"},
	})))
	if err == nil || !strings.Contains(err.Error(), "schema drift") {
		t.Fatalf("err = %v, want terminal schema-drift error", err)
	}
	if len(drifts) != 1 || drifts[0].Column != "extra" {
		t.Fatalf("drifts = %+v, want one report for %q", drifts, "extra")
	}
	if len(fc.batches) != 0 {
		t.Fatalf("batches = %d, want none — divergent rows must never be written", len(fc.batches))
	}
}

var _ sink.TableWriter = (*fakeCommitter)(nil)

// A resumed snapshot disables the pure-append path: the bloom guard was
// recreated empty, so it cannot know which keys live events touched before
// the crash. Snapshot rows must take the upsert path (equality delete) or
// pending chunks would duplicate already-committed live rows.
func TestResumedSnapshotUsesUpsertPath(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.UpsertMode)
	w.SetSnapshotState("t", string(snapshot.StateInProgress), []uint32{2})
	w.MarkSnapshotResumed("t")

	ingest := make(chan rowchange.Change, 4)
	ingest <- snapChange("t", 1, "s1", "low")
	ingest <- snapChange("t", 2, "s2", "low")
	close(ingest)
	if err := w.Run(context.Background(), IngestFromChanges(context.Background(), ingest, testSchema())); err != nil {
		t.Fatalf("run: %v", err)
	}

	// A single upsert batch (no append-only split), carrying both rows
	// through the delete-then-append path.
	if len(fc.batches) != 1 {
		t.Fatalf("batches = %d, want 1 upsert batch (no append split)", len(fc.batches))
	}
	b := fc.batches[0]
	if b.Mode != rowchange.UpsertMode {
		t.Fatalf("mode = %v, want upsert on a resumed snapshot", b.Mode)
	}
	if len(batchUpserts(b)) != 2 || b.Position != "low" {
		t.Fatalf("upserts = %d pos %q, want 2 rows at low", len(batchUpserts(b)), b.Position)
	}
}

// 28.2: append-only delete handling. record with a before image appends the
// row; a delete with no before image (Kafka tombstone) is dropped and
// counted, never written as an all-null row; onDelete: skip drops deletes
// even when a before image exists.
func TestAppendModeDeleteHandling(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.AppendMode)
	var dropped []string
	w.OnDroppedDelete(func(table, pos string) { dropped = append(dropped, pos) })

	ingest := make(chan rowchange.Change, 8)
	ingest <- rowchange.Change{Op: rowchange.OpInsert, Table: "t", Key: []any{1},
		After: map[string]any{"id": int64(1), "v": "a"}, Position: "p1"}
	// record: has a before image -> row appended.
	ingest <- rowchange.Change{Op: rowchange.OpDelete, Table: "t", Key: []any{2},
		Before: map[string]any{"id": int64(2), "v": "gone"}, Position: "p2"}
	// record: NO before image -> dropped, counted, never an all-null row.
	ingest <- rowchange.Change{Op: rowchange.OpDelete, Table: "t", Key: []any{3},
		Position: "p3"}
	close(ingest)
	if err := w.Run(context.Background(), IngestFromChanges(context.Background(), ingest, testSchema())); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(fc.batches))
	}
	b := fc.batches[0]
	// AppendMode: every surviving change becomes a row — the insert plus
	// the before-image delete rewritten as a row carrying op=delete.
	if len(b.Changes) != 2 {
		t.Fatalf("rows = %d, want 2 (insert + recorded delete) — no all-null row", len(b.Changes))
	}
	if len(dropped) != 1 || dropped[0] != "p3" {
		t.Fatalf("dropped = %v, want [p3]", dropped)
	}
	if w.DroppedDeletes("t") != 1 {
		t.Fatalf("DroppedDeletes = %d, want 1", w.DroppedDeletes("t"))
	}
}

func TestAppendModeOnDeleteSkip(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "t", fc, dataplane.AppendMode)
	w.SetDropDeletes("t", true)

	ingest := make(chan rowchange.Change, 4)
	ingest <- rowchange.Change{Op: rowchange.OpInsert, Table: "t", Key: []any{1},
		After: map[string]any{"id": int64(1), "v": "a"}, Position: "p1"}
	ingest <- rowchange.Change{Op: rowchange.OpDelete, Table: "t", Key: []any{2},
		Before: map[string]any{"id": int64(2), "v": "gone"}, Position: "p2"}
	close(ingest)
	if err := w.Run(context.Background(), IngestFromChanges(context.Background(), ingest, testSchema())); err != nil {
		t.Fatalf("run: %v", err)
	}
	b := fc.batches[0]
	if len(batchUpserts(b)) != 1 {
		t.Fatalf("upserts = %d, want 1 — skip drops even a before-image delete", len(batchUpserts(b)))
	}
	if w.DroppedDeletes("t") != 1 {
		t.Fatalf("DroppedDeletes = %d, want 1", w.DroppedDeletes("t"))
	}
}

// Nested schema drift (a field added inside a struct column) is detected at
// the SOURCE boundary, where the native row shape exists — encoding against
// the canonical schema would silently drop the new field before the worker
// sees it. Coverage lives in sourcepull/drift_test.go (driftAgainst /
// driftNested). The worker's own schemaDrift remains the top-level backstop
// for schema-less producers.

// TestWorkerRegimeBoundaryRowColumnar: the snapshot partition path (row-based)
// and the columnar collapse must agree on the same data — the boundary
// between the two regimes must not diverge. In snapshot mode: an untouched
// PK's snapshot row is pure-appended, a touched PK's live version wins its
// snapshot row, a live insert survives, and a live delete removes its key —
// all in ONE flush that crosses both regimes.
func TestWorkerRegimeBoundaryRowColumnar(t *testing.T) {
	batches, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0}, []rowchange.Change{
		snapChange("t", 1, "s1", "low"),               // touched by the live update below
		snapChange("t", 2, "s2", "low"),               // untouched -> pure append
		chg("t", rowchange.OpUpdate, 1, "live", "p2"), // touches PK 1 -> rest
		chg("t", rowchange.OpInsert, 3, "new", "p3"),  // live -> rest
		chg("t", rowchange.OpDelete, 4, "", "p4"),     // live delete -> rest
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	// Expect two commits: the append batch (untouched PK 2) then the upsert
	// batch (rest: PK 1 live wins, PK 3 insert, PK 4 delete).
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want append + upsert", len(batches))
	}
	ab, ub := batches[0], batches[1]
	if ab.Mode != rowchange.AppendMode {
		t.Fatalf("batch 0 mode = %v, want append", ab.Mode)
	}
	if len(batchUpserts(ab)) != 1 || batchUpserts(ab)[0].Key[0] != int64(2) || batchUpserts(ab)[0].After["v"] != "s2" {
		t.Fatalf("append batch = %+v, want [2:s2]", batchUpserts(ab))
	}

	if ub.Mode != rowchange.UpsertMode {
		t.Fatalf("batch 1 mode = %v, want upsert", ub.Mode)
	}
	got := map[int64]string{}
	for _, u := range batchUpserts(ub) {
		got[u.Key[0].(int64)] = u.After["v"].(string)
	}
	if got[1] != "live" {
		t.Fatalf("PK 1 must carry the live value, got %q (the snapshot version must lose)", got[1])
	}
	if got[3] != "new" {
		t.Fatalf("PK 3 must survive, got %q", got[3])
	}
	if len(batchDeletes(ub)) != 1 || batchDeletes(ub)[0].Key[0] != int64(4) {
		t.Fatalf("deletes = %+v, want [4]", batchDeletes(ub))
	}
}
