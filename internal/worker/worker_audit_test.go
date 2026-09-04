package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/sink"
	"github.com/maltzsama/urutau/internal/snapshot"
)

// snapChange builds a snapshot row (chunk SELECT result).
func snapChange(table string, id int64, v, pos string) change.Change {
	return change.Change{
		Op: change.OpInsert, Table: table, Key: []any{id}, Position: pos,
		After:    map[string]any{"id": id, "v": v},
		Snapshot: true,
	}
}

// runSnapshotWorker boots a worker with the given snapshot state and feeds
// the changes, returning the committed batches.
func runSnapshotWorker(t *testing.T, state string, pending []uint32, changes []change.Change) ([]change.Batch, error) {
	t.Helper()
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.RegisterCommitter("t", fc, change.UpsertMode)
	w.SetSnapshotState("t", state, pending)
	ingest := make(chan change.Change, len(changes)+1)
	for _, c := range changes {
		ingest <- c
	}
	close(ingest)
	err := w.Run(context.Background(), ingest)
	return fc.batches, err
}

// The partitioned snapshot flush must advance the position only on the last
// commit: when live events follow the snapshot rows, the append batch carries
// no position — a crash between the two commits would otherwise resume past
// the never-committed live events.
func TestSnapshotPartitionPositionOnlyOnLastCommit(t *testing.T) {
	batches, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0, 1}, []change.Change{
		snapChange("t", 1, "s1", "low"),
		chg("t", change.OpInsert, 2, "live", "p2"),
		chg("t", change.OpInsert, 3, "live", "p3"),
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want append + upsert", len(batches))
	}
	ab, ub := batches[0], batches[1]
	if ab.Mode != change.AppendMode {
		t.Fatalf("batch 0 mode = %v, want append", ab.Mode)
	}
	if ab.Position != "" {
		t.Fatalf("append batch position = %q, want empty (upsert batch commits last)", ab.Position)
	}
	if len(ab.Upserts) != 1 || ab.Upserts[0].Key[0] != int64(1) {
		t.Fatalf("append batch rows = %+v, want snapshot id=1 only", ab.Upserts)
	}
	if ub.Mode != change.UpsertMode || ub.Position != "p3" {
		t.Fatalf("upsert batch = mode %v pos %q, want upsert at p3", ub.Mode, ub.Position)
	}
}

// With no live events in the buffer, the append batch IS the last commit and
// must carry the position.
func TestSnapshotAppendOnlyCarriesPosition(t *testing.T) {
	batches, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0}, []change.Change{
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
	batches, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0}, []change.Change{
		chg("t", change.OpUpdate, 5, "live5", "p1"), // live: touches key 5
		snapChange("t", 5, "snap5", "low"),          // snapshot re-reads key 5
		snapChange("t", 6, "snap6", "low"),          // key 6 was never touched
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want append + upsert", len(batches))
	}
	ab, ub := batches[0], batches[1]
	if len(ab.Upserts) != 1 || ab.Upserts[0].Key[0] != int64(6) {
		t.Fatalf("append batch = %+v, want only the untouched key 6", ab.Upserts)
	}
	// Key 5 must take the upsert path (collapse keeps the last version —
	// the snapshot re-read the updated row — and it carries an equality
	// delete). The critical assertion is that it is NOT in the append
	// batch above.
	found := false
	for _, u := range ub.Upserts {
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
	if _, err := runSnapshotWorker(t, string(snapshot.StateComplete), nil, []change.Change{
		snapChange("t", 1, "s1", "low"),
		chg("t", change.OpInsert, 2, "live", "p1"),
	}); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if _, err := runSnapshotWorker(t, string(snapshot.StateInProgress), []uint32{0}, []change.Change{
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
	w.RegisterCommitter("t", fc, change.UpsertMode)
	w.SetKnownColumns("t", map[string]bool{"id": true})
	w.OnSchemaDrift(func(d SchemaDrift) { drifts = append(drifts, d) })

	ingest := make(chan change.Change, 8)
	ingest <- change.Change{Op: change.OpInsert, Table: "t", Key: []any{1},
		After: map[string]any{"id": int64(1), "extra": "x"}, Position: "p1"}
	ingest <- change.Change{Op: change.OpInsert, Table: "t", Key: []any{2},
		After: map[string]any{"id": int64(2), "extra": "y", "other": "z"}, Position: "p2"}
	close(ingest)
	err := w.Run(context.Background(), ingest)
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
	w.RegisterCommitter("t", fc, change.UpsertMode)
	w.SetSnapshotState("t", string(snapshot.StateInProgress), []uint32{2})
	w.MarkSnapshotResumed("t")

	ingest := make(chan change.Change, 4)
	ingest <- snapChange("t", 1, "s1", "low")
	ingest <- snapChange("t", 2, "s2", "low")
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}

	// A single upsert batch (no append-only split), carrying both rows
	// through the delete-then-append path.
	if len(fc.batches) != 1 {
		t.Fatalf("batches = %d, want 1 upsert batch (no append split)", len(fc.batches))
	}
	b := fc.batches[0]
	if b.Mode != change.UpsertMode {
		t.Fatalf("mode = %v, want upsert on a resumed snapshot", b.Mode)
	}
	if len(b.Upserts) != 2 || b.Position != "low" {
		t.Fatalf("upserts = %d pos %q, want 2 rows at low", len(b.Upserts), b.Position)
	}
}

// 28.2: append-only delete handling. record with a before image appends the
// row; a delete with no before image (Kafka tombstone) is dropped and
// counted, never written as an all-null row; onDelete: skip drops deletes
// even when a before image exists.
func TestAppendModeDeleteHandling(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.RegisterCommitter("t", fc, change.AppendMode)
	var dropped []string
	w.OnDroppedDelete(func(table, pos string) { dropped = append(dropped, pos) })

	ingest := make(chan change.Change, 8)
	ingest <- change.Change{Op: change.OpInsert, Table: "t", Key: []any{1},
		After: map[string]any{"id": int64(1), "v": "a"}, Position: "p1"}
	// record: has a before image -> row appended.
	ingest <- change.Change{Op: change.OpDelete, Table: "t", Key: []any{2},
		Before: map[string]any{"id": int64(2), "v": "gone"}, Position: "p2"}
	// record: NO before image -> dropped, counted, never an all-null row.
	ingest <- change.Change{Op: change.OpDelete, Table: "t", Key: []any{3},
		Position: "p3"}
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 {
		t.Fatalf("batches = %d, want 1", len(fc.batches))
	}
	b := fc.batches[0]
	if len(b.Upserts) != 2 {
		t.Fatalf("upserts = %d, want 2 (insert + recorded delete) — no all-null row", len(b.Upserts))
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
	w.RegisterCommitter("t", fc, change.AppendMode)
	w.SetDropDeletes("t", true)

	ingest := make(chan change.Change, 4)
	ingest <- change.Change{Op: change.OpInsert, Table: "t", Key: []any{1},
		After: map[string]any{"id": int64(1), "v": "a"}, Position: "p1"}
	ingest <- change.Change{Op: change.OpDelete, Table: "t", Key: []any{2},
		Before: map[string]any{"id": int64(2), "v": "gone"}, Position: "p2"}
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}
	b := fc.batches[0]
	if len(b.Upserts) != 1 {
		t.Fatalf("upserts = %d, want 1 — skip drops even a before-image delete", len(b.Upserts))
	}
	if w.DroppedDeletes("t") != 1 {
		t.Fatalf("DroppedDeletes = %d, want 1", w.DroppedDeletes("t"))
	}
}
