package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/internal/snapshot"
)

// A chunk's resume cursor is the high key of the last window whose Closes
// marker was acked: onMarkerAck records it, redoFrom returns it, and the redo
// re-reads the chunk from there instead of re-emitting committed windows
// (issue #646).
func TestChunkCursorRoundTrip(t *testing.T) {
	c, _ := coordHarness()
	c.markers.Note("w0", snapshot.Marker{ID: 100, Target: "raw.orders", Window: 1, ChunkRef: 7, HighKey: []any{int64(50)}})
	c.markers.RecordCursor("w0", 100)

	got := c.markers.TakeCursor("raw.orders", 7)
	if len(got) != 1 || got[0] != int64(50) {
		t.Fatalf("TakeCursor = %v, want [50]", got)
	}
	if got := c.markers.TakeCursor("raw.orders", 7); got != nil {
		t.Fatalf("TakeCursor after take = %v, want nil (the cursor is forgotten)", got)
	}
}

// A chunk with no committed window has no cursor: the redo re-reads it from
// its start.
func TestRedoFromNoCursorWhenNoWindowCommitted(t *testing.T) {
	c, w := coordHarness()
	c.markers.Lose("w0", []snapshot.Marker{{Target: "raw.orders", Window: 2}})

	windows := map[uint64]int{2: 0}
	from, cursor := c.redoFrom(w, "raw.orders", windows, 0, 0)
	if from != 0 {
		t.Fatalf("redoFrom = %d, want 0", from)
	}
	if cursor != nil {
		t.Fatalf("cursor = %v, want nil (no window committed)", cursor)
	}
}

// When window 1 of a chunk committed and window 2 was lost, the redo resumes
// the chunk from window 1's high key.
func TestRedoFromResumesAtLastCommittedWindow(t *testing.T) {
	c, w := coordHarness()
	// Window 1 (seq 1) committed: its marker was acked, recording the cursor.
	c.markers.Note(w.name, snapshot.Marker{ID: 100, Target: "raw.orders", Window: 1, ChunkRef: 0, HighKey: []any{int64(50)}})
	c.markers.RecordCursor(w.name, 100)
	// Window 2 (seq 2) was lost without its marker being acked.
	c.markers.Lose(w.name, []snapshot.Marker{{Target: "raw.orders", Window: 2}})

	windows := map[uint64]int{1: 0, 2: 0}
	from, cursor := c.redoFrom(w, "raw.orders", windows, 0, 0)
	if from != 0 {
		t.Fatalf("redoFrom = %d, want 0", from)
	}
	if len(cursor) != 1 || cursor[0] != int64(50) {
		t.Fatalf("cursor = %v, want [50] (window 1's high key)", cursor)
	}
}

// The cursor is recorded only on a marker ack, never for a window whose
// Closes marker was never sent: a mid-window loss (worker died between
// WindowOpen and sendCloses) must re-read the whole chunk, not resume past
// rows that were never committed.
func TestCursorNotRecordedWithoutMarkerAck(t *testing.T) {
	c, _ := coordHarness()
	c.markers.Note("w0", snapshot.Marker{ID: 100, Target: "raw.orders", Window: 1, ChunkRef: 7, HighKey: []any{int64(50)}})
	// No onMarkerAck: the marker was never acked.
	if got := c.markers.TakeCursor("raw.orders", 7); got != nil {
		t.Fatalf("cursor = %v, want nil (no ack)", got)
	}
}

// A cursor belongs to one table's chunk. Table A finishes its snapshot with a
// cursor recorded at (partition 0, chunk 0) and never redoes it; table B then
// loses its worker at its own (partition 0, chunk 0). B's redo must start the
// chunk from its beginning: resuming from A's high key would skip every row of
// B's chunk below it.
func TestAChunkCursorNeverCrossesTables(t *testing.T) {
	c, w := coordHarness()
	c.markers.Note(w.name, snapshot.Marker{ID: 100, Target: "raw.a", Window: 1, ChunkRef: snapshot.ChunkRef(0, 0), HighKey: []any{int64(50)}})
	c.markers.RecordCursor(w.name, 100)

	c.markers.Lose(w.name, []snapshot.Marker{{Target: "raw.b", Window: 9}})
	from, cursor := c.redoFrom(w, "raw.b", map[uint64]int{9: 0}, 0, 0)
	if from != 0 {
		t.Fatalf("redoFrom = %d, want 0", from)
	}
	if cursor != nil {
		t.Fatalf("table raw.b resumes its chunk from %v, table raw.a's cursor; want nil", cursor)
	}
}

// A table's cursors are dropped when its snapshot finishes: nothing will redo
// its chunks again, and the entries would otherwise stay for the whole run.
func TestFinishedSnapshotForgetsItsCursors(t *testing.T) {
	c, w := coordHarness()
	c.markers.Note(w.name, snapshot.Marker{ID: 100, Target: "raw.a", Window: 1, ChunkRef: snapshot.ChunkRef(0, 0), HighKey: []any{int64(50)}})
	c.markers.Note(w.name, snapshot.Marker{ID: 101, Target: "raw.b", Window: 2, ChunkRef: snapshot.ChunkRef(0, 0), HighKey: []any{int64(70)}})
	c.markers.RecordCursor(w.name, 100)
	c.markers.RecordCursor(w.name, 101)

	c.markers.ForgetCursors("raw.a")

	if got := c.markers.TakeCursor("raw.a", snapshot.ChunkRef(0, 0)); got != nil {
		t.Fatalf("raw.a still has cursor %v after its snapshot finished", got)
	}
	if got := c.markers.TakeCursor("raw.b", snapshot.ChunkRef(0, 0)); len(got) != 1 || got[0] != int64(70) {
		t.Fatalf("raw.b's cursor = %v, want [70]: another table's finish must not touch it", got)
	}
}

// redoFrom takes the lost windows of its own table and leaves another
// table's where they are, for that table's redo.
func TestRedoFromLeavesAnotherTablesLostWindows(t *testing.T) {
	c, w := coordHarness()
	c.markers.Lose(w.name, []snapshot.Marker{
		{Target: "raw.a", Window: 1},
		{Target: "raw.b", Window: 2},
	})

	if from, _ := c.redoFrom(w, "raw.a", map[uint64]int{1: 0}, 0, 3); from != 0 {
		t.Fatalf("redoFrom(raw.a) = %d, want 0", from)
	}
	left := c.markers.Lost(w.name)
	if len(left) != 1 || left[0].Target != "raw.b" {
		t.Fatalf("lost windows left = %+v, want raw.b's marker kept for its own redo", left)
	}
	if from, _ := c.redoFrom(w, "raw.b", map[uint64]int{2: 1}, 0, 3); from != 1 {
		t.Fatalf("redoFrom(raw.b) = %d, want 1: its lost window was dropped by raw.a's redo", from)
	}
	if left := c.markers.Lost(w.name); len(left) != 0 {
		t.Fatal("the worker's entry was kept after its last marker was taken")
	}
}
