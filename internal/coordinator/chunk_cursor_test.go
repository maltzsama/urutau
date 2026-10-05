package coordinator

import "testing"

// A chunk's resume cursor is the high key of the last window whose Closes
// marker was acked: onMarkerAck records it, redoFrom returns it, and the redo
// re-reads the chunk from there instead of re-emitting committed windows
// (issue #646).
func TestChunkCursorRoundTrip(t *testing.T) {
	c, _ := coordHarness()
	c.noteChunkMarker("w0", 100, "raw.orders", 1, 7, []any{int64(50)})
	c.recordCursor("w0", 100)

	got := c.takeCursor(7)
	if len(got) != 1 || got[0] != int64(50) {
		t.Fatalf("takeCursor = %v, want [50]", got)
	}
	if got := c.takeCursor(7); got != nil {
		t.Fatalf("takeCursor after take = %v, want nil (the cursor is forgotten)", got)
	}
}

// A chunk with no committed window has no cursor: the redo re-reads it from
// its start.
func TestRedoFromNoCursorWhenNoWindowCommitted(t *testing.T) {
	c, w := coordHarness()
	c.lostWindows = map[string][]chunkMarker{"w0": {{target: "raw.orders", window: 2}}}

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
	c.noteChunkMarker(w.name, 100, "raw.orders", 1, 0, []any{int64(50)})
	c.recordCursor(w.name, 100)
	// Window 2 (seq 2) was lost without its marker being acked.
	c.lostWindows = map[string][]chunkMarker{w.name: {{target: "raw.orders", window: 2}}}

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
	c.noteChunkMarker("w0", 100, "raw.orders", 1, 7, []any{int64(50)})
	// No onMarkerAck/recordCursor: the marker was never acked.
	if got := c.takeCursor(7); got != nil {
		t.Fatalf("cursor = %v, want nil (no ack)", got)
	}
}
