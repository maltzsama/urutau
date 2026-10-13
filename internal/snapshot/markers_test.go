package snapshot

import "testing"

// Held prunes the markers the worker committed and keeps the rest in order.
func TestMarkersHeldDropsCommitted(t *testing.T) {
	var m Markers
	for id := uint64(1); id <= 4; id++ {
		m.Note("w0", Marker{ID: id, Target: "raw.a", Window: id})
	}
	inFlight := map[uint64]bool{2: true, 4: true}
	held := m.Held("w0", func(id uint64) bool { return inFlight[id] })
	if len(held) != 2 || held[0].ID != 2 || held[1].ID != 4 {
		t.Fatalf("held = %+v, want markers 2 and 4", held)
	}
	delete(inFlight, 2)
	if held := m.Held("w0", func(id uint64) bool { return inFlight[id] }); len(held) != 1 || held[0].ID != 4 {
		t.Fatalf("held = %+v, want marker 4", held)
	}
	if held := m.Held("w0", func(uint64) bool { return false }); held != nil {
		t.Fatalf("held = %+v, want none once everything committed", held)
	}
	if held := m.Held("w1", func(uint64) bool { return true }); held != nil {
		t.Fatalf("held of an unknown worker = %+v, want none", held)
	}
}

// A cursor is recorded only for a marker still queued, and is taken once.
func TestMarkersCursorIsTakenOnce(t *testing.T) {
	var m Markers
	m.Note("w0", Marker{ID: 10, Target: "raw.a", Window: 1, ChunkRef: 3, HighKey: []any{int64(7)}})
	m.Note("w0", Marker{ID: 11, Target: "raw.a", Window: 2, ChunkRef: 4})
	m.RecordCursor("w0", 10)
	m.RecordCursor("w0", 11) // no high key: no cursor
	m.RecordCursor("w0", 99) // never queued
	if got := m.TakeCursor("raw.a", 4); got != nil {
		t.Fatalf("cursor of a marker without a high key = %v, want nil", got)
	}
	if got := m.TakeCursor("raw.b", 3); got != nil {
		t.Fatalf("another table's cursor = %v, want nil: chunk refs are per table", got)
	}
	if got := m.TakeCursor("raw.a", 3); len(got) != 1 || got[0] != int64(7) {
		t.Fatalf("cursor = %v, want [7]", got)
	}
	if got := m.TakeCursor("raw.a", 3); got != nil {
		t.Fatalf("cursor after take = %v, want nil", got)
	}
}

func TestMarkersForgetCursorsIsPerTable(t *testing.T) {
	var m Markers
	m.Note("w0", Marker{ID: 1, Target: "raw.a", HighKey: []any{int64(1)}})
	m.Note("w0", Marker{ID: 2, Target: "raw.b", HighKey: []any{int64(2)}})
	m.RecordCursor("w0", 1)
	m.RecordCursor("w0", 2)
	m.ForgetCursors("raw.a")
	if got := m.TakeCursor("raw.a", 0); got != nil {
		t.Fatalf("raw.a's cursor = %v, want nil", got)
	}
	if got := m.TakeCursor("raw.b", 0); len(got) != 1 {
		t.Fatalf("raw.b's cursor = %v, want kept", got)
	}
}

// TakeLost takes one table's lost windows and keeps the others.
func TestMarkersTakeLostIsPerTable(t *testing.T) {
	var m Markers
	m.Lose("w0", nil)
	if got := m.Lost("w0"); len(got) != 0 {
		t.Fatalf("lost = %+v, want none", got)
	}
	m.Lose("w0", []Marker{{Target: "raw.a", Window: 1}, {Target: "raw.b", Window: 2}})
	m.Lose("w0", []Marker{{Target: "raw.a", Window: 3}})
	if got := m.TakeLost("w0", "raw.a"); len(got) != 2 || got[0].Window != 1 || got[1].Window != 3 {
		t.Fatalf("taken = %+v, want raw.a's windows 1 and 3", got)
	}
	if got := m.Lost("w0"); len(got) != 1 || got[0].Target != "raw.b" {
		t.Fatalf("left = %+v, want raw.b's window", got)
	}
	if got := m.TakeLost("w0", "raw.b"); len(got) != 1 {
		t.Fatalf("taken = %+v, want raw.b's window", got)
	}
	if got := m.Lost("w0"); len(got) != 0 {
		t.Fatalf("left = %+v, want none", got)
	}
}

// The redo starts at the earliest chunk whose window the worker lost, and
// resumes that chunk from the last window it did commit.
func TestMarkersRedoFrom(t *testing.T) {
	var m Markers
	// Chunk 1's first window committed (its cursor is recorded); its second
	// window and chunk 2's were lost.
	m.Note("w0", Marker{ID: 10, Target: "raw.a", Window: 5, ChunkRef: ChunkRef(0, 1), HighKey: []any{int64(40)}})
	m.RecordCursor("w0", 10)
	m.Lose("w0", []Marker{{Target: "raw.a", Window: 6}, {Target: "raw.a", Window: 7}})
	windows := map[uint64]int{5: 1, 6: 1, 7: 2}

	from, cursor := m.RedoFrom("w0", "raw.a", windows, 0, 3)
	if from != 1 || len(cursor) != 1 || cursor[0] != int64(40) {
		t.Fatalf("redo from chunk %d with cursor %v, want chunk 1 from key 40", from, cursor)
	}
	// Nothing lost any more: the snapshot goes on where it was.
	if from, cursor := m.RedoFrom("w0", "raw.a", windows, 0, 3); from != 3 || cursor != nil {
		t.Fatalf("second redo from chunk %d with cursor %v, want chunk 3 and no cursor", from, cursor)
	}
}
