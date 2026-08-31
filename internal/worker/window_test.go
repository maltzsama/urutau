package worker

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/change"
)

// TestWindowSnapshotSingleBatch drives one DBLog window through the batcher:
// the chunk SELECT rows land in the window, a live UPDATE of one key tags it
// InWindow (the stale snapshot row must be discarded), and the Closes marker
// emits the remaining rows. The final commit must contain the live value for
// the updated key and the snapshot values for the rest — and never the stale
// snapshot value.
func TestWindowSnapshotSingleBatch(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.RegisterCommitter("raw.orders", fc)

	// chunk rows: id=1 v=a (stale), id=2 v=x (stable), id=3 v=y (stable)
	snap := []change.Change{
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, Position: "p0"},
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "v": "x"}, Position: "p0"},
		{Op: change.OpInsert, Table: "raw.orders", Key: []any{int64(3)}, After: map[string]any{"id": int64(3), "v": "y"}, Position: "p0"},
	}
	if err := w.AddWindowRows("raw.orders", 7, snap); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}

	ingest := make(chan change.Change, 8)
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ingest) }()

	// live UPDATE of id=1 inside the window: must discard the stale v=a row.
	ingest <- change.Change{
		Op: change.OpUpdate, Table: "raw.orders", Key: []any{int64(1)},
		After:    map[string]any{"id": int64(1), "v": "b"},
		Position: "p1",
		Window:   &change.Window{ChunkID: 7, InWindow: true},
	}
	// Closes: emits the remaining window rows (id=2, id=3) as inserts.
	ingest <- change.Change{
		Table: "raw.orders", Position: "p2",
		Window: &change.Window{ChunkID: 7, Closes: true},
	}
	close(ingest)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 {
		t.Fatalf("want 1 commit, got %d", len(fc.batches))
	}
	b := fc.batches[0]
	got := map[int64]string{}
	for _, u := range b.Upserts {
		got[u.Key[0].(int64)] = u.After["v"].(string)
	}
	if got[1] != "b" {
		t.Fatalf("id=1 must carry the live value b, got %q", got[1])
	}
	if got[2] != "x" || got[3] != "y" {
		t.Fatalf("ids 2,3 must carry snapshot values, got %v", got)
	}
	if w.DroppedByWindow("raw.orders") != 1 {
		t.Fatalf("dropped = %d, want 1 (the stale id=1 row)", w.DroppedByWindow("raw.orders"))
	}
}

// TestWindowLiveDeleteWins: a live DELETE inside the window removes the key
// from the window (no snapshot insert) and flows through as a delete.
func TestWindowLiveDeleteWins(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.RegisterCommitter("t", fc)

	_ = w.AddWindowRows("t", 1, []change.Change{
		{Op: change.OpInsert, Table: "t", Key: []any{int64(9)}, After: map[string]any{"id": int64(9), "v": "s"}, Position: "p0"},
	})

	ingest := make(chan change.Change, 8)
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ingest) }()

	ingest <- change.Change{
		Op: change.OpDelete, Table: "t", Key: []any{int64(9)}, Position: "p1",
		Window: &change.Window{ChunkID: 1, InWindow: true},
	}
	ingest <- change.Change{
		Table: "t", Position: "p2", Window: &change.Window{ChunkID: 1, Closes: true},
	}
	close(ingest)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 {
		t.Fatalf("want 1 commit, got %d", len(fc.batches))
	}
	b := fc.batches[0]
	if len(b.Upserts) != 0 {
		t.Fatalf("no snapshot row may survive a live delete, got upserts %+v", b.Upserts)
	}
	if len(b.Deletes) != 1 || b.Deletes[0].Key[0] != int64(9) {
		t.Fatalf("want delete of id=9, got %+v", b.Deletes)
	}
}

// TestWindowNoEventsClosesEmitsAll: an idle table where no live event hits
// the window — the Closes marker must emit every snapshot row.
func TestWindowNoEventsClosesEmitsAll(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.RegisterCommitter("t", fc)

	_ = w.AddWindowRows("t", 1, []change.Change{
		{Op: change.OpInsert, Table: "t", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, Position: "p0"},
		{Op: change.OpInsert, Table: "t", Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "v": "b"}, Position: "p0"},
	})

	ingest := make(chan change.Change, 8)
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ingest) }()

	ingest <- change.Change{Table: "t", Position: "p5", Window: &change.Window{ChunkID: 1, Closes: true}}
	close(ingest)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 {
		t.Fatalf("want 1 commit, got %d", len(fc.batches))
	}
	b := fc.batches[0]
	if len(b.Upserts) != 2 {
		t.Fatalf("want both snapshot rows emitted, got %+v", b.Upserts)
	}
	// Emitted rows carry the marker's position (the safe resume point).
	if b.Position != "p5" {
		t.Fatalf("batch position = %q, want p5 (marker)", b.Position)
	}
}
