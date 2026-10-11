package worker

import (
	"testing"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// A chunk is one SELECT, read in pages that become windows one after the
// other. A row of a later page shows the table as it was when the SELECT
// began; a change committed since can reach the worker before that page is
// stored as a window. It then touched no window, and the page's stale row
// went into the sink after it: a row deleted during the snapshot came back
// (#827). A live change seen while a chunk is being read counts against the
// windows that chunk still has to store.
func TestLiveChangeSeenDuringAChunkReadTouchesItsLaterWindows(t *testing.T) {
	w := New(Config{})
	regTable(t, w, "raw.orders", &fakeCommitter{}, dataplane.UpsertMode)
	p := w.tables["raw.orders"]

	w.BeginChunkRead("raw.orders")
	// The delete of key 2 arrives while the chunk's page holding it is still
	// to be stored.
	live := wireBatch(t, "raw.orders", dataplane.UpsertMode, []rowchange.Change{
		chg("raw.orders", rowchange.OpDelete, 2, "", "p9"),
	})
	defer live.Release()
	if err := markBatchSideEffects(p, live, Ingest{Table: "raw.orders", Batch: live, Win: &rowchange.Window{InWindow: true, WindowID: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.AddWindowRows("raw.orders", 2, toWindow(t, "raw.orders", payloadRows(1, 3, 8))); err != nil {
		t.Fatal(err)
	}
	w.EndChunkRead("raw.orders")

	out, held, err := closeWindow(p, Ingest{Table: "raw.orders", Win: &rowchange.Window{Closes: true, WindowID: 2}, Position: "0/9"})
	if err != nil || !held {
		t.Fatalf("closeWindow: held=%v err=%v", held, err)
	}
	defer out.Release()
	if out.Record.NumRows() != 2 {
		t.Fatalf("the window emits %d rows, want 2: the row of key 2 is older than the delete the worker already applied", out.Record.NumRows())
	}

	// Outside a chunk read nothing is remembered: a later chunk's window is
	// not touched by a change that came before its SELECT began.
	if err := markBatchSideEffects(p, live, Ingest{Table: "raw.orders", Batch: live}); err != nil {
		t.Fatal(err)
	}
	w.BeginChunkRead("raw.orders")
	if err := w.AddWindowRows("raw.orders", 3, toWindow(t, "raw.orders", payloadRows(1, 3, 8))); err != nil {
		t.Fatal(err)
	}
	w.EndChunkRead("raw.orders")
	out2, _, err := closeWindow(p, Ingest{Table: "raw.orders", Win: &rowchange.Window{Closes: true, WindowID: 3}, Position: "0/9"})
	if err != nil {
		t.Fatal(err)
	}
	defer out2.Release()
	if out2.Record.NumRows() != 3 {
		t.Fatalf("a later chunk's window emits %d rows, want 3", out2.Record.NumRows())
	}
}
