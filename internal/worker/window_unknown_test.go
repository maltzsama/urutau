package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// stagedClosesPending runs one Closes marker naming chunks 8 and 9 still to
// do through a staged table, holding the window's rows first only if held,
// and returns the snapshot progress the marker's delivery carried.
func stagedClosesPending(t *testing.T, held bool) (delivered bool, pending []uint32) {
	t.Helper()
	sc := &stagingCommitter{desc: []byte("d")}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", sc, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	var mu sync.Mutex
	w.OnStaged(func(_ string, s uint64, _ []byte, _, _ string, p []uint32) error {
		mu.Lock()
		defer mu.Unlock()
		if s == 42 {
			delivered, pending = true, p
		}
		return nil
	})
	if err := w.SetStaged("raw.orders"); err != nil {
		t.Fatal(err)
	}
	ingest := make(chan Ingest, 4)
	if held {
		if err := w.AddWindowRows("raw.orders", 7, toWindow(t, "raw.orders", []rowchange.Change{windowRow(1, "a")})); err != nil {
			t.Fatal(err)
		}
		// A live event touches the window's only row: it emits no rows.
		ingest <- toIngest(t, rowchange.Change{
			Op: rowchange.OpUpdate, Table: "raw.orders", Key: []any{int64(1)},
			After: map[string]any{"id": int64(1), "v": "z"}, Position: "p1",
		}, &rowchange.Window{ChunkID: 7, InWindow: true})
	}
	closes := toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9"}, &rowchange.Window{ChunkID: 7, Closes: true})
	closes.Seq, closes.Staged, closes.SnapshotPending = 42, true, []uint32{8, 9}
	ingest <- closes
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	return delivered, pending
}

// A redelivered Closes marker of a window this worker does not hold — a lost
// window, whose rows died with the previous process — still owes its cycle a
// delivery, but must not carry the snapshot progress: committed, it would
// record the chunk done with none of its rows, and a coordinator restarted
// before the redo would resume past it (PR #462 review).
func TestAnUnknownWindowsMarkerCarriesNoSnapshotProgress(t *testing.T) {
	delivered, pending := stagedClosesPending(t, false)
	if !delivered {
		t.Fatal("the unknown window's cycle was never delivered")
	}
	if pending != nil {
		t.Fatalf("the unknown window's delivery carried snapshot progress %v; want none", pending)
	}
}

// A window this worker holds that emits no rows (live events touched them
// all) did its chunk: its progress is real and commits.
func TestAHeldEmptyWindowsMarkerCarriesItsProgress(t *testing.T) {
	delivered, pending := stagedClosesPending(t, true)
	if !delivered || len(pending) != 2 {
		t.Fatalf("delivered=%v pending=%v, want the held window's progress [8 9]", delivered, pending)
	}
}
