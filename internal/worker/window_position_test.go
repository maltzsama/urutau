package worker

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// runWindowAfterLive runs one live change at p5 (when live), then a window
// closed by a marker at p9 with id 42, and returns the commits and the marker
// ids the worker acked.
func runWindowAfterLive(t *testing.T, live bool) ([]committed, []uint64) {
	t.Helper()
	cl := &commitLog{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", cl, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	var mu sync.Mutex
	var acked []uint64
	w.OnMarkerCommitted(func(_ string, id uint64) {
		mu.Lock()
		acked = append(acked, id)
		mu.Unlock()
	})
	if err := w.AddWindowRows("raw.orders", 7, toWindow(t, "raw.orders", []rowchange.Change{windowRow(1, "a")})); err != nil {
		t.Fatal(err)
	}
	ingest := make(chan Ingest, 3)
	if live {
		ingest <- toIngest(t, rowchange.Change{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(9)},
			After: map[string]any{"id": int64(9), "v": "x"}, Position: "p5"})
	}
	closes := toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9"}, &rowchange.Window{ChunkID: 7, Closes: true})
	closes.MarkerID = 42
	ingest <- closes
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatal(err)
	}
	cl.mu.Lock()
	defer cl.mu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	return append([]committed(nil), cl.commits...), append([]uint64(nil), acked...)
}

// #468: a window's marker carries the reader's position, past stream batches
// of the table still in the pump; committed as the table's position it
// covered them, and a crash skipped them on replay (chaos-1M-406f460 lost
// pr_items transactions 25474-25483). A window's rows commit no position:
// the commit takes the last stream row's, and the worker acks the marker by
// its id once the rows are committed.
func TestAWindowCommitsNoPositionAndAcksItsMarkerByID(t *testing.T) {
	commits, acked := runWindowAfterLive(t, true)
	if len(commits) == 0 || commits[len(commits)-1].watermark != "p5" {
		t.Fatalf("commits %+v, want the last at the stream's p5, not the marker's p9", commits)
	}
	if !slices.Equal(acked, []uint64{42}) {
		t.Fatalf("acked markers %v, want [42]", acked)
	}
}

// A window alone commits its rows with no position at all.
func TestAWindowAloneCommitsNoPosition(t *testing.T) {
	commits, acked := runWindowAfterLive(t, false)
	if len(commits) != 1 || commits[0].watermark != "" || commits[0].rows != 1 {
		t.Fatalf("commits %+v, want the window's row with no position", commits)
	}
	if !slices.Equal(acked, []uint64{42}) {
		t.Fatalf("acked markers %v, want [42]", acked)
	}
}
