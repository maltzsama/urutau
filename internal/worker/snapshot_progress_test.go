package worker

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
)

// Issue #461: a window's Closes marker names the table's snapshot chunks
// still to do, and the window's rows commit with them, as state in_progress
// and cdc.snapshot.pending, in the same commit. A restarted coordinator
// resumes the snapshot from what is recorded there instead of chunk 0.
func TestWindowCommitsTheSnapshotPendingItsMarkerNames(t *testing.T) {
	cl := &commitLog{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", cl, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	if err := w.AddWindowRows("raw.orders", 7, toWindow(t, "raw.orders", []rowchange.Change{windowRow(1, "a"), windowRow(2, "b")})); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}
	closes := toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9"}, &rowchange.Window{WindowID: 7, Closes: true})
	closes.SnapshotPending = []uint32{3, 4}
	ingest := make(chan Ingest, 1)
	ingest <- closes
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()
	if len(cl.commits) != 1 {
		t.Fatalf("commits %+v, want the window's rows", cl.commits)
	}
	got := cl.commits[0]
	if got.rows != 2 || got.state != string(snapshot.StateInProgress) || !slices.Equal(got.pending, []uint32{3, 4}) {
		t.Fatalf("window commit %+v, want its 2 rows with state in_progress and pending [3 4]", got)
	}
}

// When the pipeline holds a snapshot state of its own, a Closes marker that
// names the chunks still to do is still what the window commits: the marker
// carries the latest progress (review of #462).
func TestWindowMarkerProgressWinsOverThePipelineState(t *testing.T) {
	cl := &commitLog{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", cl, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	w.SetSnapshotState("raw.orders", string(snapshot.StateInProgress), []uint32{1, 2, 3, 4})
	if err := w.AddWindowRows("raw.orders", 7, toWindow(t, "raw.orders", []rowchange.Change{windowRow(1, "a")})); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}
	closes := toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9"}, &rowchange.Window{WindowID: 7, Closes: true})
	closes.SnapshotPending = []uint32{3, 4}
	ingest := make(chan Ingest, 1)
	ingest <- closes
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if len(cl.commits) != 1 || !slices.Equal(cl.commits[0].pending, []uint32{3, 4}) {
		t.Fatalf("commits %+v, want the window committed with the marker's pending [3 4]", cl.commits)
	}
}
