package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// runStagedWindow runs a staged table through one DBLog window: the chunk's
// rows, the given live changes, then the Closes marker carrying seq. It
// returns the seqs of every staged delivery, in order.
func runStagedWindow(t *testing.T, snap, live []rowchange.Change, seq uint64) []uint64 {
	t.Helper()
	sc := &stagingCommitter{desc: []byte("d")}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", sc, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	var mu sync.Mutex
	var seqs []uint64
	w.OnStaged(func(_ string, s uint64, _ []byte, _, _ string, _ []uint32) error {
		mu.Lock()
		seqs = append(seqs, s)
		mu.Unlock()
		return nil
	})
	if err := w.SetStaged("raw.orders"); err != nil {
		t.Fatalf("SetStaged: %v", err)
	}
	if err := w.AddWindowRows("raw.orders", 7, toWindow(t, "raw.orders", snap)); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}
	ingest := make(chan Ingest, len(live)+1)
	for _, c := range live {
		ingest <- toIngest(t, c)
	}
	closes := toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9", Window: &rowchange.Window{ChunkID: 7, Closes: true}})
	closes.Seq, closes.Staged = seq, true
	ingest <- closes
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return seqs
}

func windowRow(id int64, v string) rowchange.Change {
	return rowchange.Change{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{id}, After: map[string]any{"id": id, "v": v}, Position: "p0"}
}

// On a staged table a window's rows are a cycle of the coordinator's send
// order: they must go out under the Closes marker's seq, so the coordinator
// commits them after every live cycle released ahead of the marker. As seq 0
// they committed on arrival, out of order, and could move the table's
// committed position past live cycles still open (#416).
func TestStagedWindowRowsCarryTheMarkerSeq(t *testing.T) {
	seqs := runStagedWindow(t, []rowchange.Change{windowRow(1, "a"), windowRow(2, "b")}, nil, 42)
	if len(seqs) != 1 || seqs[0] != 42 {
		t.Fatalf("staged deliveries %v, want exactly one under the marker's seq 42", seqs)
	}
}

// A window every row of which a live event touched emits no rows, but its
// cycle still needs a delivery, or it would wedge every later cycle.
func TestStagedEmptyWindowDeliversItsSeq(t *testing.T) {
	live := []rowchange.Change{{
		Op: rowchange.OpUpdate, Table: "raw.orders", Key: []any{int64(1)},
		After: map[string]any{"id": int64(1), "v": "z"}, Position: "p1",
		Window: &rowchange.Window{ChunkID: 7, InWindow: true},
	}}
	seqs := runStagedWindow(t, []rowchange.Change{windowRow(1, "a")}, live, 42)
	found := false
	for _, s := range seqs {
		if s == 42 {
			found = true
		}
	}
	if !found {
		t.Fatalf("staged deliveries %v: the emptied window's seq 42 was never delivered", seqs)
	}
}
