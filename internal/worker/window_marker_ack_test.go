package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// A window's marker is acked only once its rows are staged and shipped. The
// batcher acked it as soon as it handed the rows to the committer, so a
// worker killed before the ship had already told the coordinator the window
// was done: the snapshot redo skipped the chunk, and the window's cycle stayed
// open at the head of the table's send order, blocking every later cycle
// (chaos-1M-a3da90e: pr_accounts stuck 9 minutes on seq 59).
func TestAWindowsMarkerIsNotAckedBeforeItsRowsShip(t *testing.T) {
	sc := &stagingCommitter{desc: []byte("d")}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", sc, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	w.OnStaged(func(string, uint64, []byte, string, string, []uint32) error {
		return errors.New("session lost") // the worker dies before the ship
	})
	if err := w.SetStaged("raw.orders"); err != nil {
		t.Fatalf("SetStaged: %v", err)
	}
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
	ingest := make(chan Ingest, 1)
	closes := toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9"}, &rowchange.Window{WindowID: 7, Closes: true})
	closes.Seq, closes.Staged, closes.MarkerID = 5, true, 42
	ingest <- closes
	close(ingest)
	if err := w.Run(context.Background(), ingest); err == nil {
		t.Fatal("run succeeded, want the ship failure")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(acked) != 0 {
		t.Fatalf("acked markers %v before the window's rows shipped, want none", acked)
	}
}
