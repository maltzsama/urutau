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
	w.OnStaged(func(string, uint64, []byte, string, string, []uint32, bool) error {
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

// A window that emits no rows — its rows all touched by live events, or a
// window this process never held — still owes its cycle an empty delivery,
// which the committer ships. The batcher acked the marker at once, ahead of
// that delivery: a worker killed in between had told the coordinator the
// marker was done, so it was never redelivered, the cycle waited for a
// delivery nobody would send, and every later cycle of the table queued
// behind it (#833: pr_accounts held at one position for the rest of the
// coordinator's life in the 1M chaos run).
func TestAnEmptyWindowsMarkerIsNotAckedBeforeItsDeliveryShips(t *testing.T) {
	sc := &stagingCommitter{desc: []byte("d")}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", sc, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	w.OnStaged(func(string, uint64, []byte, string, string, []uint32, bool) error {
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
	// No AddWindowRows: the window's rows went to a process that died.
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
		t.Fatalf("acked markers %v before the window's empty delivery shipped, want none", acked)
	}
}

// With the delivery shipped, the empty window's marker is acked.
func TestAnEmptyWindowsMarkerIsAckedOnceItsDeliveryShips(t *testing.T) {
	sc := &stagingCommitter{desc: []byte("d")}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	w.Register("raw.orders", sc, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", testSchema())
	var mu sync.Mutex
	var events []string
	w.OnStaged(func(string, uint64, []byte, string, string, []uint32, bool) error {
		mu.Lock()
		events = append(events, "delivery")
		mu.Unlock()
		return nil
	})
	if err := w.SetStaged("raw.orders"); err != nil {
		t.Fatalf("SetStaged: %v", err)
	}
	w.OnMarkerCommitted(func(string, uint64) {
		mu.Lock()
		events = append(events, "marker")
		mu.Unlock()
	})
	ingest := make(chan Ingest, 1)
	closes := toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9"}, &rowchange.Window{WindowID: 7, Closes: true})
	closes.Seq, closes.Staged, closes.MarkerID = 5, true, 42
	ingest <- closes
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("Run: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 || events[0] != "delivery" || events[1] != "marker" {
		t.Fatalf("events = %v, want the delivery and then the marker's ack", events)
	}
}
