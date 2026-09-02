package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/sink"
)

type fakeCommitter struct {
	mu      sync.Mutex
	batches []change.Batch
	failAt  map[int]bool // fail the Nth flush (0-based)
}

func (f *fakeCommitter) Close() error { return nil }

func (f *fakeCommitter) Commit(_ context.Context, b change.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := len(f.batches)
	f.batches = append(f.batches, b)
	if f.failAt != nil && f.failAt[i] {
		return errors.New("boom")
	}
	return nil
}

func chg(table string, op change.Op, id int64, v, pos string) change.Change {
	c := change.Change{Op: op, Table: table, Key: []any{id}, Position: pos}
	if op != change.OpDelete {
		c.After = map[string]any{"id": id, "v": v}
	}
	return c
}

func runWorker(t *testing.T, cfg Config, targets []string, committers map[string]sink.TableWriter, changes []change.Change) error {
	t.Helper()
	w := New(cfg)
	for _, target := range targets {
		w.RegisterCommitter(target, committers[target])
	}
	ingest := make(chan change.Change, len(changes)+1)
	for _, c := range changes {
		ingest <- c
	}
	close(ingest)
	return w.Run(context.Background(), ingest)
}

func TestFlushOnCloseCollapses(t *testing.T) {
	fc := &fakeCommitter{}
	err := runWorker(t, Config{MaxRows: 100, MaxInterval: time.Hour}, []string{"raw.orders"},
		map[string]sink.TableWriter{"raw.orders": fc}, []change.Change{
			chg("raw.orders", change.OpInsert, 1, "a", "p1"),
			chg("raw.orders", change.OpUpdate, 1, "b", "p2"),
			chg("raw.orders", change.OpInsert, 2, "x", "p3"),
			chg("raw.orders", change.OpDelete, 2, "", "p4"),
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 {
		t.Fatalf("want single batch on close, got %d", len(fc.batches))
	}
	b := fc.batches[0]
	if len(b.Upserts) != 1 || b.Upserts[0].After["v"] != "b" {
		t.Fatalf("want id=1 v=b collapsed, got %+v", b.Upserts)
	}
	if len(b.Deletes) != 1 || b.Deletes[0].Key[0] != int64(2) {
		t.Fatalf("want id=2 deleted, got %+v", b.Deletes)
	}
	if b.Position != "p4" {
		t.Fatalf("position = %q, want p4 (last change)", b.Position)
	}
}

func TestFlushByMaxRows(t *testing.T) {
	fc := &fakeCommitter{}
	err := runWorker(t, Config{MaxRows: 2, MaxInterval: time.Hour}, []string{"t"},
		map[string]sink.TableWriter{"t": fc}, []change.Change{
			chg("t", change.OpInsert, 1, "a", "p1"),
			chg("t", change.OpInsert, 2, "b", "p2"),
			chg("t", change.OpInsert, 3, "c", "p3"),
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(fc.batches) != 2 {
		t.Fatalf("want 2 batches (2 flush + remainder), got %d", len(fc.batches))
	}
	if fc.batches[0].Position != "p2" || fc.batches[1].Position != "p3" {
		t.Fatalf("batch positions = %q, %q", fc.batches[0].Position, fc.batches[1].Position)
	}
}

func TestFlushByInterval(t *testing.T) {
	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: 30 * time.Millisecond})
	w.RegisterCommitter("t", fc)

	ingest := make(chan change.Change, 2)
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), ingest) }()

	ingest <- chg("t", change.OpInsert, 1, "a", "p1")
	time.Sleep(120 * time.Millisecond) // interval fires while channel stays open
	ingest <- chg("t", change.OpInsert, 2, "b", "p2")
	close(ingest)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 2 {
		t.Fatalf("want interval flush + close flush, got %d batches", len(fc.batches))
	}
}

func TestCommitFailureIsTerminalAndNeverSkips(t *testing.T) {
	fc := &fakeCommitter{failAt: map[int]bool{0: true}}
	err := runWorker(t, Config{MaxRows: 1, MaxInterval: time.Hour}, []string{"t"},
		map[string]sink.TableWriter{"t": fc}, []change.Change{
			chg("t", change.OpInsert, 1, "a", "p1"), // batch 0: fails
			chg("t", change.OpInsert, 2, "b", "p2"), // batch 1: must NOT be committed after failure
		})
	if err == nil {
		t.Fatal("commit failure must surface")
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if len(fc.batches) != 1 {
		t.Fatalf("no batch may be committed after a terminal failure, got %d", len(fc.batches))
	}
}

func TestTablesCommitIndependently(t *testing.T) {
	orders := &fakeCommitter{}
	items := &fakeCommitter{}
	err := runWorker(t, Config{MaxRows: 100, MaxInterval: time.Hour},
		[]string{"raw.orders", "raw.order_items"},
		map[string]sink.TableWriter{"raw.orders": orders, "raw.order_items": items},
		[]change.Change{
			chg("raw.orders", change.OpInsert, 1, "a", "p1"),
			chg("raw.order_items", change.OpInsert, 1, "x", "p2"),
			chg("raw.orders", change.OpDelete, 1, "", "p3"),
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(orders.batches) != 1 || len(orders.batches[0].Deletes) != 1 {
		t.Fatalf("orders batches = %+v", orders.batches)
	}
	if len(items.batches) != 1 || len(items.batches[0].Upserts) != 1 {
		t.Fatalf("items batches = %+v", items.batches)
	}
}

func TestCommitsAreSerializedPerTable(t *testing.T) {
	// A committer that asserts single-flight: Commit overlapping another
	// Commit would trip the detector.
	var mu sync.Mutex
	inFlight := 0
	overlapped := false
	slow := CommitterFunc(func(context.Context, change.Batch) error {
		mu.Lock()
		inFlight++
		if inFlight > 1 {
			overlapped = true
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	})

	changes := make([]change.Change, 0, 50)
	for i := range 50 {
		changes = append(changes, chg("t", change.OpInsert, int64(i), "v", "p"))
	}
	if err := runWorker(t, Config{MaxRows: 5, MaxInterval: time.Hour}, []string{"t"},
		map[string]sink.TableWriter{"t": slow}, changes); err != nil {
		t.Fatalf("run: %v", err)
	}
	if overlapped {
		t.Fatal("commits for the same table overlapped — serialization broken")
	}
}

type CommitterFunc func(context.Context, change.Batch) error

func (f CommitterFunc) Close() error { return nil }

func (f CommitterFunc) Commit(ctx context.Context, b change.Batch) error { return f(ctx, b) }
