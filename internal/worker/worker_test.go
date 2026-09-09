package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/sink"
)

type fakeCommitter struct {
	mu      sync.Mutex
	batches []rowchange.Batch
	failAt  map[int]bool // fail the Nth flush (0-based)
}

func (f *fakeCommitter) Close() error { return nil }

func (f *fakeCommitter) Commit(_ context.Context, b *dataplane.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := len(f.batches)
	if b.Record == nil || b.Record.NumRows() == 0 {
		f.batches = append(f.batches, rowchange.Batch{Table: b.Table, Position: string(b.Watermark), Mode: rowchange.ToRowMode(b.Mode)})
	} else {
		rows, _ := transport.DecodeBatch(b.Record, b.Table, []string{"id"})
		if b.Mode == dataplane.AppendMode {
			// Append: every row is an upsert (deletes already rewritten/dropped).
			f.batches = append(f.batches, rowchange.Batch{Table: b.Table, Changes: rows, Position: string(b.Watermark), Mode: rowchange.ToRowMode(b.Mode)})
		} else {
			var upserts, deletes []rowchange.Change
			for _, r := range rows {
				if r.Op == rowchange.OpDelete {
					deletes = append(deletes, r)
				} else {
					upserts = append(upserts, r)
				}
			}
			f.batches = append(f.batches, rowchange.Batch{Table: b.Table, Changes: append(append([]rowchange.Change{}, upserts...), deletes...), Position: string(b.Watermark), Mode: rowchange.ToRowMode(b.Mode)})
		}
	}
	if f.failAt != nil && f.failAt[i] {
		return errors.New("boom")
	}
	return nil
}

func chg(table string, op rowchange.Op, id int64, v, pos string) rowchange.Change {
	c := rowchange.Change{Op: op, Table: table, Key: []any{id}, Position: pos}
	if op != rowchange.OpDelete {
		c.After = map[string]any{"id": id, "v": v}
	}
	return c
}

// regTable registers a table with the id/v schema (tests). The worker's
// columnar collapse needs the PK; tests that set up workers manually must
// use this instead of bare RegisterCommitter.
func regTable(t *testing.T, w *Worker, target string, c sink.TableWriter, mode dataplane.WriteMode) {
	t.Helper()
	w.RegisterCommitter(target, c, mode)
	w.SetKnownSchema(target, core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	})
}

func runWorker(t *testing.T, cfg Config, targets []string, committers map[string]sink.TableWriter, changes []rowchange.Change) error {
	t.Helper()
	w := New(cfg)
	for _, target := range targets {
		w.RegisterCommitter(target, committers[target], dataplane.UpsertMode)
		w.SetKnownSchema(target, testSchema())
	}
	raw := make(chan rowchange.Change, len(changes)+1)
	for _, c := range changes {
		raw <- c
	}
	close(raw)
	ingest := IngestFromChanges(context.Background(), raw, testSchema())
	return w.Run(context.Background(), ingest)
}

// testSchema is the id/v schema used across worker tests.
func testSchema() core.Schema {
	return core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
}

// toIngest wraps one change into an Ingest (bridging to a batch), or a
// window marker into Ingest with Win set.
func toIngest(t *testing.T, c rowchange.Change) Ingest {
	t.Helper()
	cb := rowchange.Batch{Table: c.Table, Changes: []rowchange.Change{c}, Mode: rowchange.UpsertMode}
	dpb, err := dpint.BatchFromChangeBatch(cb, testSchema())
	if err != nil {
		t.Fatalf("toIngest: %v", err)
	}
	if c.Window != nil {
		if c.Window.Closes {
			return Ingest{Table: c.Table, Win: c.Window, Position: c.Position}
		}
		// InWindow is DATA + a routing tag; the batch must survive.
		return Ingest{Table: c.Table, Batch: dpb, Win: c.Window}
	}
	return Ingest{Table: c.Table, Batch: dpb}
}

// toWindow bridges window rows into a batch for AddWindowRows.
func toWindow(t *testing.T, target string, rows []rowchange.Change) *dataplane.Batch {
	t.Helper()
	cb := rowchange.Batch{Table: target, Changes: rows, Mode: rowchange.AppendMode}
	dpb, err := dpint.BatchFromChangeBatch(cb, testSchema())
	if err != nil {
		t.Fatalf("toWindow: %v", err)
	}
	return dpb
}

func TestFlushOnCloseCollapses(t *testing.T) {
	fc := &fakeCommitter{}
	err := runWorker(t, Config{MaxRows: 100, MaxInterval: time.Hour}, []string{"raw.orders"},
		map[string]sink.TableWriter{"raw.orders": fc}, []rowchange.Change{
			chg("raw.orders", rowchange.OpInsert, 1, "a", "p1"),
			chg("raw.orders", rowchange.OpUpdate, 1, "b", "p2"),
			chg("raw.orders", rowchange.OpInsert, 2, "x", "p3"),
			chg("raw.orders", rowchange.OpDelete, 2, "", "p4"),
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 {
		t.Fatalf("want single batch on close, got %d", len(fc.batches))
	}
	b := fc.batches[0]
	if len(batchUpserts(b)) != 1 || batchUpserts(b)[0].After["v"] != "b" {
		t.Fatalf("want id=1 v=b collapsed, got %+v", batchUpserts(b))
	}
	if len(batchDeletes(b)) != 1 || batchDeletes(b)[0].Key[0] != int64(2) {
		t.Fatalf("want id=2 deleted, got %+v", batchDeletes(b))
	}
	if b.Position != "p4" {
		t.Fatalf("position = %q, want p4 (last change)", b.Position)
	}
}

func TestFlushByMaxRows(t *testing.T) {
	fc := &fakeCommitter{}
	err := runWorker(t, Config{MaxRows: 2, MaxInterval: time.Hour}, []string{"t"},
		map[string]sink.TableWriter{"t": fc}, []rowchange.Change{
			chg("t", rowchange.OpInsert, 1, "a", "p1"),
			chg("t", rowchange.OpInsert, 2, "b", "p2"),
			chg("t", rowchange.OpInsert, 3, "c", "p3"),
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
	regTable(t, w, "t", fc, dataplane.UpsertMode)

	ingest := make(chan rowchange.Change, 2)
	dpIngest := IngestFromChanges(context.Background(), ingest, testSchema())
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background(), dpIngest) }()

	ingest <- chg("t", rowchange.OpInsert, 1, "a", "p1")
	time.Sleep(120 * time.Millisecond) // interval fires while channel stays open
	ingest <- chg("t", rowchange.OpInsert, 2, "b", "p2")
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
		map[string]sink.TableWriter{"t": fc}, []rowchange.Change{
			chg("t", rowchange.OpInsert, 1, "a", "p1"), // batch 0: fails
			chg("t", rowchange.OpInsert, 2, "b", "p2"), // batch 1: must NOT be committed after failure
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
		[]rowchange.Change{
			chg("raw.orders", rowchange.OpInsert, 1, "a", "p1"),
			chg("raw.order_items", rowchange.OpInsert, 1, "x", "p2"),
			chg("raw.orders", rowchange.OpDelete, 1, "", "p3"),
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(orders.batches) != 1 || len(batchDeletes(orders.batches[0])) != 1 {
		t.Fatalf("orders batches = %+v", orders.batches)
	}
	if len(items.batches) != 1 || len(batchUpserts(items.batches[0])) != 1 {
		t.Fatalf("items batches = %+v", items.batches)
	}
}

func TestCommitsAreSerializedPerTable(t *testing.T) {
	// A committer that asserts single-flight: Commit overlapping another
	// Commit would trip the detector.
	var mu sync.Mutex
	inFlight := 0
	overlapped := false
	slow := CommitterFunc(func(context.Context, *dataplane.Batch) error {
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

	changes := make([]rowchange.Change, 0, 50)
	for i := range 50 {
		changes = append(changes, chg("t", rowchange.OpInsert, int64(i), "v", "p"))
	}
	if err := runWorker(t, Config{MaxRows: 5, MaxInterval: time.Hour}, []string{"t"},
		map[string]sink.TableWriter{"t": slow}, changes); err != nil {
		t.Fatalf("run: %v", err)
	}
	if overlapped {
		t.Fatal("commits for the same table overlapped — serialization broken")
	}
}

type CommitterFunc func(context.Context, *dataplane.Batch) error

func (f CommitterFunc) Close() error { return nil }

func (f CommitterFunc) Commit(ctx context.Context, b *dataplane.Batch) error { return f(ctx, b) }

// mergeBatches must propagate Mode on all three paths (a-only, b-only,
// concat) — a lost mode silently defaulted to upsert before the ModeUnset
// guard existed (audit #5). This is the exact trap the enum shift exposed.
func TestMergeBatchesPropagatesMode(t *testing.T) {
	mk := func(mode dataplane.WriteMode) *dataplane.Batch {
		b := dpint.GenerateBatch(1, dpint.GeneratorOpts{NumRows: 1, Allocator: nil})
		b.Mode = mode
		return b
	}
	for _, mode := range []dataplane.WriteMode{dataplane.UpsertMode, dataplane.AppendMode} {
		// a-only
		got, err := mergeBatches(mk(mode), nil, nil)
		if err != nil || got.Mode != mode {
			t.Fatalf("a-only mode = %v, want %v (err %v)", got.Mode, mode, err)
		}
		got.Release()
		// b-only
		got, err = mergeBatches(nil, mk(mode), nil)
		if err != nil || got.Mode != mode {
			t.Fatalf("b-only mode = %v, want %v (err %v)", got.Mode, mode, err)
		}
		got.Release()
		// concat
		got, err = mergeBatches(mk(mode), mk(mode), nil)
		if err != nil || got.Mode != mode {
			t.Fatalf("concat mode = %v, want %v (err %v)", got.Mode, mode, err)
		}
		got.Release()
	}
}

// batchUpserts/batchDeletes: ByOp views for assertions.
func batchUpserts(b rowchange.Batch) []rowchange.Change {
	u, _ := b.ByOp()
	return u
}

func batchDeletes(b rowchange.Batch) []rowchange.Change {
	_, d := b.ByOp()
	return d
}
