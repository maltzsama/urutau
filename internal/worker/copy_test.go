package worker

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// payloadRows are n inserts of raw.orders whose v is size bytes.
func payloadRows(from, n, size int) []rowchange.Change {
	rows := make([]rowchange.Change, n)
	for i := range rows {
		id := int64(from + i)
		rows[i] = rowchange.Change{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{id},
			After: map[string]any{"id": id, "v": strings.Repeat("x", size)}, Position: fmt.Sprintf("p%d", id)}
	}
	return rows
}

// vData is the address of the v column's data buffer: equal addresses mean
// the rows were not copied.
func vData(t *testing.T, rec arrow.RecordBatch) uintptr {
	t.Helper()
	for i := range int(rec.NumCols()) {
		if rec.ColumnName(i) == "v" {
			b := rec.Column(i).Data().Buffers()[2].Bytes()
			return uintptr(unsafe.Pointer(&b[0]))
		}
	}
	t.Fatal("no v column")
	return 0
}

// A window no live event touched goes out whole: its rows must not be copied
// (a Take of every row) on the way to the sink — the full profile's dying
// events worker held ~373 MB of such copies (#448).
func TestUntouchedWindowClosesWithoutCopying(t *testing.T) {
	w := New(Config{})
	regTable(t, w, "raw.orders", &fakeCommitter{}, dataplane.UpsertMode)
	win := toWindow(t, "raw.orders", payloadRows(1, 100, 1024))
	before := vData(t, win.Record)
	if err := w.AddWindowRows("raw.orders", 7, win); err != nil {
		t.Fatal(err)
	}
	out, _, err := closeWindow(w.tables["raw.orders"], Ingest{Table: "raw.orders", Win: &rowchange.Window{Closes: true, WindowID: 7}, Position: "0/9"})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Record.NumRows() != 100 {
		t.Fatalf("%d rows out, want 100", out.Record.NumRows())
	}
	if vData(t, out.Record) != before {
		t.Fatal("the untouched window's rows were copied on close")
	}
}

// concatBatches merged pending batches pairwise: ((1+2)+3)+... copies the
// growing prefix again for every batch, quadratic in their count. One
// concatenation copies each batch once.
func TestConcatBatchesCopiesEachBatchOnce(t *testing.T) {
	const k, rows, size = 16, 64, 1024 // 16 batches of 64 KiB of v
	bs := make([]*dataplane.Batch, k)
	for i := range bs {
		bs[i] = toWindow(t, "raw.orders", payloadRows(i*rows, rows, size))
		defer bs[i].Release()
	}
	var m0, m1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m0)
	out, err := dpint.ConcatBatches(memory.DefaultAllocator, bs)
	runtime.ReadMemStats(&m1)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Release()
	if out.Record.NumRows() != k*rows {
		t.Fatalf("%d rows, want %d", out.Record.NumRows(), k*rows)
	}
	total := uint64(k * rows * size)
	alloc := m1.TotalAlloc - m0.TotalAlloc
	t.Logf("concatenating %d KiB allocated %d KiB (%.1fx)", total>>10, alloc>>10, float64(alloc)/float64(total))
	if alloc > total*3/2 {
		t.Fatalf("concatenating %d KiB of batches allocated %d KiB (%.1fx); want at most 1.5x", total>>10, alloc>>10, float64(alloc)/float64(total))
	}
}
