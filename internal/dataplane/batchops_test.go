package dataplane_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
)

// MergeBatches must propagate Mode on all three paths (a-only, b-only,
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
		got, err := dpint.MergeBatches(mk(mode), nil, nil)
		if err != nil || got.Mode != mode {
			t.Fatalf("a-only mode = %v, want %v (err %v)", got.Mode, mode, err)
		}
		got.Release()
		// b-only
		got, err = dpint.MergeBatches(nil, mk(mode), nil)
		if err != nil || got.Mode != mode {
			t.Fatalf("b-only mode = %v, want %v (err %v)", got.Mode, mode, err)
		}
		got.Release()
		// concat
		got, err = dpint.MergeBatches(mk(mode), mk(mode), nil)
		if err != nil || got.Mode != mode {
			t.Fatalf("concat mode = %v, want %v (err %v)", got.Mode, mode, err)
		}
		got.Release()
	}
}

func TestCountOpsNilBatch(t *testing.T) {
	u, d := dpint.CountOps(nil)
	if u != 0 || d != 0 {
		t.Errorf("CountOps(nil) = %d, %d, want 0, 0", u, d)
	}
}

// The batch operations own what they return and leave their inputs to the
// caller: under the checked allocator, releasing inputs and outputs frees
// everything.
func TestBatchOpsReleaseEverything(t *testing.T) {
	alloc := checkedAlloc(t)
	gen := func() *dataplane.Batch {
		b := dpint.GenerateBatch(4, dpint.GeneratorOpts{NumRows: 4, Allocator: alloc})
		b.Mode = dataplane.UpsertMode
		return b
	}
	a, b := gen(), gen()
	defer a.Release()
	defer b.Release()

	merged, err := dpint.MergeBatches(a, b, alloc)
	if err != nil || merged.Record.NumRows() != 8 {
		t.Fatalf("MergeBatches: %v rows, %v", merged, err)
	}
	merged.Release()

	cat, err := dpint.ConcatBatches([]*dataplane.Batch{a, b, a})
	if err != nil || cat.Record.NumRows() != 12 {
		t.Fatalf("ConcatBatches: %v", err)
	}
	cat.Release()

	sel, err := dpint.SelectRows(a, []int32{3, 0}, "p9", dataplane.AppendMode, "", nil)
	if err != nil || sel.Record.NumRows() != 2 || string(sel.Watermark) != "p9" || sel.Mode != dataplane.AppendMode {
		t.Fatalf("SelectRows: %+v, %v", sel, err)
	}
	sel.Release()

	e := dpint.EmptyBatch(a, dataplane.AppendMode)
	if e.Record.NumRows() != 0 || e.Mode != dataplane.AppendMode {
		t.Fatalf("EmptyBatch: %+v", e)
	}
	e.Release()

	if got := dpint.LastRowPos(a); got == "" {
		t.Fatal("LastRowPos of a non-empty batch is empty")
	}
	if got := dpint.LastRowPos(e); got != "" {
		t.Fatalf("LastRowPos of an empty batch = %q", got)
	}
}

// Batches of different schemas are refused, never concatenated misaligned.
func TestConcatBatchesRejectsASchemaMismatch(t *testing.T) {
	a := dpint.GenerateBatch(1, dpint.GeneratorOpts{NumRows: 1})
	defer a.Release()
	rec := a.Record
	narrow := array.NewRecordBatch(arrow.NewSchema(rec.Schema().Fields()[1:], nil), rec.Columns()[1:], rec.NumRows())
	b := &dataplane.Batch{Table: a.Table, Record: narrow}
	defer b.Release()
	if _, err := dpint.ConcatBatches([]*dataplane.Batch{a, b}); err == nil {
		t.Fatal("concatenated batches of different schemas")
	}
}
