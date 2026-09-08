package dataplane_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/internal/dataplane"
)

func TestSplitByOp(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 20})
	defer b.Release()

	ins, del, upd, err := dataplane.SplitByOp(context.Background(), b)
	if err != nil {
		t.Fatalf("SplitByOp: %v", err)
	}

	total := 0
	if ins != nil {
		defer ins.Release()
		total += int(ins.Record.NumRows())
	}
	if del != nil {
		defer del.Release()
		total += int(del.Record.NumRows())
	}
	if upd != nil {
		defer upd.Release()
		total += int(upd.Record.NumRows())
	}

	if total != int(b.Record.NumRows()) {
		t.Errorf("total rows %d != batch rows %d", total, b.Record.NumRows())
	}

	_ = alloc // leak check via t.Cleanup
}

func TestSplitByOpWatermarkPreserved(t *testing.T) {
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 10})
	defer b.Release()

	ins, del, upd, err := dataplane.SplitByOp(context.Background(), b)
	if err != nil {
		t.Fatalf("SplitByOp: %v", err)
	}

	for _, out := range []*dataplane.Batch{ins, del, upd} {
		if out != nil {
			defer out.Release()
			if string(out.Watermark) != string(b.Watermark) {
				t.Errorf("Watermark changed: %q -> %q", b.Watermark, out.Watermark)
			}
			if out.Table != b.Table {
				t.Errorf("Table changed: %q -> %q", b.Table, out.Table)
			}
		}
	}
}

func TestSplitByOpEachRowClassified(t *testing.T) {
	b := dataplane.GenerateBatch(7, dataplane.GeneratorOpts{NumRows: 30})
	defer b.Release()

	ins, del, upd, err := dataplane.SplitByOp(context.Background(), b)
	if err != nil {
		t.Fatalf("SplitByOp: %v", err)
	}

	opIdx := -1
	for i := range b.Record.Schema().NumFields() {
		if b.Record.Schema().Field(i).Name == "__op" {
			opIdx = i
			break
		}
	}
	opCol := b.Record.Column(opIdx).(*array.Uint8)

	insCount, delCount, updCount := 0, 0, 0
	for i := range int(opCol.Len()) {
		switch opCol.Value(i) {
		case 0:
			insCount++
		case 2:
			delCount++
		case 1:
			updCount++
		}
	}

	if ins != nil && int(ins.Record.NumRows()) != insCount {
		t.Errorf("inserts: got %d, want %d", ins.Record.NumRows(), insCount)
	}
	if del != nil && int(del.Record.NumRows()) != delCount {
		t.Errorf("deletes: got %d, want %d", del.Record.NumRows(), delCount)
	}
	if upd != nil && int(upd.Record.NumRows()) != updCount {
		t.Errorf("updates: got %d, want %d", upd.Record.NumRows(), updCount)
	}
}

func TestEvaluatePredicateEqual(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 20})
	defer b.Release()

	// Find an existing id value
	idCol := b.Record.Column(0).(*array.Int64)
	target := idCol.Value(5)

	mask, err := dataplane.EvaluatePredicate(context.Background(), b, dataplane.Predicate{
		Column: "id",
		Op:     "=",
		Value:  target,
	})
	if err != nil {
		t.Fatalf("EvaluatePredicate: %v", err)
	}
	defer mask.Release()

	boolMask := mask.(*array.Boolean)
	count := 0
	for i := range boolMask.Len() {
		if boolMask.Value(i) {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 match for id=%d, got %d", target, count)
	}
}

func TestEvaluatePredicateNullCoalesce(t *testing.T) {
	b := dataplane.AdversarialNullBefore(0)
	defer b.Release()

	// val column: row 0 is null, row 1 is "hello"
	mask, err := dataplane.EvaluatePredicate(context.Background(), b, dataplane.Predicate{
		Column: "val",
		Op:     "=",
		Value:  "hello",
	})
	if err != nil {
		t.Fatalf("EvaluatePredicate: %v", err)
	}
	defer mask.Release()

	boolMask := mask.(*array.Boolean)
	// null row → false (coalesce), "hello" row → true
	if boolMask.Value(0) {
		t.Error("null row should be false (coalesce)")
	}
	if !boolMask.Value(1) {
		t.Error("matching row should be true")
	}
}

func TestFilterWithMask(t *testing.T) {
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 10})
	defer b.Release()

	// Build mask: keep first 5 rows
	mask, err := dataplane.EvaluatePredicate(context.Background(), b, dataplane.Predicate{
		Column: "id",
		Op:     "=",
		Value:  int64(1),
	})
	if err != nil {
		t.Fatalf("EvaluatePredicate: %v", err)
	}
	defer mask.Release()

	ins, del, upd, err := dataplane.Filter(context.Background(), b, mask)
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	defer func() {
		if ins != nil {
			ins.Release()
		}
		if del != nil {
			del.Release()
		}
		if upd != nil {
			upd.Release()
		}
	}()

	total := 0
	if ins != nil {
		total += int(ins.Record.NumRows())
	}
	if del != nil {
		total += int(del.Record.NumRows())
	}
	if upd != nil {
		total += int(upd.Record.NumRows())
	}
	if total != 1 {
		t.Errorf("expected 1 row after filter, got %d", total)
	}
}

func TestFilterNilBatch(t *testing.T) {
	b := &dataplane.Batch{Table: "t", Watermark: []byte("w")}
	ins, del, upd, err := dataplane.Filter(context.Background(), b, nil)
	if err != nil {
		t.Fatalf("Filter nil batch: %v", err)
	}
	if ins != nil || del != nil || upd != nil {
		t.Error("expected nil outputs for nil batch")
	}
}
