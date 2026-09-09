package dataplane_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/internal/dataplane"
)

// TestTransitionMatrixQuadrants verifies the four quadrants with a
// deterministic 4-row fixture: one row per quadrant.
func TestTransitionMatrixQuadrants(t *testing.T) {
	alloc := memory.NewGoAllocator()

	// Schema: id, val, __before_val, __op, __pos, __commit_ts
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "__before_val", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "__op", Type: arrow.PrimitiveTypes.Uint8, Nullable: false},
		{Name: "__pos", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "__commit_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)

	bb := array.NewRecordBuilder(alloc, schema)
	defer bb.Release()

	// Row 0: before="out", after="in"  → INSERT (!before & after)
	bb.Field(0).(*array.Int64Builder).Append(1)
	bb.Field(1).(*array.StringBuilder).Append("in")
	bb.Field(2).(*array.StringBuilder).Append("out")
	bb.Field(3).(*array.Float64Builder).Append(10.0)
	bb.Field(4).(*array.BooleanBuilder).Append(true)
	bb.Field(5).(*array.Uint8Builder).Append(0) // __op = insert
	bb.Field(6).(*array.StringBuilder).Append("pos1")
	bb.Field(7).(*array.TimestampBuilder).AppendNull()

	// Row 1: before="in", after="out"  → DELETE (before & !after)
	bb.Field(0).(*array.Int64Builder).Append(2)
	bb.Field(1).(*array.StringBuilder).Append("out")
	bb.Field(2).(*array.StringBuilder).Append("in")
	bb.Field(3).(*array.Float64Builder).Append(20.0)
	bb.Field(4).(*array.BooleanBuilder).Append(true)
	bb.Field(5).(*array.Uint8Builder).Append(2) // __op = delete
	bb.Field(6).(*array.StringBuilder).Append("pos2")
	bb.Field(7).(*array.TimestampBuilder).AppendNull()

	// Row 2: before="in", after="in"   → UPDATE (before & after)
	bb.Field(0).(*array.Int64Builder).Append(3)
	bb.Field(1).(*array.StringBuilder).Append("in")
	bb.Field(2).(*array.StringBuilder).Append("in")
	bb.Field(3).(*array.Float64Builder).Append(30.0)
	bb.Field(4).(*array.BooleanBuilder).Append(true)
	bb.Field(5).(*array.Uint8Builder).Append(1) // __op = update
	bb.Field(6).(*array.StringBuilder).Append("pos3")
	bb.Field(7).(*array.TimestampBuilder).AppendNull()

	// Row 3: before="out", after="out" → DROP (!before & !after)
	bb.Field(0).(*array.Int64Builder).Append(4)
	bb.Field(1).(*array.StringBuilder).Append("out")
	bb.Field(2).(*array.StringBuilder).Append("out")
	bb.Field(3).(*array.Float64Builder).Append(40.0)
	bb.Field(4).(*array.BooleanBuilder).Append(true)
	bb.Field(5).(*array.Uint8Builder).Append(0) // __op = insert
	bb.Field(6).(*array.StringBuilder).Append("pos4")
	bb.Field(7).(*array.TimestampBuilder).AppendNull()

	rec := bb.NewRecordBatch()
	b := &dataplane.Batch{Table: "t", Record: rec, Watermark: []byte("w100")}

	beforePred := dataplane.Predicate{Column: "val", Op: "=", Value: "in", Side: dataplane.BeforeSide}
	afterPred := dataplane.Predicate{Column: "val", Op: "=", Value: "in", Side: dataplane.AfterSide}

	ins, del, upd, err := dataplane.TransitionMatrix(
		context.Background(), alloc, b,
		[]dataplane.Predicate{beforePred}, // before: __before_val == "in"
		[]dataplane.Predicate{afterPred},  // after: val == "in"
	)
	if err != nil {
		t.Fatalf("TransitionMatrix: %v", err)
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

	// Insert: row 0 only (1 row)
	if ins == nil || ins.Record.NumRows() != 1 {
		t.Errorf("inserts: got %v, want 1 row", insNumRows(ins))
	}
	// Delete: row 1 only (1 row)
	if del == nil || del.Record.NumRows() != 1 {
		t.Errorf("deletes: got %v, want 1 row", delNumRows(del))
	}
	// Update: row 2 only (1 row)
	if upd == nil || upd.Record.NumRows() != 1 {
		t.Errorf("updates: got %v, want 1 row", updNumRows(upd))
	}

	// Total: ins + del + upd = 3 (row 3 dropped)
	total := insNumRows(ins) + delNumRows(del) + updNumRows(upd)
	if total != 3 {
		t.Errorf("total rows: got %d, want 3 (drop row excluded)", total)
	}
}

// TestTransitionMatrixEquivalence verifies TransitionMatrix against a
// row-path implementation over 100 seeds.
func TestTransitionMatrixEquivalence(t *testing.T) {
	alloc := checkedAlloc(t)

	for seed := range 100 {
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{
			NumRows:   20,
			PKDomain:  5,
			Allocator: alloc,
		})

		// Before predicate: __before_val == "hello" (Side: BeforeSide)
		// After predicate: val == "hello" (Side: AfterSide, default)
		beforePred := dataplane.Predicate{Column: "val", Op: "=", Value: "hello", Side: dataplane.BeforeSide}
		afterPred := dataplane.Predicate{Column: "val", Op: "=", Value: "hello", Side: dataplane.AfterSide}

		ins, del, upd, err := dataplane.TransitionMatrix(
			context.Background(), alloc, b,
			[]dataplane.Predicate{beforePred},
			[]dataplane.Predicate{afterPred},
		)
		if err != nil {
			b.Release()
			t.Fatalf("seed %d: TransitionMatrix: %v", seed, err)
		}

		// Row-path: classify each row by before/after coalesce
		beforeCol := b.Record.Column(colIndexByName(b.Record.Schema(), "__before_val"))
		afterCol := b.Record.Column(colIndexByName(b.Record.Schema(), "val"))
		var rowIns, rowDel, rowUpd []eqChange
		for i := range int(b.Record.NumRows()) {
			bPass := coalesceStr(beforeCol, i) == "hello"
			aPass := coalesceStr(afterCol, i) == "hello"
			ec := eqChange{
				ID:  b.Record.Column(0).(*array.Int64).Value(i),
				Val: afterCol.(*array.String).Value(i),
				Op:  b.Record.Column(colIndexByName(b.Record.Schema(), "__op")).(*array.Uint8).Value(i),
			}
			switch {
			case !bPass && aPass:
				rowIns = append(rowIns, ec)
			case bPass && !aPass:
				rowDel = append(rowDel, ec)
			case bPass && aPass:
				rowUpd = append(rowUpd, ec)
			}
		}

		insSlice := batchToRowSlice(ins)
		delSlice := batchToRowSlice(del)
		updSlice := batchToRowSlice(upd)

		if !sliceEqual(rowIns, insSlice) {
			t.Errorf("seed %d: inserts mismatch: row=%d, col=%d", seed, len(rowIns), len(insSlice))
		}
		if !sliceEqual(rowDel, delSlice) {
			t.Errorf("seed %d: deletes mismatch: row=%d, col=%d", seed, len(rowDel), len(delSlice))
		}
		if !sliceEqual(rowUpd, updSlice) {
			t.Errorf("seed %d: updates mismatch: row=%d, col=%d", seed, len(rowUpd), len(updSlice))
		}

		b.Release()
		if ins != nil {
			ins.Release()
		}
		if del != nil {
			del.Release()
		}
		if upd != nil {
			upd.Release()
		}
	}
}

// TestTransitionMatrixMissingBeforeColumn verifies that a BeforeSide
// predicate on a missing __before_ column returns an error.
func TestTransitionMatrixMissingBeforeColumn(t *testing.T) {
	alloc := memory.NewGoAllocator()
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 5, Allocator: alloc})
	defer b.Release()

	beforePred := dataplane.Predicate{Column: "nonexistent", Op: "=", Value: "x", Side: dataplane.BeforeSide}
	afterPred := dataplane.Predicate{Column: "val", Op: "=", Value: "hello"}

	_, _, _, err := dataplane.TransitionMatrix(
		context.Background(), alloc, b,
		[]dataplane.Predicate{beforePred},
		[]dataplane.Predicate{afterPred},
	)
	if err == nil {
		t.Fatal("expected error for missing __before_nonexistent column")
	}
}

// TestTransitionMatrixNullsCoalesce verifies that null before/after
// values coalesce to false (same as predicate evaluation).
func TestTransitionMatrixNullsCoalesce(t *testing.T) {
	alloc := memory.NewGoAllocator()
	b := dataplane.AdversarialNullBefore(alloc)
	defer b.Release()

	// BeforeSide predicate: __before_val == "hello"
	// Row 0 has null __before_val → coalesce → false → !before
	// Row 1 has __before_val = "hello" → true → before
	beforePred := dataplane.Predicate{Column: "val", Op: "=", Value: "hello", Side: dataplane.BeforeSide}
	afterPred := dataplane.Predicate{Column: "val", Op: "=", Value: "hello"}

	ins, del, upd, err := dataplane.TransitionMatrix(
		context.Background(), alloc, b,
		[]dataplane.Predicate{beforePred},
		[]dataplane.Predicate{afterPred},
	)
	if err != nil {
		t.Fatalf("TransitionMatrix: %v", err)
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

	// Row 0: before=false (null), after=(val == "hello") → depends on val
	// Row 1: before=true (val == "hello"), after=(val == "hello") → update
	total := insNumRows(ins) + delNumRows(del) + updNumRows(upd)
	if total < 1 {
		t.Errorf("expected at least 1 row in output, got %d", total)
	}
}

// TestTransitionMatrixEmptyPreds verifies that empty predicate lists
// pass everything through — rows are classified by __op only.
func TestTransitionMatrixEmptyPreds(t *testing.T) {
	alloc := memory.NewGoAllocator()
	b := dataplane.GenerateBatch(1, dataplane.GeneratorOpts{NumRows: 10, Allocator: alloc})
	defer b.Release()

	ins, del, upd, err := dataplane.TransitionMatrix(
		context.Background(), alloc, b,
		nil, // empty before
		nil, // empty after
	)
	if err != nil {
		t.Fatalf("TransitionMatrix: %v", err)
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

	// Empty preds → all pass → __op determines buckets. The per-bucket
	// counts must match the source batch's per-op distribution exactly —
	// a total-only assert would hide rows silently vanishing from one
	// bucket while another over-counts.
	var wantIns, wantDel, wantUpd int
	opIdx := -1
	for i := range b.Record.Schema().NumFields() {
		if b.Record.Schema().Field(i).Name == "__op" {
			opIdx = i
			break
		}
	}
	if opIdx < 0 {
		t.Fatal("__op column not found")
	}
	opCol := b.Record.Column(opIdx).(*array.Uint8)
	for i := range opCol.Len() {
		switch opCol.Value(i) {
		case uint8(dataplane.OpInsert):
			wantIns++
		case uint8(dataplane.OpDelete):
			wantDel++
		case uint8(dataplane.OpUpdate):
			wantUpd++
		}
	}
	if got := insNumRows(ins); got != wantIns {
		t.Errorf("inserts = %d, want %d (per-op distribution)", got, wantIns)
	}
	if got := delNumRows(del); got != wantDel {
		t.Errorf("deletes = %d, want %d (per-op distribution)", got, wantDel)
	}
	if got := updNumRows(upd); got != wantUpd {
		t.Errorf("updates = %d, want %d (per-op distribution)", got, wantUpd)
	}
}

// ── helpers ────────────────────────────────────────────────────────

func insNumRows(b *dataplane.Batch) int {
	if b == nil {
		return 0
	}
	return int(b.Record.NumRows())
}

func delNumRows(b *dataplane.Batch) int {
	if b == nil {
		return 0
	}
	return int(b.Record.NumRows())
}

func updNumRows(b *dataplane.Batch) int {
	if b == nil {
		return 0
	}
	return int(b.Record.NumRows())
}

func colIndexByName(s *arrow.Schema, name string) int {
	for i := range s.NumFields() {
		if s.Field(i).Name == name {
			return i
		}
	}
	return -1
}

func coalesceStr(col arrow.Array, i int) string {
	if col.IsNull(i) {
		return ""
	}
	return col.(*array.String).Value(i)
}
