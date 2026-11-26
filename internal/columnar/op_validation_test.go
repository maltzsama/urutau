package columnar_test

// T-11: a batch carrying an unknown __op value (3) must be rejected by the
// op-consuming stage. An unknown op that slipped through would vanish
// silently: no mask matches it, so the row never reaches the sink.
//
// Collapse is the only such stage left — the row path (SplitByOp, Filter,
// TransitionMatrix) was removed as dead code. Validation runs against the
// ORIGINAL batch, before Take, so a poison op is caught even when it sits in
// a row that loses its group and would never survive the collapse.

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/internal/columnar"
	"github.com/maltzsama/urutau/internal/columnar/columnartest"
)

func TestCollapseRejectsUnknownOp(t *testing.T) {
	alloc := checkedAlloc(t)
	b := columnartest.GenerateBatch(42, columnartest.GeneratorOpts{NumRows: 2, Allocator: alloc})
	defer b.Release()

	bad := withOpValues(t, b, 0, 3)
	defer bad.Release()

	if _, _, err := columnar.Collapse(context.Background(), alloc, bad, []string{"id"}); err == nil {
		t.Error("Collapse: unknown op accepted")
	}
}

// The poison sits in row 0, which loses to row 1 whenever the two share a key.
// Validating after Take would drop it silently; validating before catches it
// either way.
func TestCollapseRejectsUnknownOpInLosingRow(t *testing.T) {
	alloc := checkedAlloc(t)
	b := columnartest.GenerateBatch(42, columnartest.GeneratorOpts{NumRows: 2, Allocator: alloc})
	defer b.Release()

	bad := withOpValues(t, b, 3, 0)
	defer bad.Release()

	if _, _, err := columnar.Collapse(context.Background(), alloc, bad, []string{"id"}); err == nil {
		t.Error("Collapse: poison op in potentially-losing row accepted")
	}
}

// Every valid op must still pass — the guard rejects the unknown, not the known.
func TestCollapseAcceptsEveryValidOp(t *testing.T) {
	alloc := checkedAlloc(t)
	b := columnartest.GenerateBatch(42, columnartest.GeneratorOpts{NumRows: 3, Allocator: alloc})
	defer b.Release()

	ok := withOpValues(t, b, 0, 1, 2) // insert, update, delete
	defer ok.Release()

	ups, dels, err := columnar.Collapse(context.Background(), alloc, ok, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse rejected a batch of valid ops: %v", err)
	}
	if ups != nil {
		ups.Release()
	}
	if dels != nil {
		dels.Release()
	}
}

// withOpValues returns a new Batch (caller releases) with the __op column
// replaced by the given values. Record immutability forbids mutation, so the
// record is rebuilt sharing every other column.
func withOpValues(t *testing.T, b *columnar.Batch, ops ...uint8) *columnar.Batch {
	t.Helper()
	rec := b.Record
	opIdx := -1
	for i := range rec.Schema().NumFields() {
		if rec.Schema().Field(i).Name == "__op" {
			opIdx = i
			break
		}
	}
	if opIdx < 0 {
		t.Fatal("__op column not found")
	}

	opBld := array.NewUint8Builder(checkedAlloc(t))
	defer opBld.Release()
	for _, op := range ops {
		opBld.Append(op)
	}
	opArr := opBld.NewUint8Array()
	defer opArr.Release()

	cols := make([]arrow.Array, rec.NumCols())
	for i := range int(rec.NumCols()) {
		if i == opIdx {
			opArr.Retain()
			cols[i] = opArr
		} else {
			rec.Column(i).Retain()
			cols[i] = rec.Column(i)
		}
	}
	newRec := array.NewRecordBatch(rec.Schema(), cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return &columnar.Batch{Table: b.Table, Record: newRec, Watermark: b.Watermark}
}

// __op is NOT NULL by contract (dataplane.Batch). array.Uint8.Value reads a
// null as 0, i.e. OpInsert, so validation passes; after Take the null is
// compared by the masks, the comparison yields null, and the row is filtered
// out of both upserts and deletes. A contract violation must be an error, not
// silent row loss.
func TestCollapseRejectsNullOp(t *testing.T) {
	alloc := checkedAlloc(t)
	b := columnartest.GenerateBatch(42, columnartest.GeneratorOpts{NumRows: 1, Allocator: alloc})
	defer b.Release()

	bad := withNullOp(t, b, 0)
	defer bad.Release()

	ups, dels, err := columnar.Collapse(context.Background(), alloc, bad, []string{"id"})
	if err == nil {
		n := 0
		if ups != nil {
			n += int(ups.Record.NumRows())
			ups.Release()
		}
		if dels != nil {
			n += int(dels.Record.NumRows())
			dels.Release()
		}
		t.Fatalf("Collapse: null __op accepted; %d of 1 input rows survived", n)
	}
}

// withNullOp returns a new Batch (caller releases) with __op[row] set to null.
func withNullOp(t *testing.T, b *columnar.Batch, row int) *columnar.Batch {
	t.Helper()
	rec := b.Record
	opIdx := -1
	for i := range rec.Schema().NumFields() {
		if rec.Schema().Field(i).Name == "__op" {
			opIdx = i
			break
		}
	}
	if opIdx < 0 {
		t.Fatal("__op column not found")
	}
	src, ok := rec.Column(opIdx).(*array.Uint8)
	if !ok {
		t.Fatalf("__op column type %T, want *array.Uint8", rec.Column(opIdx))
	}

	opBld := array.NewUint8Builder(checkedAlloc(t))
	defer opBld.Release()
	for i := range int(rec.NumRows()) {
		if i == row {
			opBld.AppendNull()
			continue
		}
		opBld.Append(src.Value(i))
	}
	opArr := opBld.NewUint8Array()
	defer opArr.Release()

	cols := make([]arrow.Array, rec.NumCols())
	for i := range int(rec.NumCols()) {
		if i == opIdx {
			opArr.Retain()
			cols[i] = opArr
		} else {
			rec.Column(i).Retain()
			cols[i] = rec.Column(i)
		}
	}
	newRec := array.NewRecordBatch(rec.Schema(), cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return &columnar.Batch{Table: b.Table, Record: newRec, Watermark: b.Watermark}
}
