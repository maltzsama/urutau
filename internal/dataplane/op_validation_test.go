package dataplane_test

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

	"github.com/maltzsama/urutau/internal/dataplane"
)

func TestCollapseRejectsUnknownOp(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 2, Allocator: alloc})
	defer b.Release()

	bad := withOpValues(t, b, 0, 3)
	defer bad.Release()

	if _, _, err := dataplane.Collapse(context.Background(), alloc, bad, []string{"id"}); err == nil {
		t.Error("Collapse: unknown op accepted")
	}
}

// The poison sits in row 0, which loses to row 1 whenever the two share a key.
// Validating after Take would drop it silently; validating before catches it
// either way.
func TestCollapseRejectsUnknownOpInLosingRow(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 2, Allocator: alloc})
	defer b.Release()

	bad := withOpValues(t, b, 3, 0)
	defer bad.Release()

	if _, _, err := dataplane.Collapse(context.Background(), alloc, bad, []string{"id"}); err == nil {
		t.Error("Collapse: poison op in potentially-losing row accepted")
	}
}

// Every valid op must still pass — the guard rejects the unknown, not the known.
func TestCollapseAcceptsEveryValidOp(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.GenerateBatch(42, dataplane.GeneratorOpts{NumRows: 3, Allocator: alloc})
	defer b.Release()

	ok := withOpValues(t, b, 0, 1, 2) // insert, update, delete
	defer ok.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), alloc, ok, []string{"id"})
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
func withOpValues(t *testing.T, b *dataplane.Batch, ops ...uint8) *dataplane.Batch {
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
	return &dataplane.Batch{Table: b.Table, Record: newRec, Watermark: b.Watermark}
}
