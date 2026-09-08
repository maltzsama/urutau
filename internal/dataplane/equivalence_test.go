package dataplane_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/internal/dataplane"
)

// eqChange mirrors a row for order-sensitive comparison.
type eqChange struct {
	ID  int64
	Val string
	Op  uint8
}

func batchToRowSet(b *dataplane.Batch) map[eqChange]bool {
	set := make(map[eqChange]bool)
	if b == nil || b.Record == nil {
		return set
	}
	idCol := b.Record.Column(0).(*array.Int64)
	valCol := b.Record.Column(1).(*array.String)
	opCol := b.Record.Column(4).(*array.Uint8)
	for i := range int(idCol.Len()) {
		set[eqChange{
			ID:  idCol.Value(i),
			Val: valCol.Value(i),
			Op:  opCol.Value(i),
		}] = true
	}
	return set
}

// TestEquivalence_SplitByOp_MatchesRowPath verifies that SplitByOp
// produces the same row-level classification as iterating the batch
// row-by-row and checking __op.
func TestEquivalence_SplitByOp_MatchesRowPath(t *testing.T) {
	alloc := checkedAlloc(t)

	for seed := range 100 {
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{NumRows: 20})
		defer b.Release()

		// Row-path classification
		opCol := b.Record.Column(4).(*array.Uint8)
		idCol := b.Record.Column(0).(*array.Int64)
		valCol := b.Record.Column(1).(*array.String)

		var rowIns, rowDel, rowUpd []eqChange
		for i := range opCol.Len() {
			ec := eqChange{
				ID:  idCol.Value(i),
				Val: valCol.Value(i),
				Op:  opCol.Value(i),
			}
			switch opCol.Value(i) {
			case 0:
				rowIns = append(rowIns, ec)
			case 2:
				rowDel = append(rowDel, ec)
			case 1:
				rowUpd = append(rowUpd, ec)
			}
		}

		// Columnar path
		colIns, colDel, colUpd, err := dataplane.SplitByOp(context.Background(), b)
		if err != nil {
			t.Fatalf("seed %d: SplitByOp: %v", seed, err)
		}
		colInsSet := batchToRowSet(colIns)
		colDelSet := batchToRowSet(colDel)
		colUpdSet := batchToRowSet(colUpd)

		if len(rowIns) != len(colInsSet) {
			t.Errorf("seed %d: inserts: row=%d, col=%d", seed, len(rowIns), len(colInsSet))
		}
		for _, ec := range rowIns {
			if !colInsSet[ec] {
				t.Errorf("seed %d: row insert %v not in columnar set", seed, ec)
			}
		}
		if len(rowDel) != len(colDelSet) {
			t.Errorf("seed %d: deletes: row=%d, col=%d", seed, len(rowDel), len(colDelSet))
		}
		for _, ec := range rowDel {
			if !colDelSet[ec] {
				t.Errorf("seed %d: row delete %v not in columnar set", seed, ec)
			}
		}
		if len(rowUpd) != len(colUpdSet) {
			t.Errorf("seed %d: updates: row=%d, col=%d", seed, len(rowUpd), len(colUpdSet))
		}
		for _, ec := range rowUpd {
			if !colUpdSet[ec] {
				t.Errorf("seed %d: row update %v not in columnar set", seed, ec)
			}
		}
	}
	_ = alloc
}

// TestEquivalence_Collapse_MatchesRowPath verifies that Collapse
// produces the same surviving rows as a row-by-row last-occurrence map.
func TestEquivalence_Collapse_MatchesRowPath(t *testing.T) {
	alloc := checkedAlloc(t)

	for seed := range 100 {
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{NumRows: 30})
		defer b.Release()

		idCol := b.Record.Column(0).(*array.Int64)
		valCol := b.Record.Column(1).(*array.String)
		opCol := b.Record.Column(4).(*array.Uint8)

		// Row-path collapse: last occurrence per PK wins
		type rowInfo struct {
			id  int64
			val string
			op  uint8
		}
		groupOrder := make(map[int64]bool)
		var groupOrderList []int64
		lastSeen := make(map[int64]rowInfo)

		for i := range int(idCol.Len()) {
			k := idCol.Value(i)
			r := rowInfo{id: idCol.Value(i), val: valCol.Value(i), op: opCol.Value(i)}
			if !groupOrder[k] {
				groupOrder[k] = true
				groupOrderList = append(groupOrderList, k)
			}
			lastSeen[k] = r
		}

		var rowUps, rowDel []eqChange
		for _, k := range groupOrderList {
			r := lastSeen[k]
			ec := eqChange{ID: r.id, Val: r.val, Op: r.op}
			if r.op == 2 {
				rowDel = append(rowDel, ec)
			} else {
				rowUps = append(rowUps, ec)
			}
		}

		// Columnar path
		colUps, colDel, err := dataplane.Collapse(context.Background(), b, []string{"id"})
		if err != nil {
			t.Fatalf("seed %d: Collapse: %v", seed, err)
		}
		colUpsSet := batchToRowSet(colUps)
		colDelSet := batchToRowSet(colDel)

		if len(rowUps) != len(colUpsSet) {
			t.Errorf("seed %d: upserts: row=%d, col=%d", seed, len(rowUps), len(colUpsSet))
		}
		for _, ec := range rowUps {
			if !colUpsSet[ec] {
				t.Errorf("seed %d: row upsert %v not in columnar set", seed, ec)
			}
		}
		if len(rowDel) != len(colDelSet) {
			t.Errorf("seed %d: deletes: row=%d, col=%d", seed, len(rowDel), len(colDelSet))
		}
		for _, ec := range rowDel {
			if !colDelSet[ec] {
				t.Errorf("seed %d: row delete %v not in columnar set", seed, ec)
			}
		}
	}
	_ = alloc
}

// TestEquivalence_FilterPredicate_MatchesRowPath verifies that
// EvaluatePredicate matches row-by-row filtering.
func TestEquivalence_FilterPredicate_MatchesRowPath(t *testing.T) {
	alloc := checkedAlloc(t)

	for seed := range 100 {
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{NumRows: 25})
		defer b.Release()

		idCol := b.Record.Column(0).(*array.Int64)
		valCol := b.Record.Column(1).(*array.String)
		opCol := b.Record.Column(4).(*array.Uint8)

		// Pick a predicate value from the data
		target := idCol.Value(seed % int(idCol.Len()))

		// Row-path: filter by id == target
		var rowSet []eqChange
		for i := range int(idCol.Len()) {
			if idCol.Value(i) == target {
				rowSet = append(rowSet, eqChange{
					ID:  idCol.Value(i),
					Val: valCol.Value(i),
					Op:  opCol.Value(i),
				})
			}
		}

		// Columnar path
		mask, err := dataplane.EvaluatePredicate(context.Background(), b, dataplane.Predicate{
			Column: "id",
			Op:     "=",
			Value:  target,
		})
		if err != nil {
			t.Fatalf("seed %d: EvaluatePredicate: %v", seed, err)
		}
		defer mask.Release()

		ins, del, upd, err := dataplane.Filter(context.Background(), b, mask)
		if err != nil {
			t.Fatalf("seed %d: Filter: %v", seed, err)
		}
		colSet := batchToRowSet(ins)
		for k := range batchToRowSet(del) {
			colSet[k] = true
		}
		for k := range batchToRowSet(upd) {
			colSet[k] = true
		}

		if len(rowSet) != len(colSet) {
			t.Errorf("seed %d: filtered: row=%d, col=%d (target=%d)", seed, len(rowSet), len(colSet), target)
		}
		for _, ec := range rowSet {
			if !colSet[ec] {
				t.Errorf("seed %d: row filtered %v not in columnar set", seed, ec)
			}
		}
	}
	_ = alloc
}

// TestEquivalence_CollapseInsertAfterDelete verifies that an
// insert-after-delete ends up as an upsert (not a delete).
func TestEquivalence_CollapseInsertAfterDelete(t *testing.T) {
	b := dataplane.AdversarialInsertAfterDelete(0)
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()

	if ups == nil || ups.Record.NumRows() != 1 {
		t.Errorf("expected 1 upsert, got %v", ups)
	}
	if dels != nil && dels.Record.NumRows() > 0 {
		t.Errorf("expected 0 deletes, got %d", dels.Record.NumRows())
	}
	if ups != nil {
		// Find __op by name
		opIdx := -1
		for i := range ups.Record.Schema().NumFields() {
			if ups.Record.Schema().Field(i).Name == "__op" {
				opIdx = i
				break
			}
		}
		if opIdx < 0 {
			t.Fatal("__op not found in upsert schema")
		}
		opCol := ups.Record.Column(opIdx).(*array.Uint8)
		if opCol.Value(0) != 0 {
			t.Errorf("expected op=insert (0), got %d", opCol.Value(0))
		}
	}
}

// TestEquivalence_DeleteLast verifies that a PK whose last operation
// is DELETE ends up in deletes, not upserts.
func TestEquivalence_DeleteLast(t *testing.T) {
	b := dataplane.AdversarialDeleteLast(0)
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
		if dels != nil {
			dels.Release()
		}
	}()

	if ups != nil && ups.Record.NumRows() > 0 {
		t.Errorf("expected no upserts for delete-last, got %d", ups.Record.NumRows())
	}
	if dels == nil || dels.Record.NumRows() != 1 {
		t.Errorf("expected 1 delete, got %v", dels)
	}
}

// TestEquivalence_CompositeKey verifies that two distinct composite
// PKs survive collapse as separate rows.
func TestEquivalence_CompositeKey(t *testing.T) {
	b := dataplane.AdversarialCompositeKey(0)
	defer b.Release()

	ups, _, err := dataplane.Collapse(context.Background(), b, []string{"pk1", "pk2"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
	}()

	total := 0
	if ups != nil {
		total = int(ups.Record.NumRows())
	}
	if total != 2 {
		t.Errorf("expected 2 upserts for 2 distinct composite keys, got %d", total)
	}
}

// TestEquivalence_Int64Overflow verifies that int64 values outside
// float64 precision survive with exact values.
func TestEquivalence_Int64Overflow(t *testing.T) {
	b := dataplane.AdversarialInt64Overflow(0)
	defer b.Release()

	ups, _, err := dataplane.Collapse(context.Background(), b, []string{"id"})
	if err != nil {
		t.Fatalf("Collapse: %v", err)
	}
	defer func() {
		if ups != nil {
			ups.Release()
		}
	}()

	if ups == nil || ups.Record.NumRows() != 2 {
		t.Fatalf("expected 2 upserts, got %v", ups)
	}

	// Check the 'big' column (index 1) for exact overflow preservation
	bigCol := ups.Record.Column(1).(*array.Int64)
	got := map[int64]bool{
		bigCol.Value(0): true,
		bigCol.Value(1): true,
	}
	want1 := (int64(1) << 53) + 1
	want2 := int64(math.MaxInt64)

	if !got[want1] {
		t.Errorf("missing int64 value %d", want1)
	}
	if !got[want2] {
		t.Errorf("missing int64 overflow value %d", want2)
	}
}
