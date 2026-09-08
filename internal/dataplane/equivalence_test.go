package dataplane_test

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/internal/dataplane"
)

// eqChange mirrors a row for order-sensitive comparison.
type eqChange struct {
	ID  int64
	Val string
	Op  uint8
}

// batchToRowSlice extracts rows in order from a batch.
func batchToRowSlice(b *dataplane.Batch) []eqChange {
	if b == nil || b.Record == nil {
		return nil
	}
	idCol := b.Record.Column(0).(*array.Int64)
	valCol := b.Record.Column(1).(*array.String)
	opCol := extractOpCol(b)
	if opCol == nil {
		return nil
	}
	rows := make([]eqChange, int(idCol.Len()))
	for i := range int(idCol.Len()) {
		rows[i] = eqChange{
			ID:  idCol.Value(i),
			Val: valCol.Value(i),
			Op:  opCol[i],
		}
	}
	return rows
}

// extractOpCol extracts the __op column values as a slice.
func extractOpCol(b *dataplane.Batch) []uint8 {
	if b == nil || b.Record == nil {
		return nil
	}
	for i := range b.Record.Schema().NumFields() {
		if b.Record.Schema().Field(i).Name == "__op" {
			col := b.Record.Column(i).(*array.Uint8)
			vals := make([]uint8, col.Len())
			for j := range col.Len() {
				vals[j] = col.Value(j)
			}
			return vals
		}
	}
	return nil
}

// TestEquivalence_SplitByOp_MatchesRowPath verifies that SplitByOp
// produces the same row-level classification as iterating the batch
// row-by-row and checking __op.
func TestEquivalence_SplitByOp_MatchesRowPath(t *testing.T) {
	alloc := checkedAlloc(t)

	for seed := range 100 {
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{NumRows: 20, Allocator: alloc})
		defer b.Release()

		// Row-path: classify by __op in iteration order
		opVals := extractOpCol(b)
		idCol := b.Record.Column(0).(*array.Int64)
		valCol := b.Record.Column(1).(*array.String)

		var rowIns, rowDel, rowUpd []eqChange
		for i := range opVals {
			ec := eqChange{
				ID:  idCol.Value(i),
				Val: valCol.Value(i),
				Op:  opVals[i],
			}
			switch opVals[i] {
			case 0:
				rowIns = append(rowIns, ec)
			case 2:
				rowDel = append(rowDel, ec)
			case 1:
				rowUpd = append(rowUpd, ec)
			}
		}

		// Columnar path
		colIns, colDel, colUpd, err := dataplane.SplitByOp(context.Background(), alloc, b)
		if err != nil {
			t.Fatalf("seed %d: SplitByOp: %v", seed, err)
		}
		colInsSlice := batchToRowSlice(colIns)
		colDelSlice := batchToRowSlice(colDel)
		colUpdSlice := batchToRowSlice(colUpd)

		// Order-sensitive comparison (slice, not set)
		if !sliceEqual(rowIns, colInsSlice) {
			t.Errorf("seed %d: inserts mismatch: row=%d, col=%d", seed, len(rowIns), len(colInsSlice))
		}
		if !sliceEqual(rowDel, colDelSlice) {
			t.Errorf("seed %d: deletes mismatch: row=%d, col=%d", seed, len(rowDel), len(colDelSlice))
		}
		if !sliceEqual(rowUpd, colUpdSlice) {
			t.Errorf("seed %d: updates mismatch: row=%d, col=%d", seed, len(rowUpd), len(colUpdSlice))
		}
	}
}

// TestEquivalence_Collapse_MatchesChangeCollapse verifies that the
// columnar Collapse produces the same result as change.Collapse (the
// production row-side reference).
func TestEquivalence_Collapse_MatchesChangeCollapse(t *testing.T) {
	alloc := checkedAlloc(t)

	for seed := range 100 {
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{
			NumRows:   30,
			PKDomain:  10, // force duplicate PKs
			Allocator: alloc,
		})
		defer b.Release()

		// Row-path reference: change.Collapse (production code)
		changes := batchToChanges(b)
		rowCollapsed := change.Collapse(changes)

		// Columnar path
		colUps, colDel, err := dataplane.Collapse(context.Background(), alloc, b, []string{"id"})
		if err != nil {
			t.Fatalf("seed %d: Collapse: %v", seed, err)
		}
		colUpsSlice := batchToRowSlice(colUps)
		colDelSlice := batchToRowSlice(colDel)

		// Compare upserts: order-sensitive
		rowUps := changesToUpserts(rowCollapsed)
		if !sliceEqual(rowUps, colUpsSlice) {
			t.Errorf("seed %d: upserts mismatch: row=%d, col=%d", seed, len(rowUps), len(colUpsSlice))
		}

		// Compare deletes: order-sensitive
		rowDel := changesToDeletes(rowCollapsed)
		if !sliceEqual(rowDel, colDelSlice) {
			t.Errorf("seed %d: deletes mismatch: row=%d, col=%d", seed, len(rowDel), len(colDelSlice))
		}
	}
}

// TestEquivalence_FilterPredicate_MatchesRowPath verifies that
// EvaluatePredicate matches row-by-row filtering.
func TestEquivalence_FilterPredicate_MatchesRowPath(t *testing.T) {
	alloc := checkedAlloc(t)

	for seed := range 100 {
		// PKDomain=5 with NumRows=25 → ~5 rows per PK → multi-row matches
		// with mixed ops. This makes order comparison meaningful.
		b := dataplane.GenerateBatch(int64(seed), dataplane.GeneratorOpts{NumRows: 25, PKDomain: 5, Allocator: alloc})
		defer b.Release()

		idCol := b.Record.Column(0).(*array.Int64)
		target := idCol.Value(seed % 5) // pick from 1..5 to guarantee a match

		// Row-path: classify by __op, filter by id == target
		valCol := b.Record.Column(1).(*array.String)
		opVals := extractOpCol(b)
		var rowIns, rowUpd, rowDel []eqChange
		for i := range int(idCol.Len()) {
			if idCol.Value(i) != target {
				continue
			}
			ec := eqChange{ID: idCol.Value(i), Val: valCol.Value(i), Op: opVals[i]}
			switch opVals[i] {
			case 0:
				rowIns = append(rowIns, ec)
			case 1:
				rowUpd = append(rowUpd, ec)
			case 2:
				rowDel = append(rowDel, ec)
			}
		}

		// Columnar path
		mask, err := dataplane.EvaluatePredicate(context.Background(), alloc, b, dataplane.Predicate{
			Column: "id",
			Op:     "=",
			Value:  target,
		})
		if err != nil {
			t.Fatalf("seed %d: EvaluatePredicate: %v", seed, err)
		}
		defer mask.Release()

		ins, del, upd, err := dataplane.Filter(context.Background(), alloc, b, mask)
		if err != nil {
			t.Fatalf("seed %d: Filter: %v", seed, err)
		}

		// Compare per class — each class preserves original order within it
		if !sliceEqual(rowIns, batchToRowSlice(ins)) {
			t.Errorf("seed %d: inserts mismatch: row=%d, col=%d (target=%d)", seed, len(rowIns), len(batchToRowSlice(ins)), target)
		}
		if !sliceEqual(rowUpd, batchToRowSlice(upd)) {
			t.Errorf("seed %d: updates mismatch: row=%d, col=%d (target=%d)", seed, len(rowUpd), len(batchToRowSlice(upd)), target)
		}
		if !sliceEqual(rowDel, batchToRowSlice(del)) {
			t.Errorf("seed %d: deletes mismatch: row=%d, col=%d (target=%d)", seed, len(rowDel), len(batchToRowSlice(del)), target)
		}
	}
}

// TestEquivalence_CollapseInsertAfterDelete verifies that an
// insert-after-delete ends up as an upsert (not a delete).
func TestEquivalence_CollapseInsertAfterDelete(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialInsertAfterDelete(0, alloc)
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), alloc, b, []string{"id"})
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
		opVals := extractOpCol(ups)
		if len(opVals) > 0 && opVals[0] != 0 {
			t.Errorf("expected op=insert (0), got %d", opVals[0])
		}
	}
}

// TestEquivalence_DeleteLast verifies that a PK whose last operation
// is DELETE ends up in deletes, not upserts.
func TestEquivalence_DeleteLast(t *testing.T) {
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialDeleteLast(0, alloc)
	defer b.Release()

	ups, dels, err := dataplane.Collapse(context.Background(), alloc, b, []string{"id"})
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
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialCompositeKey(0, alloc)
	defer b.Release()

	ups, _, err := dataplane.Collapse(context.Background(), alloc, b, []string{"pk1", "pk2"})
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
	alloc := checkedAlloc(t)
	b := dataplane.AdversarialInt64Overflow(0, alloc)
	defer b.Release()

	ups, _, err := dataplane.Collapse(context.Background(), alloc, b, []string{"id"})
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

// ── helpers ────────────────────────────────────────────────────────

// sliceEqual compares two eqChange slices element-by-element (order-sensitive).
func sliceEqual(a, b []eqChange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// batchToChanges converts a dataplane.Batch to []change.Change for use
// with change.Collapse (the production row-side reference).
func batchToChanges(b *dataplane.Batch) []change.Change {
	if b == nil || b.Record == nil {
		return nil
	}
	nrows := int(b.Record.NumRows())
	idCol := b.Record.Column(0).(*array.Int64)
	valCol := b.Record.Column(1).(*array.String)
	opVals := extractOpCol(b)
	posCol := -1
	for i := range b.Record.Schema().NumFields() {
		if b.Record.Schema().Field(i).Name == "__pos" {
			posCol = i
			break
		}
	}
	changes := make([]change.Change, nrows)
	for i := range nrows {
		changes[i] = change.Change{
			Op:       change.Op(opVals[i]),
			Table:    b.Table,
			Key:      []any{idCol.Value(i)},
			After:    map[string]any{"id": idCol.Value(i), "val": valCol.Value(i)},
			Position: b.Record.Column(posCol).(*array.String).Value(i),
		}
	}
	return changes
}

// changesToUpserts extracts upserts from a Collapsed as eqChange slices.
func changesToUpserts(c change.Collapsed) []eqChange {
	out := make([]eqChange, len(c.Upserts))
	for i, ch := range c.Upserts {
		out[i] = eqChange{
			ID:  ch.Key[0].(int64),
			Val: ch.After["val"].(string),
			Op:  uint8(ch.Op),
		}
	}
	return out
}

// changesToDeletes extracts deletes from a Collapsed as eqChange slices.
func changesToDeletes(c change.Collapsed) []eqChange {
	out := make([]eqChange, len(c.Deletes))
	for i, ch := range c.Deletes {
		out[i] = eqChange{
			ID:  ch.Key[0].(int64),
			Val: ch.After["val"].(string),
			Op:  uint8(ch.Op),
		}
	}
	return out
}
