package change

import (
	"reflect"
	"testing"
)

func chg(op Op, id int64, v string, pos string) Change {
	c := Change{Op: op, Key: []any{id}, Position: pos}
	if op != OpDelete {
		c.After = map[string]any{"id": id, "v": v}
	}
	return c
}

func TestCollapseLastOperationWins(t *testing.T) {
	got := Collapse([]Change{
		chg(OpInsert, 1, "a", "p1"),
		chg(OpUpdate, 1, "b", "p2"),
		chg(OpUpdate, 1, "c", "p3"),
	})

	if len(got.Upserts) != 1 || len(got.Deletes) != 0 {
		t.Fatalf("want 1 upsert / 0 deletes, got %+v", got)
	}
	if got.Upserts[0].After["v"] != "c" {
		t.Fatalf("want final value c, got %v", got.Upserts[0].After["v"])
	}
	if got.Upserts[0].Position != "p3" {
		t.Fatalf("want position of last change, got %v", got.Upserts[0].Position)
	}
}

func TestCollapseDeleteLastNeverYieldsDataRow(t *testing.T) {
	got := Collapse([]Change{
		chg(OpInsert, 7, "a", "p1"),
		chg(OpDelete, 7, "", "p2"),
	})

	if len(got.Upserts) != 0 {
		t.Fatalf("deleted key must never yield a data row, got upserts %+v", got.Upserts)
	}
	if len(got.Deletes) != 1 || got.Deletes[0].Key[0] != int64(7) {
		t.Fatalf("want single delete for key 7, got %+v", got.Deletes)
	}
}

func TestCollapseReinsertAfterDelete(t *testing.T) {
	got := Collapse([]Change{
		chg(OpInsert, 1, "a", "p1"),
		chg(OpDelete, 1, "", "p2"),
		chg(OpInsert, 1, "b", "p3"),
	})

	if len(got.Upserts) != 1 || len(got.Deletes) != 0 {
		t.Fatalf("re-inserted key must end as an upsert, got %+v", got)
	}
	if got.Upserts[0].After["v"] != "b" {
		t.Fatalf("want final value b, got %v", got.Upserts[0].After["v"])
	}
}

func TestCollapseKeepsFirstAppearanceOrder(t *testing.T) {
	got := Collapse([]Change{
		chg(OpInsert, 1, "a", "p1"),
		chg(OpInsert, 2, "x", "p2"),
		chg(OpUpdate, 1, "b", "p3"),
		chg(OpDelete, 2, "", "p4"),
	})

	wantKeys := []any{int64(1)}
	if len(got.Upserts) != len(wantKeys) {
		t.Fatalf("want %d upserts, got %+v", len(wantKeys), got.Upserts)
	}
	if got.Upserts[0].Key[0] != int64(1) {
		t.Fatalf("want key 1 first, got %v", got.Upserts[0].Key)
	}
	if len(got.Deletes) != 1 || got.Deletes[0].Key[0] != int64(2) {
		t.Fatalf("want key 2 deleted, got %+v", got.Deletes)
	}
}

func TestCollapseKeysCoversBothSets(t *testing.T) {
	got := Collapse([]Change{
		chg(OpUpdate, 1, "b", "p1"),
		chg(OpDelete, 2, "", "p2"),
	})

	keys := got.Keys()
	if !reflect.DeepEqual(keys, [][]any{{int64(1)}, {int64(2)}}) {
		t.Fatalf("want keys [1 2], got %v", keys)
	}
}

func TestCollapseEmpty(t *testing.T) {
	got := Collapse(nil)
	if len(got.Upserts) != 0 || len(got.Deletes) != 0 {
		t.Fatalf("empty batch must collapse to empty, got %+v", got)
	}
}

func TestCollapseCompositeKey(t *testing.T) {
	mk := func(a, b int64) Change {
		return Change{Op: OpInsert, Key: []any{a, b}, After: map[string]any{}}
	}
	got := Collapse([]Change{mk(1, 2), mk(1, 3), mk(12, 3)})
	if len(got.Upserts) != 3 {
		t.Fatalf("composite keys must stay distinct, got %+v", got.Upserts)
	}
}

// Key rendering must never merge distinct values: int64(1) and float64(1)
// format identically without a type tag, and a silent merge would collapse
// two different rows into one map entry.
func TestKeyStringDistinguishesTypes(t *testing.T) {
	if KeyString([]any{int64(1)}) == KeyString([]any{float64(1)}) {
		t.Fatal("int64(1) and float64(1) render the same key")
	}
	if KeyString([]any{int64(1)}) == KeyString([]any{"1"}) {
		t.Fatal("int64(1) and string \"1\" render the same key")
	}
	if KeyString([]any{int64(1)}) == KeyString([]any{uint64(1)}) {
		t.Fatal("int64(1) and uint64(1) render the same key")
	}
	// Equal values of the same type must still match.
	if KeyString([]any{int64(1)}) != KeyString([]any{int64(1)}) {
		t.Fatal("identical keys render differently")
	}
}
