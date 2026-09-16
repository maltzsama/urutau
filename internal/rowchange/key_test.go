package rowchange

import "testing"

// KeyString indexes equality-delete tuples by key position in the sink, so two
// different values rendering to the same string is corruption, not a
// deduplication nicety. The type prefix is what prevents it.
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
	k1 := KeyString([]any{int64(1)})
	if k1 != KeyString([]any{int64(1)}) {
		t.Fatal("identical keys render differently")
	}
}

// Adjacent elements must not merge across the separator: ["ab","c"] and
// ["a","bc"] are different composite keys.
func TestKeyStringCompositeNoAdjacentMerge(t *testing.T) {
	if KeyString([]any{"ab", "c"}) == KeyString([]any{"a", "bc"}) {
		t.Fatal(`["ab","c"] and ["a","bc"] render the same key`)
	}
}

// A nil element is its own value and must not collide with the string "nil".
func TestKeyStringNilDistinctFromString(t *testing.T) {
	if KeyString([]any{nil}) == KeyString([]any{"nil"}) {
		t.Fatal(`nil and the string "nil" render the same key`)
	}
}
