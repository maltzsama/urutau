package position

import "testing"

// opaque is an identity-only position (like a plugin offset cookie, contract
// §8.2): same type, but two different values are Incomparable — there is no
// order. This is the case P1 guards: a resume fold must NOT pick an
// arbitrary minimum among opaque positions.
type opaque string

func (o opaque) String() string { return string(o) }
func (o opaque) Compare(other Position) int {
	p, ok := other.(opaque)
	if !ok {
		return Incomparable
	}
	if o == p {
		return 0
	}
	return Incomparable
}
func (o opaque) Contains(other Position) bool {
	p, ok := other.(opaque)
	return ok && o == p
}

func TestMinSafeRejectsIncomparable(t *testing.T) {
	a := opaque("cookie-A")
	b := opaque("cookie-B")
	if _, err := MinSafe([]Position{a, b}); err == nil {
		t.Fatal("incomparable opaque positions must error, not pick arbitrarily")
	}
	// Identity is still comparable.
	got, err := MinSafe([]Position{a, a})
	if err != nil {
		t.Fatalf("identical opaque positions must fold: %v", err)
	}
	if got.String() != "cookie-A" {
		t.Fatalf("got %s", got)
	}
	if got, err := MinSafe(nil); err != nil || got != nil {
		t.Fatalf("empty: got %v err %v", got, err)
	}
}

func TestMinSafeFoldsOrdered(t *testing.T) {
	a := MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:1")
	b := MustGTID("3e11fa47-71ca-11e1-9e33-c80aa9429562:5")
	got, err := MinSafe([]Position{a, b})
	if err != nil {
		t.Fatalf("MinSafe: %v", err)
	}
	if got.String() != a.String() {
		t.Fatalf("min = %s, want %s", got, a)
	}
}
