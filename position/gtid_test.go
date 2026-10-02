package position

import (
	"testing"
)

const (
	uuidA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	uuidB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
)

func TestParseGTIDAndString(t *testing.T) {
	in := uuidA + ":1-5," + uuidB + ":7"
	g, err := ParseGTID(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if g.String() != in {
		t.Fatalf("round trip = %q, want %q", g.String(), in)
	}
}

func TestParseGTIDEmpty(t *testing.T) {
	g, err := ParseGTID("")
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if g.String() != "" {
		t.Fatalf("empty set string = %q", g.String())
	}
}

func TestParseGTIDInvalid(t *testing.T) {
	if _, err := ParseGTID("garbage"); err == nil {
		t.Fatal("want parse error for garbage")
	}
}

func TestGTIDContains(t *testing.T) {
	sub := MustGTID(uuidA + ":1-3")
	sup := MustGTID(uuidA + ":1-10")
	if !sup.Contains(sub) {
		t.Fatal("superset must contain subset")
	}
	if sub.Contains(sup) {
		t.Fatal("subset must not contain superset")
	}
}

func TestGTIDAdd(t *testing.T) {
	a := MustGTID(uuidA + ":1-3")
	b := MustGTID(uuidA + ":4-6")
	a.Add(b)
	want := MustGTID(uuidA + ":1-6")
	if !a.Contains(want) || !want.Contains(a) {
		t.Fatalf("after add: %q vs %q", a, want)
	}
}

func TestGTIDCompare(t *testing.T) {
	small := MustGTID(uuidA + ":1-3")
	big := MustGTID(uuidA + ":1-10")
	if small.Compare(big) >= 0 {
		t.Fatal("small must compare less than big")
	}
	if big.Compare(small) <= 0 {
		t.Fatal("big must compare greater than small")
	}
}

func TestMinContained(t *testing.T) {
	oldest := MustGTID(uuidA + ":1-3")
	mid := MustGTID(uuidA + ":1-8")
	newest := MustGTID(uuidA + ":1-20")
	got := Min([]Position{newest, oldest, mid})
	if got.String() != oldest.String() {
		t.Fatalf("min = %q, want %q", got, oldest)
	}
}

func TestMinDisjointGTIDIsIncomparable(t *testing.T) {
	// Disjoint uuid universes have no defined order. The old "smallest max
	// interval" heuristic returned a value that could be ahead of the other,
	// so a resume fold could skip data a failover had not replicated
	// (issue #485). Compare now matches Offsets: Incomparable.
	a := MustGTID(uuidA + ":1-5")
	b := MustGTID(uuidB + ":1-2")
	if got := a.Compare(b); got != Incomparable {
		t.Fatalf("Compare(disjoint) = %d, want Incomparable", got)
	}
	if _, err := MinSafe([]Position{a, b}); err == nil {
		t.Fatal("MinSafe of disjoint GTID sets must error, not pick a side")
	}
}
