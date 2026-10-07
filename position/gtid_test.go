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

// A MySQL 8.4 tagged GTID (uuid:tag:interval) is accepted and stays distinct
// from its untagged form (issue #576).
func TestParseGTIDTagged(t *testing.T) {
	const uuid = "3d3b4a6a-2f4b-11e9-9c9b-0242ac110002"
	g, err := ParseGTID(uuid + ":mytag:1-5")
	if err != nil {
		t.Fatalf("ParseGTID(tagged): %v", err)
	}
	if got := g.String(); got != uuid+":mytag:1-5" {
		t.Fatalf("tagged String() = %q", got)
	}
	if g.Contains(MustGTID(uuid + ":1-5")) {
		t.Fatal("a tagged set must not equal its untagged form")
	}
}

// Compare/Contains must not panic on a position of another kind (issue #576).
func TestGTIDCompareAndContainsOtherKind(t *testing.T) {
	g := MustGTID("3d3b4a6a-2f4b-11e9-9c9b-0242ac110002:1")
	if c := g.Compare(offsetsPosition{}); c != Incomparable {
		t.Fatalf("Compare(other) = %d, want Incomparable", c)
	}
	if g.Contains(offsetsPosition{}) {
		t.Fatal("Contains(other) = true, want false")
	}
}

type offsetsPosition struct{}

func (offsetsPosition) String() string         { return "" }
func (offsetsPosition) Compare(Position) int   { return Incomparable }
func (offsetsPosition) Contains(Position) bool { return false }
