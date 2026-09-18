package position

import (
	"testing"
)

func TestParseOffsets(t *testing.T) {
	o, err := ParseOffsets("orders:p0=10,p1=20")
	if err != nil {
		t.Fatal(err)
	}
	parts, ok := o.Topics["orders"]
	if !ok {
		t.Fatalf("topics = %v, want an orders entry", o.Topics)
	}
	if parts[0] != 10 || parts[1] != 20 {
		t.Errorf("parts = %v, want {0:10, 1:20}", parts)
	}
}

func TestParseOffsetsEmpty(t *testing.T) {
	o, err := ParseOffsets("orders:")
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Topics["orders"]) != 0 {
		t.Errorf("parts = %v, want empty", o.Topics["orders"])
	}
}

func TestParseOffsetsBadFormat(t *testing.T) {
	if _, err := ParseOffsets("no-colon"); err == nil {
		t.Error("expected error for missing colon")
	}
	if _, err := ParseOffsets("t:bad"); err == nil {
		t.Error("expected error for bad pair")
	}
}

func TestOffsetsString(t *testing.T) {
	o := NewOffsets("orders", map[int32]int64{1: 20, 0: 10})
	s := o.String()
	if s != "orders:p0=10,p1=20" {
		t.Errorf("String() = %q, want orders:p0=10,p1=20", s)
	}
}

func TestOffsetsCompare(t *testing.T) {
	a := NewOffsets("t", map[int32]int64{0: 10})
	b := NewOffsets("t", map[int32]int64{0: 20})
	if a.Compare(b) >= 0 {
		t.Error("a should be less than b")
	}
	if b.Compare(a) <= 0 {
		t.Error("b should be greater than a")
	}
}

// Disjoint topics carry no information about each other: neither covers the
// other's progress, so neither contains it and the pair is Incomparable.
// They do have a meet — the empty position — because claiming no progress on
// either topic is always a safe lower bound.
func TestOffsetsCompareDifferentTopic(t *testing.T) {
	a := NewOffsets("a", map[int32]int64{0: 100})
	b := NewOffsets("b", map[int32]int64{0: 1})
	if a.Compare(b) != Incomparable {
		t.Errorf("disjoint topics must be Incomparable, got %d", a.Compare(b))
	}
	m, ok := a.Meet(b)
	if !ok {
		t.Fatal("disjoint topics still have a meet (the empty position)")
	}
	if m.String() != "" {
		t.Errorf("meet = %q, want the empty position", m.String())
	}
}

func TestOffsetsContains(t *testing.T) {
	a := NewOffsets("t", map[int32]int64{0: 10, 1: 20})
	b := NewOffsets("t", map[int32]int64{0: 5, 1: 15})
	if !a.Contains(b) {
		t.Error("a should contain b")
	}
	if b.Contains(a) {
		t.Error("b should not contain a")
	}
}

func TestOffsetsContainsExtraPartition(t *testing.T) {
	a := NewOffsets("t", map[int32]int64{0: 10, 1: 20, 2: 30})
	b := NewOffsets("t", map[int32]int64{0: 5, 1: 15})
	if !a.Contains(b) {
		t.Error("a with extra partition should contain b")
	}
}

// A topic the other side has made progress on, and this side knows nothing
// about, is not covered — that progress has not been matched.
func TestOffsetsContainsDifferentTopic(t *testing.T) {
	a := NewOffsets("a", map[int32]int64{0: 10})
	b := NewOffsets("b", map[int32]int64{0: 5})
	if a.Contains(b) {
		t.Error("a knows nothing of topic b, so it cannot contain b's progress")
	}
}

// A topic present in o but absent from other is future work, not a gap.
func TestOffsetsContainsExtraTopic(t *testing.T) {
	a := &Offsets{}
	a.Set("orders", 0, 10)
	a.Set("payments", 0, 10)
	b := NewOffsets("orders", map[int32]int64{0: 5})
	if !a.Contains(b) {
		t.Error("a covers every topic b knows about, so it contains b")
	}
}

func TestOffsetsJSONRoundTrip(t *testing.T) {
	original := NewOffsets("orders", map[int32]int64{0: 10, 2: 30})
	data, err := original.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Offsets
	if err := decoded.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if got, want := decoded.String(), original.String(); got != want {
		t.Errorf("round trip = %q, want %q", got, want)
	}
}
