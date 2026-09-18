package position

import "testing"

// offs builds a single-topic position.
func offs(topic string, parts map[int32]int64) *Offsets {
	return NewOffsets(topic, parts)
}

// divergent returns the canonical divergent pair: same topic, same maxOffset,
// neither containing the other. The old maxOffset heuristic reported 0 here,
// which is the root of both bugs this file pins.
func divergent() (*Offsets, *Offsets) {
	a := offs("t", map[int32]int64{0: 100, 1: 5})
	b := offs("t", map[int32]int64{0: 5, 1: 100})
	return a, b
}

// Compare must not claim an order that Contains denies. Reporting 0 made
// covered() (coordinator/flow.go) skip a batch whose partition was not
// committed, and that data was never replayed.
func TestOffsetsDivergentIsIncomparable(t *testing.T) {
	a, b := divergent()
	if a.Contains(b) || b.Contains(a) {
		t.Fatal("fixture is not divergent")
	}
	if got := a.Compare(b); got != Incomparable {
		t.Errorf("a.Compare(b) = %d, want Incomparable", got)
	}
	if got := b.Compare(a); got != Incomparable {
		t.Errorf("b.Compare(a) = %d, want Incomparable", got)
	}
}

// Containment still orders what it can: Incomparable must not swallow the
// ordinary case of one position genuinely being ahead.
func TestOffsetsContainmentStillOrders(t *testing.T) {
	ahead := offs("t", map[int32]int64{0: 100, 1: 100})
	behind := offs("t", map[int32]int64{0: 10, 1: 10})
	if got := ahead.Compare(behind); got != 1 {
		t.Errorf("ahead.Compare(behind) = %d, want 1", got)
	}
	if got := behind.Compare(ahead); got != -1 {
		t.Errorf("behind.Compare(ahead) = %d, want -1", got)
	}
	same := offs("t", map[int32]int64{0: 10, 1: 10})
	if got := behind.Compare(same); got != 0 {
		t.Errorf("equal positions compare %d, want 0", got)
	}
}

// The meet is the per-partition minimum, and it must be contained by BOTH
// inputs — that is what makes it a safe resume point.
func TestOffsetsMeetIsGreatestLowerBound(t *testing.T) {
	a, b := divergent()
	m, ok := a.Meet(b)
	if !ok {
		t.Fatal("same-topic offsets must have a meet")
	}
	if got, want := m.String(), "t:p0=5,p1=5"; got != want {
		t.Errorf("meet = %s, want %s", got, want)
	}
	if !a.Contains(m) || !b.Contains(m) {
		t.Errorf("meet %s is not contained by both inputs", m)
	}
}

// A partition only one side knows about cannot be bounded: assuming the
// other side had also reached it would resume past unread data.
func TestOffsetsMeetDropsUnknownPartition(t *testing.T) {
	a := offs("t", map[int32]int64{0: 10, 1: 50})
	b := offs("t", map[int32]int64{0: 20})
	m, ok := a.Meet(b)
	if !ok {
		t.Fatal("same-topic offsets must have a meet")
	}
	if got, want := m.String(), "t:p0=10"; got != want {
		t.Errorf("meet = %s, want %s (p1 unknown to b)", got, want)
	}
}

// THE RESUME BUG: MinSafe folded per-owner positions (WK-001 C7) by picking
// one of them. The winner sat ahead of the other owner on some partition, so
// resume started past data that was never read. The result must now be
// contained by every input.
func TestMinSafeDivergentOwnersIsContainedByAll(t *testing.T) {
	a, b := divergent()
	got, err := MinSafe([]Position{a, b})
	if err != nil {
		t.Fatalf("MinSafe: %v", err)
	}
	if !a.Contains(got) || !b.Contains(got) {
		t.Fatalf("MinSafe returned %s, which is not a lower bound of %s and %s", got, a, b)
	}
	if want := "t:p0=5,p1=5"; got.String() != want {
		t.Errorf("MinSafe = %s, want %s", got, want)
	}
}

// Three owners fold pairwise to the same bound, independent of input order.
func TestMinSafeThreeOwnersOrderIndependent(t *testing.T) {
	x := offs("t", map[int32]int64{0: 100, 1: 5, 2: 50})
	y := offs("t", map[int32]int64{0: 5, 1: 100, 2: 50})
	z := offs("t", map[int32]int64{0: 50, 1: 50, 2: 7})
	want := "t:p0=5,p1=5,p2=7"

	for _, order := range [][]Position{{x, y, z}, {z, y, x}, {y, z, x}} {
		got, err := MinSafe(order)
		if err != nil {
			t.Fatalf("MinSafe: %v", err)
		}
		if got.String() != want {
			t.Errorf("MinSafe = %s, want %s", got, want)
		}
		for _, p := range order {
			if !p.Contains(got) {
				t.Errorf("%s does not contain the bound %s", p, got)
			}
		}
	}
}

// Disjoint topics meet to the empty position: no progress is claimed on
// either topic, so the resume reads both from the start. This used to be an
// error only because the type could not hold two topics at once — an empty
// lower bound is the honest answer, and it is safe (it never skips).
func TestMinSafeDisjointTopicsMeetToEmpty(t *testing.T) {
	a := offs("t1", map[int32]int64{0: 10})
	b := offs("t2", map[int32]int64{0: 10})
	got, err := MinSafe([]Position{a, b})
	if err != nil {
		t.Fatalf("MinSafe: %v", err)
	}
	if !a.Contains(got) || !b.Contains(got) {
		t.Errorf("bound %s is not contained by both inputs", got)
	}
	if got.String() != "" {
		t.Errorf("MinSafe = %q, want the empty position", got.String())
	}
}

// Min folds the same way, for the same reason: its caller (runner explicit
// start positions) notes that starting too late would skip data.
func TestMinDivergentFoldsToBound(t *testing.T) {
	a, b := divergent()
	got := Min([]Position{a, b})
	if !a.Contains(got) || !b.Contains(got) {
		t.Errorf("Min returned %s, not a lower bound of both inputs", got)
	}
}

// ── multi-topic (issue #141) ───────────────────────────────────────

// The bug: one reader consumes every topic its spec tables name, and
// partition numbers repeat across topics. Keyed by partition alone, the
// second topic's p0 destroyed the first's.
func TestOffsetsKeepsTopicsSeparate(t *testing.T) {
	o := &Offsets{}
	o.Set("orders", 0, 500)
	o.Set("payments", 0, 3)

	if got := o.Topics["orders"][0]; got != 500 {
		t.Errorf("orders p0 = %d, want 500", got)
	}
	if got := o.Topics["payments"][0]; got != 3 {
		t.Errorf("payments p0 = %d, want 3", got)
	}
	if got, want := o.String(), "orders:p0=500;payments:p0=3"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// A multi-topic position must survive the text round trip it is stored as.
func TestOffsetsMultiTopicRoundTrip(t *testing.T) {
	o := &Offsets{}
	o.Set("orders", 0, 500)
	o.Set("orders", 1, 12)
	o.Set("payments", 0, 3)

	back, err := ParseOffsets(o.String())
	if err != nil {
		t.Fatalf("ParseOffsets(%q): %v", o.String(), err)
	}
	if got := back.String(); got != o.String() {
		t.Errorf("round trip = %q, want %q", got, o.String())
	}
	if back.Compare(o) != 0 {
		t.Errorf("round-tripped position does not compare equal")
	}
}

// Containment is per topic: being ahead on one topic does not cover being
// behind on another.
func TestOffsetsContainsAcrossTopics(t *testing.T) {
	ahead := &Offsets{}
	ahead.Set("orders", 0, 500)
	ahead.Set("payments", 0, 100)

	behind := &Offsets{}
	behind.Set("orders", 0, 10)
	behind.Set("payments", 0, 10)

	if !ahead.Contains(behind) {
		t.Error("ahead must contain behind on every topic")
	}
	if behind.Contains(ahead) {
		t.Error("behind must not contain ahead")
	}

	mixed := &Offsets{}
	mixed.Set("orders", 0, 500) // ahead of `ahead`? no — equal
	mixed.Set("payments", 0, 1) // behind `ahead`
	if mixed.Contains(ahead) {
		t.Error("mixed is behind on payments, so it cannot contain ahead")
	}
	if got := mixed.Compare(ahead); got != -1 {
		t.Errorf("mixed.Compare(ahead) = %d, want -1 (equal on orders, behind on payments)", got)
	}
}

// The meet of two multi-topic positions is the per-topic, per-partition
// minimum.
func TestOffsetsMeetAcrossTopics(t *testing.T) {
	a := &Offsets{}
	a.Set("orders", 0, 500)
	a.Set("payments", 0, 3)

	b := &Offsets{}
	b.Set("orders", 0, 100)
	b.Set("payments", 0, 90)

	m, ok := a.Meet(b)
	if !ok {
		t.Fatal("offsets must have a meet")
	}
	if got, want := m.String(), "orders:p0=100;payments:p0=3"; got != want {
		t.Errorf("meet = %s, want %s", got, want)
	}
	if !a.Contains(m) || !b.Contains(m) {
		t.Errorf("meet %s is not contained by both inputs", m)
	}
}

// JSON is the durable wire form; a multi-topic position must survive it.
func TestOffsetsJSONMultiTopicRoundTrip(t *testing.T) {
	o := &Offsets{}
	o.Set("orders", 0, 500)
	o.Set("payments", 1, 3)

	b, err := o.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var back Offsets
	if err := back.UnmarshalJSON(b); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if got := back.String(); got != o.String() {
		t.Errorf("JSON round trip = %q, want %q", got, o.String())
	}
}
