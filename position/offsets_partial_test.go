package position

import "testing"

// divergent returns the canonical divergent pair: same topic, same maxOffset,
// neither containing the other. The old maxOffset heuristic reported 0 here,
// which is the root of both bugs this file pins.
func divergent() (*Offsets, *Offsets) {
	a := &Offsets{Topic: "t", Parts: map[int32]int64{0: 100, 1: 5}}
	b := &Offsets{Topic: "t", Parts: map[int32]int64{0: 5, 1: 100}}
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
	ahead := &Offsets{Topic: "t", Parts: map[int32]int64{0: 100, 1: 100}}
	behind := &Offsets{Topic: "t", Parts: map[int32]int64{0: 10, 1: 10}}
	if got := ahead.Compare(behind); got != 1 {
		t.Errorf("ahead.Compare(behind) = %d, want 1", got)
	}
	if got := behind.Compare(ahead); got != -1 {
		t.Errorf("behind.Compare(ahead) = %d, want -1", got)
	}
	same := &Offsets{Topic: "t", Parts: map[int32]int64{0: 10, 1: 10}}
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
	a := &Offsets{Topic: "t", Parts: map[int32]int64{0: 10, 1: 50}}
	b := &Offsets{Topic: "t", Parts: map[int32]int64{0: 20}}
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
	x := &Offsets{Topic: "t", Parts: map[int32]int64{0: 100, 1: 5, 2: 50}}
	y := &Offsets{Topic: "t", Parts: map[int32]int64{0: 5, 1: 100, 2: 50}}
	z := &Offsets{Topic: "t", Parts: map[int32]int64{0: 50, 1: 50, 2: 7}}
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

// No meet across topics: fail fast rather than invent a resume point.
func TestMinSafeDifferentTopicsStillErrors(t *testing.T) {
	a := &Offsets{Topic: "t1", Parts: map[int32]int64{0: 10}}
	b := &Offsets{Topic: "t2", Parts: map[int32]int64{0: 10}}
	if _, err := MinSafe([]Position{a, b}); err == nil {
		t.Error("MinSafe must error when there is no safe minimum")
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
