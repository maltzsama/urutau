package routing

import (
	"errors"
	"reflect"
	"testing"

	"github.com/maltzsama/urutau/source"
)

// A reader holding a layout must keep seeing it after a re-slice: WithTable
// builds a new layout and leaves the receiver, and its slices, untouched.
func TestWithTableLeavesThePublishedLayoutUntouched(t *testing.T) {
	old := NewLayout(map[string][]string{"raw.a": {"a-0", "a-1"}, "raw.b": {"b-0"}},
		map[string][]source.Chunk{"raw.a": {{}, {}}, "raw.b": {{}}})

	next := old.WithTable("raw.a", []string{"a-0"}, []source.Chunk{{}})

	if got, _ := old.OwnersOf("raw.a"); !reflect.DeepEqual(got, []string{"a-0", "a-1"}) {
		t.Fatalf("published layout owners = %v, want the two it had", got)
	}
	if got := old.RangesOf("raw.a"); len(got) != 2 {
		t.Fatalf("published layout ranges = %d, want 2", len(got))
	}
	if got, _ := next.OwnersOf("raw.a"); !reflect.DeepEqual(got, []string{"a-0"}) {
		t.Fatalf("new layout owners = %v, want [a-0]", got)
	}
	if got, ok := next.OwnersOf("raw.b"); !ok || !reflect.DeepEqual(got, []string{"b-0"}) {
		t.Fatalf("untouched table in the new layout = %v, %v", got, ok)
	}
}

func TestANilLayoutRoutesNothing(t *testing.T) {
	var l *Layout[string]
	if o, ok := l.OwnersOf("raw.a"); ok || o != nil {
		t.Fatalf("OwnersOf on nil = %v, %v", o, ok)
	}
	if r := l.RangesOf("raw.a"); r != nil {
		t.Fatalf("RangesOf on nil = %v", r)
	}
	if got, ok := l.WithOwners("raw.a", []string{"a-0"}).OwnersOf("raw.a"); !ok || len(got) != 1 {
		t.Fatalf("WithOwners on nil = %v, %v", got, ok)
	}
}

func TestPartitionNameMatchesTheBootNames(t *testing.T) {
	if got := PartitionName("shop", "raw.orders", 2); got != "shop-raw-orders-2" {
		t.Fatalf("PartitionName = %q, want shop-raw-orders-2", got)
	}
}

// A scale-in keeps the first n owners and returns the rest as removed; the
// kept slice must not be able to grow into the removed owners' slots.
func TestResliceScaleInKeepsThePrefix(t *testing.T) {
	cur := []string{"a-0", "a-1", "a-2"}
	next, removed, created, err := Reslice(cur, 1, func(int) (string, bool, error) {
		t.Fatal("a scale-in adds no owner")
		return "", false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(next, []string{"a-0"}) || !reflect.DeepEqual(removed, []string{"a-1", "a-2"}) || created != nil {
		t.Fatalf("next=%v removed=%v created=%v", next, removed, created)
	}
	_ = append(next, "x")
	if cur[1] != "a-1" {
		t.Fatalf("appending to the kept owners overwrote a removed one: %v", cur)
	}
}

// A scale-out keeps the existing owners in place, reports only the owners it
// created, and never writes to the slice a reader may still hold.
func TestResliceScaleOutAddsAtTheBoundary(t *testing.T) {
	cur := make([]string, 1, 4)
	cur[0] = "a-0"
	next, removed, created, err := Reslice(cur, 3, func(p int) (string, bool, error) {
		return PartitionName("p", "a", p), p == 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(next, []string{"a-0", "p-a-1", "p-a-2"}) || removed != nil || !reflect.DeepEqual(created, []string{"p-a-2"}) {
		t.Fatalf("next=%v removed=%v created=%v", next, removed, created)
	}
	if got := cur[:2][1]; got != "" {
		t.Fatalf("scale-out wrote into the published slice's spare capacity: %q", got)
	}
}

func TestResliceStopsOnTheFirstOwnerItCannotAdd(t *testing.T) {
	boom := errors.New("boom")
	next, removed, created, err := Reslice([]string{"a-0"}, 3, func(int) (string, bool, error) { return "", false, boom })
	if !errors.Is(err, boom) || next != nil || removed != nil || created != nil {
		t.Fatalf("next=%v removed=%v created=%v err=%v", next, removed, created, err)
	}
}
