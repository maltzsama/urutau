package coordinator

import (
	"strings"
	"testing"

	"github.com/maltzsama/urutau/position"

	"github.com/maltzsama/urutau/internal/inflight"
)

func TestPositionIndexDirty(t *testing.T) {
	idx := inflight.NewPositionIndex("run-1")
	if idx.Dirty() {
		t.Fatal("new index should not be dirty")
	}

	// add makes it dirty
	idx.Add(inflight.Batch{ID: 1, Table: "t1", Bytes: 100})
	if !idx.Dirty() {
		t.Fatal("add should set dirty")
	}

	// Manifest does NOT clear dirty (checkpoint writes it later)
	_, gen := idx.Manifest()
	if !idx.Dirty() {
		t.Fatal("Manifest should not clear dirty")
	}

	// MarkClean clears it
	idx.MarkClean(gen)
	if idx.Dirty() {
		t.Fatal("MarkClean should clear dirty")
	}

	// truncate on new table makes it dirty
	idx.Truncate("t1", positionFixture("pos-1"))
	if !idx.Dirty() {
		t.Fatal("truncate should set dirty")
	}

	_, gen = idx.Manifest()
	idx.MarkClean(gen)

	// duplicate truncate (same position) should NOT set dirty
	idx.Truncate("t1", positionFixture("pos-1"))
	if idx.Dirty() {
		t.Fatal("duplicate truncate should not set dirty")
	}
}

// positionFixture is a minimal Position implementation for tests.
type positionFixture string

func (p positionFixture) String() string { return string(p) }
func (p positionFixture) Compare(other position.Position) int {
	return strings.Compare(string(p), other.(positionFixture).String())
}
func (p positionFixture) Contains(position.Position) bool { return false }
