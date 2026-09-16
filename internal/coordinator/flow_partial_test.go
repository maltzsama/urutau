package coordinator

import (
	"testing"

	"github.com/maltzsama/urutau/position"
)

// covered() must never claim coverage the position itself denies. With the
// old maxOffset heuristic these two compared equal, covered() returned true,
// and the batch was dropped even though partition 1 was not committed — the
// data was never replayed.
func TestCoveredRejectsDivergentOffsets(t *testing.T) {
	batch := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 5, 1: 100}}
	cur := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 100, 1: 5}}

	if cur.Contains(batch) {
		t.Fatal("fixture is wrong: cur must not contain batch")
	}
	if covered(batch, cur) {
		t.Error("covered() reported an uncovered batch as covered — the batch would be skipped")
	}
}

// The mirror: a batch genuinely at or before the committed point must still
// be recognised, or the queue would never drain.
func TestCoveredStillAcceptsContained(t *testing.T) {
	batch := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 5, 1: 5}}
	cur := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 100, 1: 100}}

	if !covered(batch, cur) {
		t.Error("covered() must accept a batch the committed point contains")
	}
}

// advances() is the same guard in the other direction: a divergent position
// is not provably ahead, so it must not move the confirmed point.
func TestAdvancesRejectsDivergentOffsets(t *testing.T) {
	pos := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 5, 1: 100}}
	cur := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 100, 1: 5}}

	if advances(pos, cur) {
		t.Error("advances() reported a divergent position as strictly greater")
	}
}

func TestAdvancesStillAcceptsStrictlyAhead(t *testing.T) {
	pos := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 100, 1: 100}}
	cur := &position.Offsets{Topic: "t", Parts: map[int32]int64{0: 10, 1: 10}}

	if !advances(pos, cur) {
		t.Error("advances() must accept a position that contains and exceeds cur")
	}
}
