package clickhouse

import (
	"testing"
	"time"
)

// TestNextSeqGuardProvesDeterministicOrdering is the deterministic half of
// the seq-collision concern (CR-040 §8.7): with the clock frozen — the worst
// case, two commits reading the exact same nanosecond — the guard must still
// hand out strictly increasing seqs. argMax(position, seq) is only correct
// if this holds; in append mode there is no ReplacingMergeTree to mask it.
func TestNextSeqGuardProvesDeterministicOrdering(t *testing.T) {
	frozen := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	w := &tableWriter{now: func() time.Time { return frozen }}

	first := w.nextSeq()
	for i := 0; i < 3; i++ {
		if next := w.nextSeq(); next != first+uint64(i)+1 {
			t.Fatalf("seq %d after frozen clock: got %d, want strictly increasing", i+2, next)
		}
	}
}

// TestNextSeqClockAdvancesNormally proves the guard only steps in when the
// clock stalls: with a moving clock the seq IS the clock.
func TestNextSeqClockAdvancesNormally(t *testing.T) {
	base := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	ticks := []time.Time{base, base.Add(time.Millisecond), base.Add(2 * time.Millisecond)}
	i := 0
	w := &tableWriter{now: func() time.Time {
		t := ticks[i]
		if i < len(ticks)-1 {
			i++
		}
		return t
	}}

	for _, want := range []uint64{uint64(base.UnixNano()), uint64(base.Add(time.Millisecond).UnixNano())} {
		if got := w.nextSeq(); got != want {
			t.Fatalf("seq = %d, want %d (clock value, untouched)", got, want)
		}
	}
}

// TestNextSeqSeededFromTable proves the boot-time seed: a fresh writer on a
// table whose highest seq is above the current clock (clock stepped back
// between boots) must not regress.
func TestNextSeqSeededFromTable(t *testing.T) {
	seed := uint64(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()) // "future" seq from a previous boot
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	w := &tableWriter{now: func() time.Time { return now }, lastSeq: seed}

	if got := w.nextSeq(); got != seed+1 {
		t.Fatalf("seq after boot with stepped-back clock = %d, want %d (seed+1)", got, seed+1)
	}
}
