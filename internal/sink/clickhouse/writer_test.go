package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
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

// wireReader builds a one-row wire-schema batch (all NULL) and its reader.
func wireReader(t *testing.T, cs core.Schema) *transport.BatchReader {
	t.Helper()
	as, err := transport.CoreSchemaToArrow(cs)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, as)
	defer bld.Release()
	for j := 0; j < int(as.NumFields()); j++ {
		bld.Field(j).AppendNull()
	}
	rec := bld.NewRecordBatch()
	t.Cleanup(rec.Release)
	r, err := transport.NewBatchReader(rec, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	return r
}

// A cast column absent from the wire must fail at bind time (FIX-DOC v2 A).
func TestBindResolversErrorsOnMissingCastColumn(t *testing.T) {
	cp, err := core.ParseCastPolicy(map[string]string{"ghost": "string"})
	if err != nil {
		t.Fatalf("cast: %v", err)
	}
	w := &tableWriter{cols: []column{{name: "ghost"}}, cast: cp}
	r := wireReader(t, core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
	}})
	_, err = w.bindResolvers(r)
	if err == nil || !strings.Contains(err.Error(), `"ghost"`) || !strings.Contains(err.Error(), "kind not found") {
		t.Fatalf("missing cast column must error citing the column, got %v", err)
	}
}

// WK-001 C3 (1): a coordinator-assigned batch seq is placed ABOVE the seed,
// so it can never be smaller than a seq already written to the table — a
// raw b.Seq would be, since the coordinator's counter resets at boot.
func TestSeqFromBatchIsAboveSeed(t *testing.T) {
	w := &tableWriter{seed: 100, now: func() time.Time { return time.Unix(0, 0) }}
	if got := w.versionSeq(1); got <= w.seed {
		t.Fatalf("versionSeq(1) = %d, want > seed %d", got, w.seed)
	}
}

// WK-001 C3 (2): increasing batch seqs yield increasing version coordinates,
// which is what ReplacingMergeTree needs per key.
func TestSeqFromBatchIsMonotonic(t *testing.T) {
	w := &tableWriter{seed: 7, now: func() time.Time { return time.Unix(0, 0) }}
	prev := w.versionSeq(1)
	for _, s := range []uint64{2, 3, 10, 11} {
		got := w.versionSeq(s)
		if got <= prev {
			t.Fatalf("versionSeq(%d) = %d, not > previous %d", s, got, prev)
		}
		prev = got
	}
}

// WK-001 C3 (3): with no coordinator (b.Seq == 0) the clock-based nextSeq is
// used, preserving the collapsed behavior. A frozen clock still steps forward
// via the lastSeq guard.
func TestSeqFallsBackToClockWhenNoCoordinator(t *testing.T) {
	frozen := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	w := &tableWriter{now: func() time.Time { return frozen }}
	first := w.versionSeq(0)
	if first != uint64(frozen.UnixNano()) {
		t.Fatalf("versionSeq(0) = %d, want the clock value %d", first, frozen.UnixNano())
	}
	if second := w.versionSeq(0); second != first+1 {
		t.Fatalf("versionSeq(0) again = %d, want %d (guard)", second, first+1)
	}
}

// WK-001 C7: the durable position is the MinSafe across partitions — never
// the argMax (one partition's row) nor the last writer's. A resume must not
// start past the lagging partition.
func TestMinSafePositionAcrossPartitions(t *testing.T) {
	got, err := minSafePosition("postgres", []string{"0/100", "0/2"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "0/2" {
		t.Fatalf("minSafePosition = %q, want 0/2 (the lagging partition)", got)
	}
	// One partition: identical to the legacy single-scalar read.
	if got, err := minSafePosition("postgres", []string{"0/2"}); err != nil || got != "0/2" {
		t.Fatalf("single partition = %q, %v; want 0/2", got, err)
	}
	// The default kind is MySQL GTID (containment order).
	if got, err := minSafePosition("", []string{"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-90", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50"}); err != nil || got != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-50" {
		t.Fatalf("gtid min = %q, %v", got, err)
	}
}
