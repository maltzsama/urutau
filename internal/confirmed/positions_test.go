package confirmed

import (
	"testing"
	"time"

	"github.com/maltzsama/urutau/position"
)

func lsn(s string) position.Position { return position.MustLSN(s) }

func mustMin(t *testing.T, p *Positions, owing map[string]bool, dispatched position.Position) position.Position {
	t.Helper()
	got, err := p.Min(owing, dispatched)
	if err != nil {
		t.Fatalf("Min: %v", err)
	}
	return got
}

func TestMinOfNothingIsNil(t *testing.T) {
	var p Positions
	if got := mustMin(t, &p, nil, lsn("0/10")); got != nil {
		t.Fatalf("Min with no worker = %v, want nil", got)
	}
}

// The minimum uses the position's own ordering: "0/10" sorts before "0/2"
// as text, and a text minimum would advance the slot past data in flight.
func TestMinIsTheLaggingOwingWorker(t *testing.T) {
	var p Positions
	p.Record("w0", lsn("0/10"))
	p.Record("w1", lsn("0/2"))
	got := mustMin(t, &p, map[string]bool{"w0": true, "w1": true}, lsn("0/20"))
	if got == nil || got.String() != "0/2" {
		t.Fatalf("Min = %v, want 0/2", got)
	}
}

// An idle worker never acks: constraining the minimum, its position would
// pin the source's retention forever.
func TestAnIdleWorkerDoesNotConstrainTheMinimum(t *testing.T) {
	var p Positions
	p.Record("busy", lsn("0/100"))
	p.Record("idle", lsn("0/10"))
	got := mustMin(t, &p, map[string]bool{"busy": true}, lsn("0/200"))
	if got == nil || got.String() != "0/100" {
		t.Fatalf("Min = %v, want 0/100 (the worker that owes work)", got)
	}
}

func TestAWorkerOwingWithNoPositionHoldsTheMinimum(t *testing.T) {
	var p Positions
	p.Record("w0", lsn("0/100"))
	p.Baseline("w1", nil)
	if got := mustMin(t, &p, map[string]bool{"w0": true, "w1": true}, lsn("0/200")); got != nil {
		t.Fatalf("Min = %v, want nil while w1 owes work it has not committed", got)
	}
}

// With nothing owed every dispatched batch is committed: the result is the
// frontier. Before anything is dispatched it is the boot baseline.
func TestWithNothingOwedTheMinimumIsTheFrontier(t *testing.T) {
	var p Positions
	p.Record("w0", lsn("0/50"))
	p.Record("w1", lsn("0/30"))
	if got := mustMin(t, &p, nil, lsn("0/90")); got == nil || got.String() != "0/90" {
		t.Fatalf("Min = %v, want the dispatched frontier 0/90", got)
	}
	if got := mustMin(t, &p, nil, nil); got == nil || got.String() != "0/30" {
		t.Fatalf("Min with nothing dispatched = %v, want the baseline 0/30", got)
	}
	p.Baseline("w2", nil)
	if got := mustMin(t, &p, nil, nil); got != nil {
		t.Fatalf("Min with an uncommitted worker in the baseline = %v, want nil", got)
	}
}

func TestRecordIgnoresNilAndSeedKeepsWhatIsThere(t *testing.T) {
	var p Positions
	p.Record("w0", nil)
	if p.Has("w0") || p.Len() != 0 {
		t.Fatal("a nil position registered the worker")
	}
	p.Record("w0", lsn("0/10"))
	p.Seed("w0", lsn("0/99"))
	p.Seed("w1", lsn("0/99"))
	if got := p.Of("w0"); got.String() != "0/10" {
		t.Fatalf("Seed overwrote a committed position: %v", got)
	}
	if got := p.Of("w1"); got == nil || got.String() != "0/99" {
		t.Fatalf("Seed did not register the new worker: %v", got)
	}
	p.Forget("w0")
	if p.Has("w0") || p.Len() != 1 {
		t.Fatal("Forget left the worker registered")
	}
}

// The stall clock restarts on forward progress and whenever nothing is owed;
// a position that moves back, or stands still, leaves it running.
func TestStalledSince(t *testing.T) {
	var p Positions
	t0 := time.Unix(1000, 0)
	if at := p.StalledSince(nil, true, t0); !at.Equal(t0) {
		t.Fatalf("pending work with nothing committed: clock = %v, want it started at t0", at)
	}
	if at := p.StalledSince(lsn("0/10"), true, t0.Add(10*time.Second)); !at.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("first confirmed position: clock = %v, want it restarted", at)
	}
	if at := p.StalledSince(lsn("0/10"), true, t0.Add(40*time.Second)); !at.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("no progress: clock = %v, want it still at the last advance", at)
	}
	if at := p.StalledSince(lsn("0/5"), true, t0.Add(50*time.Second)); !at.Equal(t0.Add(10 * time.Second)) {
		t.Fatalf("a position behind the last: clock = %v, want it unchanged", at)
	}
	if at := p.StalledSince(lsn("0/20"), true, t0.Add(60*time.Second)); !at.Equal(t0.Add(60 * time.Second)) {
		t.Fatalf("forward progress: clock = %v, want it restarted", at)
	}
	if at := p.StalledSince(lsn("0/20"), false, t0.Add(90*time.Second)); !at.Equal(t0.Add(90 * time.Second)) {
		t.Fatalf("caught up: clock = %v, want it reset to now", at)
	}
}
