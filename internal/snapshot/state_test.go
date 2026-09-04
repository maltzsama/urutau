package snapshot

import (
	"testing"
	"time"
)

// The bounds round trip must preserve the exact Go types: chunk SELECTs
// bind these values into SQL comparisons, and a plain JSON round trip
// would turn every number into float64.
func TestSnapshotProgressRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	in := &SnapshotProgress{
		State: StateInProgress,
		Bounds: [][]any{
			{int64(10), "abc", true},
			{float64(1.5), at},
			{nil},
		},
		Pending: []uint32{2, 3, 7},
		Started: at.Format(time.RFC3339),
	}
	props := EncodeSnapshotProgress(in)
	got, err := ReadSnapshotProgress(props)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.State != StateInProgress {
		t.Fatalf("state = %v", got.State)
	}
	if got.Pending[0] != 2 || got.Pending[1] != 3 || got.Pending[2] != 7 {
		t.Fatalf("pending = %v", got.Pending)
	}
	if len(got.Bounds) != 3 {
		t.Fatalf("bounds rows = %d, want 3", len(got.Bounds))
	}
	if got.Bounds[0][0] != int64(10) {
		t.Fatalf("bounds[0][0] = %v (%T), want int64 10", got.Bounds[0][0], got.Bounds[0][0])
	}
	if got.Bounds[0][1] != "abc" || got.Bounds[0][2] != true {
		t.Fatalf("bounds[0] = %v", got.Bounds[0])
	}
	if got.Bounds[1][0] != float64(1.5) {
		t.Fatalf("bounds[1][0] = %v (%T), want float64 1.5", got.Bounds[1][0], got.Bounds[1][0])
	}
	gotAt, ok := got.Bounds[1][1].(time.Time)
	if !ok || !gotAt.Equal(at) {
		t.Fatalf("bounds[1][1] = %v (%T), want %v", got.Bounds[1][1], got.Bounds[1][1], at)
	}
	if got.Bounds[2][0] != nil {
		t.Fatalf("bounds[2][0] = %v, want nil", got.Bounds[2][0])
	}
	if got.Started != in.Started {
		t.Fatalf("started = %q, want %q", got.Started, in.Started)
	}
}

// Unsupported bound types must not silently corrupt the stored state:
// encoding leaves bounds absent and resume recalculates, which is the
// documented (conservative) behavior.
func TestEncodeSnapshotProgressUnsupportedType(t *testing.T) {
	in := &SnapshotProgress{
		State:   StateInProgress,
		Bounds:  [][]any{{complex(1, 2)}},
		Pending: []uint32{0},
	}
	props := EncodeSnapshotProgress(in)
	if _, ok := props[PropSnapshotBounds]; ok {
		t.Fatal("bounds property present for an unsupported bound type")
	}
	if _, err := ReadSnapshotProgress(props); err != nil {
		t.Fatalf("read with absent bounds: %v", err)
	}
}

// Bounds with driver-native integer variants and []byte cells must survive
// the round trip: chunkers return raw driver types, and a dropped bound
// would silently recalculate on resume.
func TestBoundsRoundTripNativeTypes(t *testing.T) {
	in := &SnapshotProgress{
		State: StateInProgress,
		Bounds: [][]any{
			{int32(1), uint64(2), []byte("abc"), int(7)},
			{int32(9), uint64(10), []byte("xyz"), int(70)},
		},
		Pending: []uint32{0},
	}
	props := EncodeSnapshotProgress(in)
	got, err := ReadSnapshotProgress(props)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Bounds) != 2 {
		t.Fatalf("bounds = %d rows, want 2 (none dropped)", len(got.Bounds))
	}
	row := got.Bounds[0]
	if row[0] != int64(1) { // int32 folds to the canonical int64
		t.Fatalf("bounds[0][0] = %v (%T), want int64 1", row[0], row[0])
	}
	if row[1] != uint64(2) {
		t.Fatalf("bounds[0][1] = %v (%T), want uint64 2", row[1], row[1])
	}
	if row[2] != "abc" { // []byte folds to string
		t.Fatalf("bounds[0][2] = %v (%T), want abc", row[2], row[2])
	}
	if row[3] != int64(7) {
		t.Fatalf("bounds[0][3] = %v (%T), want int64 7", row[3], row[3])
	}
}
