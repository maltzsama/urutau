package routing

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func encode(t *testing.T, key ...any) []byte {
	t.Helper()
	enc, err := EncodeKey(key)
	if err != nil {
		t.Fatalf("EncodeKey(%v): %v", key, err)
	}
	return enc
}

// TestEncodeKeyCrossWidthIntegers pins the property that makes snapshot and
// stream agree: every width and signedness of the same mathematical value
// encodes to the same bytes. dataplane.EncodeKey deliberately does not do
// this (it tags the Arrow width), which is why routing has its own.
func TestEncodeKeyCrossWidthIntegers(t *testing.T) {
	positive := [][]any{
		{int8(5)}, {int16(5)}, {int32(5)}, {int64(5)}, {int(5)},
		{uint8(5)}, {uint16(5)}, {uint32(5)}, {uint64(5)}, {uint(5)},
	}
	want := encode(t, int64(5))
	for _, k := range positive {
		if got := encode(t, k...); !bytes.Equal(got, want) {
			t.Errorf("%v encoded %x, want %x", k, got, want)
		}
	}

	negative := [][]any{
		{int8(-5)}, {int16(-5)}, {int32(-5)}, {int64(-5)}, {int(-5)},
	}
	wantNeg := encode(t, int64(-5))
	for _, k := range negative {
		if got := encode(t, k...); !bytes.Equal(got, wantNeg) {
			t.Errorf("%v encoded %x, want %x", k, got, wantNeg)
		}
	}

	// A signed and an unsigned value only collide when they are the same
	// mathematical value; the sign byte keeps them apart otherwise.
	if bytes.Equal(encode(t, int64(-5)), encode(t, int64(5))) {
		t.Fatal("-5 and 5 encoded equal")
	}
}

// TestEncodeKeyMinInt64 guards the magnitude computation against overflow.
func TestEncodeKeyMinInt64(t *testing.T) {
	enc, err := EncodeKey([]any{int64(-1 << 63)})
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) != 1+4+9 {
		t.Fatalf("len = %d, want %d", len(enc), 1+4+9)
	}
}

// TestEncodeKeyFraming proves length framing prevents tuple-boundary
// collisions in composite keys.
func TestEncodeKeyFraming(t *testing.T) {
	if bytes.Equal(encode(t, int64(1), int64(23)), encode(t, int64(12), int64(3))) {
		t.Fatal("[1,23] and [12,3] encoded equal")
	}
	if bytes.Equal(encode(t, "a", "bc"), encode(t, "ab", "c")) {
		t.Fatal("[a,bc] and [ab,c] encoded equal")
	}
	if bytes.Equal(encode(t, int64(1)), encode(t, "1")) {
		t.Fatal("int 1 and string \"1\" encoded equal")
	}
	if bytes.Equal(encode(t, []byte("a")), encode(t, "a")) {
		t.Fatal("[]byte and string encoded equal")
	}
}

func TestEncodeKeyTypesAndNull(t *testing.T) {
	if _, err := EncodeKey([]any{nil}); err == nil {
		t.Fatal("nil key column accepted")
	}
	if _, err := EncodeKey(nil); err == nil {
		t.Fatal("empty key accepted")
	}
	if _, err := EncodeKey([]any{struct{}{}}); err == nil {
		t.Fatal("unsupported type accepted")
	}
	// Same instant different location must encode equal.
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	utc := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	local := utc.In(tokyo)
	if !bytes.Equal(encode(t, utc), encode(t, local)) {
		t.Fatal("equal instants in different locations encoded differently")
	}
}

func TestSlotStableAndBounded(t *testing.T) {
	for i := 0; i < 1000; i++ {
		key := []any{fmt.Sprintf("key-%d", i)}
		a, err := Slot(key)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Slot(key)
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Fatalf("slot unstable for %v: %d != %d", key, a, b)
		}
		if a >= Slots {
			t.Fatalf("slot %d out of range [0,%d)", a, Slots)
		}
	}
}

// TestOwnerRendezvousMinimalMovement is the property the whole design rests
// on: when a worker joins or leaves, only the keys it gains or loses change
// owner. No other key is remapped (a naive modulo would remap ~everything).
func TestOwnerRendezvousMinimalMovement(t *testing.T) {
	workers4 := []string{"w0", "w1", "w2", "w3"}
	workers5 := []string{"w0", "w1", "w2", "w3", "w4"}
	workers3 := []string{"w0", "w1", "w2"}

	ownerName := func(workers []string, key []any) string {
		i, err := OwnerOfKey(key, workers)
		if err != nil {
			t.Fatal(err)
		}
		if i < 0 {
			t.Fatalf("no owner for %v", key)
		}
		return workers[i]
	}

	moved := 0
	for i := 0; i < 20000; i++ {
		key := []any{fmt.Sprintf("k-%d", i)}
		before := ownerName(workers4, key)
		after := ownerName(workers5, key)
		if before != after {
			moved++
			if after != "w4" {
				t.Fatalf("key %v moved from %s to %s, not to the joining worker w4", key, before, after)
			}
		}
		// Scale-in: a key that was not owned by w3 must be unmoved by w3's
		// removal.
		beforeIn := ownerName(workers4, key)
		afterIn := ownerName(workers3, key)
		if beforeIn != "w3" && beforeIn != afterIn {
			t.Fatalf("key %v moved from %s to %s on an unrelated scale-in", key, beforeIn, afterIn)
		}
	}
	if moved == 0 {
		t.Fatal("no key moved to the joining worker: rendezvous is not distributing")
	}
	// A 4→5 scale should move roughly 1/5 of keys, well under half.
	if moved > 20000/2 {
		t.Fatalf("scale-out moved %d/20000 keys, expected ~1/5", moved)
	}
}

func TestOwnerOrderIndependentAndDistribution(t *testing.T) {
	workers := []string{"alpha", "bravo", "charlie", "delta"}
	shuffled := []string{"delta", "charlie", "bravo", "alpha"}

	counts := map[string]int{}
	for i := 0; i < 4000; i++ {
		key := []any{fmt.Sprintf("k-%d", i)}
		ia, err := OwnerOfKey(key, workers)
		if err != nil {
			t.Fatal(err)
		}
		ib, err := OwnerOfKey(key, shuffled)
		if err != nil {
			t.Fatal(err)
		}
		if workers[ia] != shuffled[ib] {
			t.Fatalf("owner depends on worker order: %s vs %s", workers[ia], shuffled[ib])
		}
		counts[workers[ia]]++
	}
	for _, w := range workers {
		if counts[w] == 0 {
			t.Fatalf("worker %s owns nothing", w)
		}
	}
}

func TestOwnerEmptyAndSingle(t *testing.T) {
	if got := Owner(0, nil); got != -1 {
		t.Fatalf("Owner(empty) = %d, want -1", got)
	}
	if got := Owner(0, []string{"only"}); got != 0 {
		t.Fatalf("Owner(single) = %d, want 0", got)
	}
}
