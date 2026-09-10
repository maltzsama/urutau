package enrich

// E3: the same logical key loads as uint64 from a SQL reference (MySQL
// UNSIGNED) and decodes as int64 from the binlog; a signed/unsigned split
// would make the join never match.

import "testing"

func TestJoinKeySignedUnsignedShareSpace(t *testing.T) {
	if joinKey(int64(5)) != joinKey(uint64(5)) {
		t.Fatalf("int64(5)=%q and uint64(5)=%q must match", joinKey(int64(5)), joinKey(uint64(5)))
	}
	if joinKey(int32(7)) != joinKey(uint32(7)) {
		t.Fatalf("int32(7) and uint32(7) must match")
	}
	if joinKey(int(9)) != joinKey(uint64(9)) {
		t.Fatalf("int(9) and uint64(9) must match")
	}
	// A negative value has no unsigned counterpart: it must NOT collide with
	// an unsigned value's key.
	if joinKey(int64(-1)) == joinKey(uint64(18446744073709551615)) {
		t.Fatal("int64(-1) must not collide with a large uint64")
	}
	// Distinct non-negative values stay distinct.
	if joinKey(int64(5)) == joinKey(uint64(6)) {
		t.Fatal("5 and 6 must not collide")
	}
}

// float32 and float64 of the same literal are DIFFERENT VALUES (0.1f
// upcasts to 0.10000000149011612, not 0.1) and therefore different keys —
// same doctrine as string-vs-int: the cast lives in the reference query's
// SQL, not in silent coercion. Pinning the contract, not proving a bug.
func TestJoinKeyFloatWidthsAreDistinct(t *testing.T) {
	if joinKey(float32(0.1)) == joinKey(float64(0.1)) {
		t.Fatal("float32(0.1) and float64(0.1) must be distinct keys: the values differ, only the int family shares a space")
	}
}
