package core

import (
	"strings"
	"testing"
)

// fixedBinary builds a fixed(L) column type shorthand.
func fixedBinary(n int) ColumnType {
	return ColumnType{Kind: KindFixedBinary, FixedSize: n}
}

// 033: fixed widens to variable binary and to string(hex/base64); nothing
// narrows into or out of it.
func TestCheckCastFixedBinary(t *testing.T) {
	cases := []struct {
		from ColumnType
		to   string
		ok   bool
	}{
		{fixedBinary(16), "binary", true},
		{fixedBinary(16), "string(hex)", true},
		{fixedBinary(16), "string(base64)", true},
		{fixedBinary(16), "string", false}, // ambiguous without encoding
		{fixedBinary(16), "int64", false},  // narrowing-ish
		{fixedBinary(16), "uuid", false},
		{ColumnType{Kind: KindBinary}, "binary", true},
	}
	for _, tc := range cases {
		to, err := ParseCastTarget(tc.to)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.to, err)
		}
		err = CheckCast(tc.from, to)
		if tc.ok && err != nil {
			t.Errorf("%v → %s: want allowed, got %v", tc.from, tc.to, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%v → %s: want blocked", tc.from, tc.to)
		}
	}
}

// 033: the cast value converter handles fixed binary → string(hex).
func TestConvertFixedBinaryToString(t *testing.T) {
	to, err := ParseCastTarget("string(hex)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := to.Convert([]byte{0xde, 0xad})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if got != "dead" {
		t.Fatalf("got %q, want dead", got)
	}
}

// 033: a fixed(L) column stringifies with its size and requires a positive
// size to reach Iceberg.
func TestFixedBinaryString(t *testing.T) {
	if got := fixedBinary(16).String(); got != "fixed(16)" {
		t.Fatalf("String = %q, want fixed(16)", got)
	}
}

// 033 §6.2 + 035: an unmappable column with provenance fails validation
// citing type_name and vendor_name; with a cast it resolves.
func TestResolveOpaqueUnknown(t *testing.T) {
	src := Schema{Columns: []Column{
		{Name: "location", Type: ColumnType{Kind: KindUnknown, Opaque: &OpaqueOrigin{TypeName: "point", VendorName: "mysql"}}},
	}}
	p := CastPolicy{}
	if _, _, err := p.Resolve(src); err == nil || !strings.Contains(err.Error(), "mysql point") || !strings.Contains(err.Error(), "location") {
		t.Fatalf("resolve err = %v, want it to cite column and provenance", err)
	}

	// With a declared cast the column lands; provenance is not an error.
	cp, err := ParseCastPolicy(map[string]string{"location": "string"})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	if _, _, err := cp.Resolve(src); err != nil {
		t.Fatalf("resolve with cast: %v", err)
	}
}

// 033 §6.3: the Kind enum is a closed, documented set. This guard fails if
// a Kind is added without an explicit decision recorded — the admission
// criterion requires a source and a sink for every entry.
func TestKindEnumIsClosed(t *testing.T) {
	want := []Kind{
		KindUnknown, KindBool, KindInt32, KindInt64, KindFloat32, KindFloat64,
		KindDecimal, KindString, KindBinary, KindFixedBinary, KindDate, KindTime,
		KindTimestamp, KindTimestampTZ, KindUUID, KindJSON,
		KindStruct, KindList, KindMap,
	}
	if len(want) != int(KindMap)+1 {
		t.Fatalf("Kind set grew to %d entries without a documented decision; every addition needs a source and a sink",
			int(KindMap)+1)
	}
	for i, k := range want {
		if Kind(i) != k {
			t.Fatalf("Kind[%d] = %d, want %d (%s) — the enum order diverged from the documented set", i, Kind(i), k, k)
		}
	}
}
