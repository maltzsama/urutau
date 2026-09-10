package postgres

import (
	"testing"

	"github.com/maltzsama/urutau/core"
)

// 033/035: an unmappable Postgres type (geometry) maps to KindUnknown with
// its provenance — the validation error must be able to name it.
func TestMapColumnTypeOpaqueProvenance(t *testing.T) {
	st := TableState{Columns: []Column{
		{Name: "loc", DataType: "geometry"},
	}}
	cs, err := CanonicalSchema(&st)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	col, ok := cs.Column("loc")
	if !ok {
		t.Fatal("column loc missing")
	}
	if col.Type.Kind != core.KindUnknown {
		t.Fatalf("loc mapped to %v, want KindUnknown", col.Type.Kind)
	}
	if col.Type.Opaque == nil || col.Type.Opaque.TypeName == "" || col.Type.Opaque.VendorName != "postgres" {
		t.Fatalf("loc opaque = %+v, want provenance with vendor postgres", col.Type.Opaque)
	}
}

// 033 §6.2 acceptance: an interval column maps to KindString (text-encoded
// by pgoutput). A declared cast to string is a no-op (KindString → KindString).
func TestIntervalEscapeValveEndToEnd(t *testing.T) {
	st := TableState{Columns: []Column{{Name: "duration", DataType: "interval"}}}
	cs, err := CanonicalSchema(&st)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}

	// No cast: interval resolves to KindString directly.
	var noCast core.CastPolicy
	resolved, _, err := noCast.Resolve(cs)
	if err != nil {
		t.Fatalf("interval resolve: %v", err)
	}
	col, _ := resolved.Column("duration")
	if col.Type.Kind != core.KindString {
		t.Fatalf("duration = %v, want KindString", col.Type.Kind)
	}
}

// A present but unparseable numeric precision/scale is an error, not a
// silent 0 (which would look like a legitimate default).
func TestParseNumericPrecisionErrors(t *testing.T) {
	if _, _, err := parseNumericPrecision("numeric(bad,2)"); err == nil {
		t.Fatal("unparseable precision must error")
	}
	if _, _, err := parseNumericPrecision("numeric(1,bad)"); err == nil {
		t.Fatal("unparseable scale must error")
	}
	if _, _, err := parseNumericPrecision("numeric(1,2,3)"); err == nil {
		t.Fatal("malformed numeric with extra parts must error")
	}
	// Plain numeric (no precision) is a legitimate default.
	p, s, err := parseNumericPrecision("numeric")
	if err != nil || p != 0 || s != 0 {
		t.Fatalf("plain numeric = %d,%d err=%v, want 0,0", p, s, err)
	}
	// Valid precision/scale parses.
	p, s, err = parseNumericPrecision("numeric(10,2)")
	if err != nil || p != 10 || s != 2 {
		t.Fatalf("numeric(10,2) = %d,%d err=%v", p, s, err)
	}
}
