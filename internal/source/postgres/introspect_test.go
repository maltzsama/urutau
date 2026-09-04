package postgres

import (
	"strings"
	"testing"

	"github.com/maltzsama/urutau/internal/core"
)

// 033/035: an unmappable Postgres type (interval) maps to KindUnknown with
// its provenance — the validation error must be able to name it.
func TestMapColumnTypeOpaqueProvenance(t *testing.T) {
	st := TableState{Columns: []Column{
		{Name: "duration", DataType: "interval"},
		{Name: "loc", DataType: "geometry"},
	}}
	cs, err := CanonicalSchema(&st)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}
	for _, want := range []string{"duration", "loc"} {
		col, ok := cs.Column(want)
		if !ok {
			t.Fatalf("column %s missing", want)
		}
		if col.Type.Kind != core.KindUnknown {
			t.Fatalf("%s mapped to %v, want KindUnknown", want, col.Type.Kind)
		}
		if col.Type.Opaque == nil || col.Type.Opaque.TypeName == "" || col.Type.Opaque.VendorName != "postgres" {
			t.Fatalf("%s opaque = %+v, want provenance with vendor postgres", want, col.Type.Opaque)
		}
	}
}

// 033 §6.2 acceptance: an interval column without a cast fails validation
// (with the provenance in the message); with cast: {col: string} it resolves.
func TestIntervalEscapeValveEndToEnd(t *testing.T) {
	st := TableState{Columns: []Column{{Name: "duration", DataType: "interval"}}}
	cs, err := CanonicalSchema(&st)
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}

	// No cast: Resolve must reject loudly and name the column + provenance.
	var noCast core.CastPolicy
	if _, _, err := noCast.Resolve(cs); err == nil {
		t.Fatal("interval without cast resolved silently")
	} else if !strings.Contains(err.Error(), "duration") || !strings.Contains(err.Error(), "interval") {
		t.Fatalf("error = %q, want it to cite the column and the source type", err)
	}

	// Declared cast to string: resolves.
	cp, err := core.ParseCastPolicy(map[string]string{"duration": "string"})
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	resolved, _, err := cp.Resolve(cs)
	if err != nil {
		t.Fatalf("resolve with cast: %v", err)
	}
	col, _ := resolved.Column("duration")
	if col.Type.Kind != core.KindString {
		t.Fatalf("duration after cast = %v, want string", col.Type.Kind)
	}
}
