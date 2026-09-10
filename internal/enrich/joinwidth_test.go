package enrich

import (
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/spec"
)

// TestJoinWidthMismatchFailsBoot — the join never coerces. A reference
// whose join column is a string while the event join column is an int
// fails the load loudly; the error names both types and the way out.
func TestJoinWidthMismatchFailsBoot(t *testing.T) {
	cfg := refCfg(nil) // joins user_ref → id
	// Event schema: user_ref is Int64.
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// Reference: id is a STRING.
	if uerr := s.UseLoader("users", fakeRows(t, []map[string]any{
		{"id": "1", "name": "ana", "tier": "gold"},
	})); uerr != nil {
		t.Fatal(uerr)
	}
	s.Start(t.Context())
	t.Cleanup(s.Stop)

	var serr error
	for i := 0; i < 200; i++ {
		if serr = s.refs[0].stickyErr(); serr != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if serr == nil {
		t.Fatal("string reference key vs int event key did not fail the load")
	}
	msg := serr.Error()
	for _, want := range []string{`"id"`, `"user_ref"`, "utf8", "int64", "cast"} {
		if !strings.Contains(strings.ToLower(msg), strings.ToLower(want)) {
			t.Fatalf("error must cite %q: %v", want, serr)
		}
	}
	if s.refs[0].isHot() {
		t.Fatal("reference must NOT go hot on a type mismatch")
	}
}

// TestJoinWidthAlignedPassesBoot — matching types (both int) load fine,
// and the int-width family (uint64 reference, int64 event) still matches
// via normalizeKey once the Arrow types agree at Int64.
func TestJoinWidthAlignedPassesBoot(t *testing.T) {
	cfg := refCfg(nil)
	s, err := New([]spec.Enrich{cfg}, evSchema("id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	// Reference id is int64 → same Arrow type as the event's Int64.
	if uerr := s.UseLoader("users", fakeRows(t, []map[string]any{
		{"id": int64(1), "name": "ana", "tier": "gold"},
	})); uerr != nil {
		t.Fatal(uerr)
	}
	s.Start(t.Context())
	t.Cleanup(s.Stop)
	for i := 0; i < 200; i++ {
		if s.refs[0].isHot() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("aligned types must go hot: %v", s.refs[0].stickyErr())
}

// TestJoinWidthStringKeyBothSides — a string join column on both sides is
// fine (no coercion needed).
func TestJoinWidthStringKeyBothSides(t *testing.T) {
	cfg := refCfg(nil)
	s, err := New([]spec.Enrich{cfg}, evSchemaTyped(map[string]core.Kind{"user_ref": core.KindString}, "id", "user_ref", "q"), nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if uerr := s.UseLoader("users", fakeRows(t, []map[string]any{
		{"id": "u1", "name": "ana", "tier": "gold"},
	})); uerr != nil {
		t.Fatal(uerr)
	}
	s.Start(t.Context())
	t.Cleanup(s.Stop)
	for i := 0; i < 200; i++ {
		if s.refs[0].isHot() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("string-keyed both sides must go hot: %v", s.refs[0].stickyErr())
}
