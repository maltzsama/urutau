package position

import "testing"

func TestAhead(t *testing.T) {
	global := MustLSN("0/10")
	ps := map[string]Position{
		"behind": MustLSN("0/05"),
		"same":   MustLSN("0/10"),
		"ahead1": MustLSN("0/20"),
		"ahead2": MustLSN("0/30"),
	}
	got := Ahead(global, ps)
	if len(got) != 2 || got[0] != "ahead1" || got[1] != "ahead2" {
		t.Fatalf("Ahead = %v, want [ahead1 ahead2] sorted", got)
	}
	// No global: nothing to compare against.
	if got := Ahead(nil, ps); got != nil {
		t.Fatalf("Ahead(nil, ps) = %v, want nil", got)
	}
	// A nil entry is skipped, not a panic.
	ps["nil"] = nil
	if got := Ahead(global, ps); len(got) != 2 {
		t.Fatalf("Ahead with a nil entry = %v, want 2", got)
	}
}
