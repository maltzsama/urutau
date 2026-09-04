package drivers

import "testing"

func TestCapsForKind(t *testing.T) {
	for _, kind := range []string{"mysql", "postgres", "kafka"} {
		caps, err := CapsForKind(kind)
		if err != nil {
			t.Fatalf("CapsForKind(%q): %v", kind, err)
		}
		if !caps.Stream {
			t.Errorf("CapsForKind(%q): Stream = false, want true", kind)
		}
		if len(caps.Modes) == 0 {
			t.Errorf("CapsForKind(%q): Modes empty, want at least ModeCDC", kind)
		}
	}
	if _, err := CapsForKind("oracle"); err == nil {
		t.Error("CapsForKind(oracle) = nil error, want unsupported kind")
	}
}

func TestValidateParallelism(t *testing.T) {
	// Within the ceiling: accepted.
	if err := ValidateParallelism("mysql", 5); err != nil {
		t.Errorf("ValidateParallelism(mysql, 5) = %v, want nil", err)
	}
	// Above the ceiling: rejected.
	if err := ValidateParallelism("mysql", 11); err == nil {
		t.Error("ValidateParallelism(mysql, 11) = nil, want ceiling error")
	}
	// 0 or negative means auto: always accepted.
	if err := ValidateParallelism("mysql", 0); err != nil {
		t.Errorf("ValidateParallelism(mysql, 0) = %v, want nil", err)
	}
	// Unknown kind surfaces the adapter error.
	if err := ValidateParallelism("oracle", 4); err == nil {
		t.Error("ValidateParallelism(oracle, 4) = nil, want unsupported kind")
	}
}
