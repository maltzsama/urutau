package core

import "testing"

func TestUUIDTextRoundTrip(t *testing.T) {
	const text = "550e8400-e29b-41d4-a716-446655440000"
	raw, err := ParseUUIDBytes(text)
	if err != nil {
		t.Fatalf("ParseUUIDBytes: %v", err)
	}
	if len(raw) != 16 || raw[0] != 0x55 {
		t.Fatalf("raw = %x, want 16 bytes starting 55", raw)
	}
	back, err := UUIDText(raw)
	if err != nil {
		t.Fatalf("UUIDText: %v", err)
	}
	if back != text {
		t.Fatalf("round-trip = %q, want %q", back, text)
	}

	// Compact and hyphenated forms both parse.
	if _, err := ParseUUIDBytes("550e8400e29b41d4a716446655440000"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := ParseUUIDBytes("not-a-uuid"); err == nil {
		t.Fatal("invalid uuid must error")
	}
	if _, err := UUIDText(make([]byte, 8)); err == nil {
		t.Fatal("UUIDText of an 8-byte slice must error")
	}
}
