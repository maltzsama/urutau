package mysql

import "testing"

// The decoder cache (issue #117) must not leak state between calls:
// transform.Bytes resets the transformer, and the decode path is
// single-goroutine. Decode the same input repeatedly, and decode a valid
// value right after one that fails (a dangling multi-byte lead byte), to
// prove a reused decoder behaves like a fresh one.
func TestDecodeStringReuseIsStateless(t *testing.T) {
	// A lone Shift-JIS lead byte is not a complete character: the decoder
	// errors and decodeString falls back to the raw bytes.
	_ = decodeString([]byte{0x93}, "sjis_japanese_ci")

	if got := decodeString(sjisInput, "sjis_japanese_ci"); got != "日本語" {
		t.Fatalf("decode after a failed call = %q, want 日本語", got)
	}

	for i := 0; i < 1000; i++ {
		if got := decodeString(latin1Input, "latin1_swedish_ci"); got != "olá, mundo ação" {
			t.Fatalf("latin1 iteration %d = %q, want olá, mundo ação", i, got)
		}
		if got := decodeString(sjisInput, "sjis_japanese_ci"); got != "日本語" {
			t.Fatalf("sjis iteration %d = %q, want 日本語", i, got)
		}
	}
}
