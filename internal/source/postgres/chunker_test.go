package postgres

import (
	"testing"
)

// normalize folds driver-native []byte cells into strings so bounds and
// rows share one scalar mapping.
func TestNormalize(t *testing.T) {
	if got := normalize([]byte("abc")); got != "abc" {
		t.Fatalf("normalize([]byte) = %v, want string", got)
	}
	if got := normalize(int64(5)); got != int64(5) {
		t.Fatalf("normalize(int64) changed the value")
	}
}

func TestQuotedList(t *testing.T) {
	got := quotedList([]string{"id", "v"})
	want := `"id", "v"`
	if got != want {
		t.Fatalf("quotedList = %q, want %q", got, want)
	}
	// Embedded quotes are doubled.
	got = quotedList([]string{`we"ird`})
	if got != `"we""ird"` {
		t.Fatalf("quotedList with quote = %q", got)
	}
}

func TestQuoteIdent(t *testing.T) {
	if quoteIdent("orders") != `"orders"` {
		t.Fatalf("quoteIdent = %q", quoteIdent("orders"))
	}
}

func TestPlaceholders(t *testing.T) {
	if got := placeholders(1, 2); got != "$1, $2" {
		t.Fatalf("placeholders(1,2) = %q", got)
	}
	if got := placeholders(3, 1); got != "$3" {
		t.Fatalf("placeholders(3,1) = %q", got)
	}
}
