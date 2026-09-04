package mysql

import (
	"testing"
)

func TestNormalize(t *testing.T) {
	if got := normalize([]byte("abc")); got != "abc" {
		t.Fatalf("normalize([]byte) = %v, want string", got)
	}
	if got := normalize(int64(5)); got != int64(5) {
		t.Fatalf("normalize(int64) changed the value")
	}
}

func TestPlaceholders(t *testing.T) {
	if got := placeholders(3); got != "?, ?, ?" {
		t.Fatalf("placeholders(3) = %q", got)
	}
}
