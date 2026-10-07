package proc

import (
	"bufio"
	"strings"
	"testing"
)

// A newline-less line past the cap must error, not grow without bound
// (issue #576).
func TestReadLineCapsGrowth(t *testing.T) {
	br := bufio.NewReaderSize(strings.NewReader(strings.Repeat("a", maxLineBytes+1)), 4096)
	if _, err := readLine(br); err == nil {
		t.Fatal("readLine returned no error for a line over the cap")
	}

	br = bufio.NewReaderSize(strings.NewReader("hello\nrest"), 16)
	got, err := readLine(br)
	if err != nil || got != "hello" {
		t.Fatalf("readLine = %q, %v", got, err)
	}
}
