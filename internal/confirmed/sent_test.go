package confirmed

import (
	"errors"
	"testing"

	"github.com/maltzsama/urutau/position"
)

func parseLSN(s string) (position.Position, error) {
	if s == "broken" {
		return nil, errors.New("unparsable")
	}
	return position.MustLSN(s), nil
}

func TestSentKeepsTheLatestPositionOfATable(t *testing.T) {
	var s Sent
	if got := s.Of("raw.a"); got != "" {
		t.Fatalf("position before any send = %q, want none", got)
	}
	s.Note("raw.a", "0/10")
	s.Note("raw.a", "")
	if got := s.Of("raw.a"); got != "0/10" {
		t.Fatalf("position = %q, want 0/10: a batch without a position records none", got)
	}
	s.Note("raw.a", "0/20")
	if got := s.Of("raw.a"); got != "0/20" {
		t.Fatalf("position = %q, want 0/20", got)
	}
}

// The frontier is the greatest position sent, whatever the map's order; a
// position that does not parse is skipped and reported.
func TestSentFrontierIsTheGreatestSent(t *testing.T) {
	var s Sent
	if got, err := s.Frontier(parseLSN, nil); got != nil || err != nil {
		t.Fatalf("frontier before any send = %v, %v; want none", got, err)
	}
	s.Note("raw.a", "0/10")
	s.Note("raw.b", "broken")
	s.Note("raw.c", "0/40")
	s.Note("raw.d", "0/25")
	for range 50 {
		var skipped []string
		got, err := s.Frontier(parseLSN, func(table, _ string, _ error) { skipped = append(skipped, table) })
		if err != nil || got == nil || got.String() != "0/40" {
			t.Fatalf("frontier = %v, %v; want 0/40", got, err)
		}
		if len(skipped) != 1 || skipped[0] != "raw.b" {
			t.Fatalf("skipped = %v, want raw.b", skipped)
		}
	}
}
