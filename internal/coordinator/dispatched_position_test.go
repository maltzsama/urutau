package coordinator

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/position"
)

// opaqueSource parses every position as an identity-only one: two different
// values are incomparable.
type opaqueSource struct{ fakeSource }

func (opaqueSource) ParsePosition(s string) (position.Position, error) { return opaquePos(s), nil }

// When the last positions sent to the tables cannot be ordered there is no
// greatest one. Picking whichever the map yields first would make the
// position the source's retention advances to depend on iteration order; the
// frontier is undefined, so retention is held and the reason is logged.
func TestDispatchedPositionIsNilWhenPositionsAreIncomparable(t *testing.T) {
	for range 100 {
		var out bytes.Buffer
		c, _ := coordHarness()
		c.log = slog.New(slog.NewTextHandler(&out, nil))
		c.src = opaqueSource{}
		c.sent.Note("raw.a", "cookie-1")
		c.sent.Note("raw.b", "cookie-2")
		c.sent.Note("raw.c", "cookie-3")
		if got := c.dispatchedPosition(); got != nil {
			t.Fatalf("dispatchedPosition = %v, want nil: the positions have no order", got)
		}
		if !strings.Contains(out.String(), "incomparable") {
			t.Fatalf("no warning about the incomparable positions: %q", out.String())
		}
	}
}

// Comparable positions still reduce to the greatest, whatever the order.
func TestDispatchedPositionIsTheGreatestSent(t *testing.T) {
	for range 100 {
		c, _ := coordHarness()
		c.sent.Note("raw.a", "0/10")
		c.sent.Note("raw.b", "0/40")
		c.sent.Note("raw.c", "0/25")
		c.sent.Note("raw.d", "")
		got := c.dispatchedPosition()
		if got == nil || got.String() != "0/40" {
			t.Fatalf("dispatchedPosition = %v, want 0/40", got)
		}
	}
}

// One table's position repeated on every table is not a conflict.
func TestDispatchedPositionAcceptsEqualOpaquePositions(t *testing.T) {
	c, _ := coordHarness()
	c.src = opaqueSource{}
	c.sent.Note("raw.a", "cookie-1")
	c.sent.Note("raw.b", "cookie-1")
	if got := c.dispatchedPosition(); got == nil || got.String() != "cookie-1" {
		t.Fatalf("dispatchedPosition = %v, want cookie-1", got)
	}
}

// flakyParse fails to parse one table's position.
type flakyParse struct{ fakeSource }

func (flakyParse) ParsePosition(s string) (position.Position, error) {
	if s == "broken" {
		return nil, errors.New("unparsable")
	}
	return position.MustLSN(s), nil
}

// A position that does not parse is skipped, and said: the other tables still
// fold to the frontier.
func TestDispatchedPositionSkipsAndReportsAnUnparsablePosition(t *testing.T) {
	var out bytes.Buffer
	c, _ := coordHarness()
	c.log = slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c.src = flakyParse{}
	c.sent.Note("raw.a", "0/10")
	c.sent.Note("raw.b", "broken")
	c.sent.Note("raw.c", "0/30")
	got := c.dispatchedPosition()
	if got == nil || got.String() != "0/30" {
		t.Fatalf("dispatchedPosition = %v, want 0/30", got)
	}
	if !strings.Contains(out.String(), "raw.b") || !strings.Contains(out.String(), "does not parse") {
		t.Fatalf("the unparsable position was not reported: %q", out.String())
	}
}
