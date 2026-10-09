package coordinator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/observability"
	"github.com/maltzsama/urutau/position"
)

// #602: the confirmed-position age gauge grows while the minimum committed
// position is stuck, and resets when it advances.
func TestPublishConfirmedAge(t *testing.T) {
	pos, err := position.ParseLSN("0/16B6C50")
	if err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{
		metrics:   observability.New(),
		confirmed: map[string]position.Position{"w": pos},
	}
	base := time.Unix(1000, 0)

	c.publishConfirmedAge(base)
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 0 {
		t.Fatalf("age at first observation = %v, want 0", got)
	}

	c.publishConfirmedAge(base.Add(30 * time.Second))
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 30 {
		t.Fatalf("age after 30s stuck = %v, want 30", got)
	}

	pos2, err := position.ParseLSN("0/16B6C51")
	if err != nil {
		t.Fatal(err)
	}
	c.confirmed["w"] = pos2
	c.publishConfirmedAge(base.Add(45 * time.Second))
	if got := testutil.ToFloat64(c.metrics.ConfirmedPositionAge); got != 0 {
		t.Fatalf("age after the position advanced = %v, want 0", got)
	}
}

// #602: totalOpen counts staged cycles still accumulating or waiting to
// commit.
func TestStagedCyclesTotalOpen(t *testing.T) {
	s := newStagedCycles()
	if s.totalOpen() != 0 {
		t.Fatalf("empty stagedCycles totalOpen = %d, want 0", s.totalOpen())
	}
	s.expect(core.TableRef{Source: "shop.a", Target: "raw.a"}, 1, []string{"w0"})
	if s.totalOpen() != 1 {
		t.Fatalf("totalOpen after one open cycle = %d, want 1", s.totalOpen())
	}
}
