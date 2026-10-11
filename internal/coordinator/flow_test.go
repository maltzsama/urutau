package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/inflight"
)

// #209 (review): the oversized slot must be released by the ack of the
// oversized batch itself, not by the owner draining — otherwise a worker that
// keeps normal batches in flight holds the slot and blocks every later
// oversized acquisition.
func TestOversizedSlotReleasedOnAckNotOnDrain(t *testing.T) {
	b := inflight.NewBudget(100, 10)
	idx := inflight.NewPositionIndex("run")
	// A normal batch first (fits the floor), then the oversized one.
	if err := b.Acquire(context.Background(), "a", 10); err != nil {
		t.Fatalf("acquire a normal: %v", err)
	}
	if err := b.Acquire(context.Background(), "a", 200); err != nil {
		t.Fatalf("acquire a oversized: %v", err)
	}
	idx.Add(inflight.Batch{ID: 1, Table: "t", High: positionFixture("p1"), Bytes: 200, Oversized: true})
	idx.Add(inflight.Batch{ID: 2, Table: "t", High: positionFixture("p2"), Bytes: 10})

	// The ack covers only the oversized batch (p1), not p2.
	freed, freedOversized, _ := idx.Truncate("t", positionFixture("p1"))
	if freed != 200 || !freedOversized {
		t.Fatalf("truncate = %d, %v; want 200, true", freed, freedOversized)
	}
	b.Release("a", freed)
	b.ClearOversized("a")

	// Worker b's oversized batch must be admitted even though a still holds
	// its normal batch.
	done := make(chan error, 1)
	go func() { done <- b.Acquire(context.Background(), "b", 200) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire b oversized: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("the oversized slot was not released by the oversized batch's ack")
	}
}
