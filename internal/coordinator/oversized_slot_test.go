package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
)

// unparsablePositions is a source that rejects every position string.
type unparsablePositions struct{ fakeSource }

func (unparsablePositions) ParsePosition(s string) (position.Position, error) {
	return nil, fmt.Errorf("unparsable position %q", s)
}

// oversizedAdmitted reports whether the budget admits one more oversized batch
// for worker within a short wait.
func oversizedAdmitted(c *Coordinator, worker string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	n := c.budget.TotalBytes + 1
	if err := c.budget.Acquire(ctx, worker, n); err != nil {
		return false
	}
	c.budget.Release(worker, n)
	c.budget.ClearOversized(worker)
	return true
}

// An oversized batch takes the budget's single oversized slot. When it fails
// to reach the worker, the slot must be freed even though the worker still
// has ordinary traffic in flight; otherwise every later oversized batch waits
// forever for a slot nothing will release.
func TestUndeliveredOversizedBatchFreesTheOversizedSlot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		deliver func(t *testing.T, c *Coordinator, w *workerState, n int64) error
	}{
		{"unparsable position", func(t *testing.T, c *Coordinator, w *workerState, n int64) error {
			c.src = unparsablePositions{}
			return c.deliverBatch(context.Background(), w, &pb.BatchMeta{Table: "raw.orders", BatchId: 7}, nil, nil, n, false, "not-a-position")
		}},
		{"send cancelled", func(t *testing.T, c *Coordinator, w *workerState, n int64) error {
			for len(w.queue) < cap(w.queue) {
				w.queue <- queuedBatch{}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return c.deliverBatch(ctx, w, &pb.BatchMeta{Table: "raw.orders", BatchId: 7}, nil, nil, n, false, "0/30")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, w := coordHarness()
			ctx := context.Background()
			// Ordinary traffic in flight keeps the worker's charge above zero.
			if err := c.budget.Acquire(ctx, w.name, 64); err != nil {
				t.Fatalf("acquire: %v", err)
			}
			n := c.budget.TotalBytes + 1
			if err := c.budget.Acquire(ctx, w.name, n); err != nil {
				t.Fatalf("acquire oversized: %v", err)
			}
			if err := tc.deliver(t, c, w, n); err == nil {
				t.Fatal("deliverBatch succeeded; the case needs a failed delivery")
			}
			if !oversizedAdmitted(c, w.name) {
				t.Fatal("a later oversized batch is not admitted: the failed delivery left the oversized slot taken")
			}
		})
	}
}
