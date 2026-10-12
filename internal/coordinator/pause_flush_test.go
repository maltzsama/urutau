package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
)

// A re-slice pauses a table and holds its batches until the flip. They were
// then sent one by one: every held source batch, often a few rows of one
// transaction, became a cycle of its own, and a staged table commits one
// Iceberg snapshot per cycle. A minute's pause on a loaded table left it
// thousands of commits behind, the next re-slice timed out draining them and
// paused it again (#828). Held batches go out the way a gate's do:
// concatenated up to the cycle bounds.
func TestBatchesHeldByAPauseAreSentCoalesced(t *testing.T) {
	c, w0 := coordHarness()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := make(chan struct{}, 1024)
	go func() {
		for {
			select {
			case q := <-w0.queue:
				c.budget.Release("w0", int64(len(q.body)+len(q.meta)))
				sent <- struct{}{}
			case <-ctx.Done():
				return
			}
		}
	}()

	const held = 200
	c.pausedMu.Lock()
	c.pauseBuf = map[string][]*dataplane.Batch{}
	for i := 1; i <= held; i++ {
		c.pauseBuf["raw.orders"] = append(c.pauseBuf["raw.orders"], payloadBatch(t, int64(i), fmt.Sprintf("0/%X", i), 64))
	}
	c.pausedMu.Unlock()

	if err := c.flushPaused(ctx); err != nil {
		t.Fatalf("flushPaused: %v", err)
	}
	n := 0
	for done := false; !done; {
		select {
		case <-sent:
			n++
		case <-time.After(300 * time.Millisecond):
			done = true
		}
	}
	if n == 0 {
		t.Fatal("nothing was sent")
	}
	if n > 5 {
		t.Fatalf("%d held batches of a few rows went out as %d batches: one cycle, and one commit, for each", held, n)
	}
}
