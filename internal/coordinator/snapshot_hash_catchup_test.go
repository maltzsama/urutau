package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maltzsama/urutau/source"
)

// The fan-out snapshot flushes a chunk's gate and then waits for the reader
// to catch up to the chunk's high watermark. The flush forgot that the
// chunk's rows were in the workers' window, so the gate went back to blocking
// the pump when full: with more catch-up than one gate holds, the pump
// stopped, the reader with it, and the catch-up it was waiting for never came
// (#820). The window stays ready until the chunk is released.
func TestFanoutCatchUpDrainsAFullGate(t *testing.T) {
	c, w0 := coordHarness()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.releaseAllGates()
	go func() {
		for {
			select {
			case q := <-w0.queue:
				c.budget.Release("w0", int64(len(q.body)+len(q.meta)))
			case <-ctx.Done():
				return
			}
		}
	}()

	r := &distributedRelay{c: c, ctx: ctx, ref: source.TableRef{Source: "shop.orders", Target: "raw.orders"}}
	r.GateOn("raw.orders", 7)
	if r.err != nil {
		t.Fatalf("GateOn: %v", r.err)
	}
	c.markWindowReady("raw.orders", 0, 7) // what AddWindowRows does once the chunk's rows are queued
	if err := r.GateFlush(ctx); err != nil {
		t.Fatalf("GateFlush: %v", err)
	}

	// The catch-up: more of the table's live batches than the gate holds.
	const payload = 256 << 10
	n := int(gateMaxBytes/payload) + 16
	held := make(chan struct{})
	go func() {
		defer close(held)
		for i := 1; i <= n; i++ {
			if !c.gateHold(ctx, payloadBatch(t, int64(i), fmt.Sprintf("0/%X", i), payload)) {
				return
			}
		}
	}()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatalf("the pump blocked on the full gate during the chunk's catch-up: %d of %d batches held and nothing drains them",
			c.gate.Len("raw.orders", 0), n)
	}

	// Released, the chunk's window is closing: the gate holds again until
	// the next chunk's rows are in.
	r.endChunkDrain()
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		for i := 1; i <= n; i++ {
			if !c.gateHold(ctx, payloadBatch(t, int64(n+i), fmt.Sprintf("1/%X", i), payload)) {
				return
			}
		}
	}()
	select {
	case <-blocked:
		t.Fatal("the gate drained after the chunk's release, tagging batches for a window that is closing")
	case <-time.After(500 * time.Millisecond):
	}
}
