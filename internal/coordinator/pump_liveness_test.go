package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/maltzsama/urutau/dataplane"
)

// The liveness probe read a pump that had not looped for 30s as wedged. A
// pump held by backpressure does not loop either: while a chunk's SELECT runs
// the gate fills and the pump waits for ChunkReady, by design. On a table
// whose chunk takes longer than the probe allows, the kubelet restarted the
// coordinator in the middle of every attempt at that chunk and the snapshot
// never got past it (#819).
func TestPumpHeldByBackpressureIsNotWedged(t *testing.T) {
	c, w0 := coordHarness()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.releaseAllGates()
	go func() {
		for {
			select {
			case q := <-w0.queue:
				c.budget.release("w0", int64(len(q.body)+len(q.meta)))
			case <-ctx.Done():
				return
			}
		}
	}()
	c.openWindow("raw.orders", 0)

	const payload = 256 << 10
	n := int(gateMaxBytes/payload) + 16 // past the gate's byte bound
	out := make(chan *dataplane.Batch)
	go c.pump(ctx, out)
	fed := make(chan struct{})
	go func() {
		defer close(fed)
		for i := 1; i <= n; i++ {
			select {
			case out <- payloadBatch(t, int64(i), fmt.Sprintf("0/%X", i), payload):
			case <-ctx.Done():
				return
			}
		}
	}()

	// The gate fills and the pump blocks in it.
	deadline := time.Now().Add(5 * time.Second)
	for c.gate.Len("raw.orders", 0) < int(gateMaxBytes/payload) {
		if time.Now().After(deadline) {
			t.Fatalf("the gate never filled: holds %d batches", c.gate.Len("raw.orders", 0))
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // the pump takes the next batch and waits on the full gate

	s := dashState{c}
	// As a probe sees it once the chunk has been running for a minute.
	c.lastPump.Store(time.Now().Add(-time.Minute).UnixNano())
	if !s.Healthy() {
		t.Fatal("a pump waiting on the full gate for a chunk reads as wedged: the liveness probe would restart the coordinator mid-snapshot")
	}

	// Backpressure is bounded by the snapshot watchdog; past it the pump is wedged.
	if at := c.pumpHeld.Load(); at == 0 {
		t.Fatal("the pump does not report being held")
	}
	c.pumpHeld.Store(time.Now().Add(-2 * c.pumpHeldLimit()).UnixNano())
	if s.Healthy() {
		t.Fatal("a pump held past the snapshot watchdog still reads as healthy")
	}

	// Held that long with acks still arriving, the pipeline is slow, not
	// wedged: a table committing at its own pace keeps the budget full and
	// the pump behind it for as long as its backlog lasts. Restarted there,
	// the coordinator replayed the backlog from its resume point and took
	// longer still (#826).
	c.lastProgress.Store(time.Now().UnixNano())
	if !s.Healthy() {
		t.Fatal("a pump held past the limit reads as wedged although a worker has just acked")
	}
	c.lastProgress.Store(time.Now().Add(-2 * c.pumpHeldLimit()).UnixNano())
	if s.Healthy() {
		t.Fatal("a pump held past the limit with no ack since still reads as healthy")
	}

	c.markWindowReady("raw.orders", 0, 1)
	select {
	case <-fed:
	case <-time.After(5 * time.Second):
		t.Fatal("ChunkReady did not unblock the pump")
	}
	deadline = time.Now().Add(5 * time.Second)
	for !s.Healthy() {
		if time.Now().After(deadline) {
			t.Fatal("the pump does not read as healthy once it loops again")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
