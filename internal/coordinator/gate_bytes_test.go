package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The gate was bounded by batch count only. Rows carry payloads (the full
// profile's reach 256 KiB): during one chunk's SELECT the gate of the events
// table held GiBs of Arrow buffers and the coordinator was OOM-killed at 6Gi,
// over and over (#438). Before ChunkReady, a gate holding gateMaxBytes must
// block the pump, however few batches it holds; ChunkReady then drains it.
func TestGateIsBoundedByBytes(t *testing.T) {
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
	n := int(gateMaxBytes/payload) + 16 // past the byte bound, far below gateMaxEvents
	if n >= gateMaxEvents {
		t.Fatalf("test needs fewer than %d batches, has %d", gateMaxEvents, n)
	}
	held := make(chan int, n)
	go func() {
		for i := 1; i <= n; i++ {
			if !c.gateHold(ctx, payloadBatch(t, int64(i), fmt.Sprintf("0/%X", i), payload)) {
				return
			}
			held <- i
		}
	}()
	deadline := time.After(2 * time.Second)
	count := 0
wait:
	for {
		select {
		case <-held:
			count++
			if count == n {
				t.Fatalf("the gate held all %d batches of %d KiB (%d MiB) before ChunkReady; want it to block at %d MiB",
					n, payload>>10, n*payload>>20, gateMaxBytes>>20)
			}
		case <-deadline:
			break wait
		}
	}

	c.markChunkReady("raw.orders", 0, 1)
	deadline = time.After(5 * time.Second)
	for count < n {
		select {
		case <-held:
			count++
		case <-deadline:
			t.Fatalf("ChunkReady did not unblock the full gate: %d of %d batches held", count, n)
		}
	}
}
