package coordinator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// A WindowOpen says the window's rows are in the worker's window, so from
// then on a full gate drains tagged instead of blocking the pump (#435). The
// range path marked the window ready only after its catch-up (since #622):
// during the catch-up a full gate blocked the pump, the reader stopped behind
// it and never reached the window's position, and the run ended on the
// window timeout (#821).
func TestRangeCatchUpDrainsAFullGate(t *testing.T) {
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
	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}
	c.openWindow(ref.Target, 0)

	// The window is announced; its catch-up does not end by itself.
	reader := &neverCaughtReader{polled: make(chan struct{})}
	lost := make(chan struct{})
	cfg := snapshot.SnapshotConfig{CaughtUpPoll: time.Millisecond, WindowTimeout: time.Minute}
	wo := &pb.WindowOpen{Table: ref.Source, Seq: 1, Pos: "0/10"}
	closed := make(chan error, 1)
	go func() { closed <- c.closeOneWindow(ctx, reader, ref, 0, w0, cfg, wo, lost, nil, 0, nil) }()
	<-reader.polled

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
		t.Fatalf("the pump blocked on the full gate during the window's catch-up: %d of %d batches held and nothing drains them",
			c.gate.Len(ref.Target, 0), n)
	}

	close(lost)
	select {
	case err := <-closed:
		if !errors.Is(err, errWorkerLost) {
			t.Fatalf("closeOneWindow = %v, want errWorkerLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closeOneWindow did not end on the worker's loss")
	}
}
