package coordinator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// backpressuredReader mirrors the MySQL reader's backpressure: its synced
// position advances only as its batches are pulled (canal's OnRow blocks on a
// full buffer, and canal's synced GTID set with it).
type backpressuredReader struct {
	source.Reader
	batches chan *dataplane.Batch
	mu      sync.Mutex
	synced  position.Position
}

func newBackpressuredReader(t *testing.T, n int) *backpressuredReader {
	r := &backpressuredReader{batches: make(chan *dataplane.Batch, n), synced: position.MustLSN("0/0")}
	for i := 1; i <= n; i++ {
		r.batches <- gateBatch(t, int64(i), fmt.Sprintf("0/%X", i))
	}
	return r
}

func (r *backpressuredReader) Next(ctx context.Context) (*dataplane.Batch, error) {
	select {
	case b := <-r.batches:
		r.mu.Lock()
		r.synced = position.MustLSN(string(b.Watermark))
		r.mu.Unlock()
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *backpressuredReader) Synced() position.Position {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced
}

// A DBLog window waits for the reader to reach the chunk's high watermark,
// while the pump holds the windowed table's live batches in the gate. Past
// gateMaxEvents the pump blocked; the reader, which only advances as the
// pump pulls, stopped short of the watermark; the window stayed open until
// its timeout and the coordinator restarted the run — every time, on a table
// busy enough to fill the gate during one catch-up (the full production-
// readiness profile looped on it). The reader must reach the watermark.
func TestWindowCatchUpDoesNotStallOnAFullGate(t *testing.T) {
	c, w0 := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.releaseAllGates()

	// The worker session: take each queued batch and free its budget, as
	// the worker's acks would; record the window tags in arrival order.
	var mu sync.Mutex
	var got []*pb.BatchMeta
	go func() {
		for {
			select {
			case q := <-w0.queue:
				c.budget.release("w0", int64(len(q.body)+len(q.meta)))
				m := &pb.BatchMeta{}
				if err := proto.Unmarshal(q.meta, m); err == nil {
					mu.Lock()
					got = append(got, m)
					mu.Unlock()
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	n := gateMaxEvents + 200
	rdr := newBackpressuredReader(t, n)
	c.openWindow("raw.orders", 0)
	c.markWindowReady("raw.orders", 0, 7) // the worker holds chunk 7's rows
	out, _ := sourceBatches(ctx, rdr)
	go c.pump(ctx, out)

	high := position.MustLSN(fmt.Sprintf("0/%X", n))
	err := snapshot.WaitCaughtUp(ctx, rdr, high, snapshot.SnapshotConfig{WindowTimeout: 3 * time.Second, CaughtUpPoll: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("the reader never reached the window's watermark: %v", err)
	}

	// What drained early went out as the window's own events, in source order.
	mu.Lock()
	defer mu.Unlock()
	if len(got) == 0 {
		t.Fatal("nothing drained from the full gate")
	}
	var prev position.Position
	for i, m := range got {
		if m.Window == nil || !m.Window.InWindow || m.Window.WindowId != 7 {
			t.Fatalf("drained batch %d: window %+v, want InWindow for chunk 7", i, m.Window)
		}
		p := position.MustLSN(m.HighPos)
		if prev != nil && p.Compare(prev) <= 0 {
			t.Fatalf("drained batch %d at %s after %s: out of source order", i, p, prev)
		}
		prev = p
	}
}

// Before ChunkReady the chunk's rows are not in the worker's window yet: a
// full gate must wait (no early drain), and its ChunkReady must wake the
// pump, which then drains and lets the reader reach the watermark.
func TestFullGateWaitsForChunkReadyThenDrains(t *testing.T) {
	c, w0 := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.releaseAllGates()
	var sent atomic.Int64
	go func() {
		for {
			select {
			case q := <-w0.queue:
				c.budget.release("w0", int64(len(q.body)+len(q.meta)))
				sent.Add(1)
			case <-ctx.Done():
				return
			}
		}
	}()

	n := gateMaxEvents + 200
	rdr := newBackpressuredReader(t, n)
	c.openWindow("raw.orders", 0)
	out, _ := sourceBatches(ctx, rdr)
	go c.pump(ctx, out)

	high := position.MustLSN(fmt.Sprintf("0/%X", n))
	cfg := snapshot.SnapshotConfig{WindowTimeout: 500 * time.Millisecond, CaughtUpPoll: 10 * time.Millisecond}
	if err := snapshot.WaitCaughtUp(ctx, rdr, high, cfg); err == nil {
		t.Fatal("the reader passed a full gate before the chunk was ready")
	}
	if n := sent.Load(); n != 0 {
		t.Fatalf("%d batch(es) left the gate before ChunkReady", n)
	}

	c.markWindowReady("raw.orders", 0, 3)
	cfg.WindowTimeout = 3 * time.Second
	if err := snapshot.WaitCaughtUp(ctx, rdr, high, cfg); err != nil {
		t.Fatalf("ChunkReady did not unblock the pump: %v", err)
	}
}
