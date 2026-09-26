package coordinator

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// On a staged table a DBLog window's rows are delivered as the Closes
// marker's cycle (#416): the marker must take a place in the table's send
// order, expecting only its partition's worker, so the window commits after
// every live cycle released ahead of it — never on arrival, where it could
// move the committed position past live cycles still open.
func TestClosesMarkerIsACycleOfTheSendOrder(t *testing.T) {
	c, w0 := coordHarness()
	c.snk = fakeStagedSink{}
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.setRouteForTest("raw.orders", []*workerState{w0, w1})
	c.index["w1"] = newPositionIndex("run-1")
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.setRangesForTest(map[string][]source.Chunk{
		"raw.orders": {{High: []any{int64(100)}}, {Low: []any{int64(100)}}},
	})

	if err := c.sendCloses(context.Background(), w0, "raw.orders", position.MustLSN("0/10"), 7); err != nil {
		t.Fatalf("sendCloses: %v", err)
	}
	var m pb.BatchMeta
	select {
	case q := <-w0.queue:
		if err := proto.Unmarshal(q.meta, &m); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("the marker was not queued for its partition's worker")
	}
	if !m.Staged || m.BatchId == 0 || m.Window == nil || !m.Window.Closes {
		t.Fatalf("marker meta = %+v, want a staged Closes marker with a cycle id", &m)
	}
	if n := c.staged.len(); n != 1 {
		t.Fatalf("%d staged cycles, want the marker's", n)
	}
	// Only w0 owes the window: its delivery alone completes the cycle.
	committable, known := c.staged.deliver(stagedRef("raw.orders", "w0"), m.BatchId, []byte{1}, "0/10", "", nil)
	if !known || len(committable) != 1 {
		t.Fatalf("w0's window delivery: committable=%d known=%v; want the cycle complete", len(committable), known)
	}
}

// An unpartitioned table is not staged: the marker opens no cycle.
func TestClosesMarkerOnAnUnstagedTableOpensNoCycle(t *testing.T) {
	c, w0 := coordHarness()
	c.snk = fakeStagedSink{}
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	if err := c.sendCloses(context.Background(), w0, "raw.orders", position.MustLSN("0/10"), 7); err != nil {
		t.Fatalf("sendCloses: %v", err)
	}
	if n := c.staged.len(); n != 0 {
		t.Fatalf("%d staged cycles for a single-owner table, want 0", n)
	}
}
