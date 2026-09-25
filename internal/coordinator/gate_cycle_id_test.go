package coordinator

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// gateBatch builds a one-row raw.orders batch with key id at pos.
func gateBatch(t *testing.T, id int64, pos string) *dataplane.Batch {
	t.Helper()
	changes := []rowchange.Change{{
		Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{id},
		After: map[string]any{"id": id, "v": "x"}, Position: pos, IngestTS: time.Now(),
	}}
	rec, err := transport.RecordFromChanges(changes, transport.InferSchemaFromChanges(changes), nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	return &dataplane.Batch{Table: "raw.orders", Record: rec, Watermark: []byte(pos), Mode: dataplane.UpsertMode}
}

// gateStagedHarness is a two-partition staged table (id < 100 → w0,
// id >= 100 → w1) with no committed position yet: a first snapshot.
func gateStagedHarness(t *testing.T) *Coordinator {
	t.Helper()
	c, w0 := coordHarness()
	c.snk = fakeStagedSink{}
	w1 := &workerState{name: "w1", attached: true, queue: make(chan queuedBatch, 8)}
	c.workers["w1"] = w1
	c.setRouteForTest("raw.orders", []*workerState{w0, w1})
	c.index["w1"] = newPositionIndex("run-1")
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.setRangesForTest(map[string][]source.Chunk{
		"raw.orders": {{Low: nil, High: []any{int64(100)}}, {Low: []any{int64(100)}, High: nil}},
	})
	return c
}

// Live batches held by the DBLog gate during a snapshot are released
// together by flushWindow (and closeWindow). Each is its own binlog batch and
// must be its own staged cycle. The drain passed one shared BatchMeta to
// enqueueBatch, which assigns the cycle id into it: every held batch got the
// first one's id, the first to complete committed the cycle, and the other
// batches' deliveries arrived for a cycle that no longer existed ("staged
// delivery not committable") and were dropped with their rows.
func TestGateDrainGivesEachHeldBatchItsOwnCycle(t *testing.T) {
	for _, drain := range []struct {
		name string
		run  func(c *Coordinator) error
	}{
		{"flushWindow", func(c *Coordinator) error { return c.flushWindow(context.Background(), "raw.orders", 0, 1) }},
		{"closeWindow", func(c *Coordinator) error { return c.closeWindow(context.Background(), "raw.orders", 0) }},
	} {
		t.Run(drain.name, func(t *testing.T) {
			c := gateStagedHarness(t)
			c.openWindow("raw.orders", 0)
			for _, b := range []*dataplane.Batch{
				gateBatch(t, 1, "0/10"),   // → w0
				gateBatch(t, 150, "0/20"), // → w1
			} {
				if !c.gateHold(context.Background(), b) {
					t.Fatal("gateHold: the open window did not hold the batch")
				}
			}
			if err := drain.run(c); err != nil {
				t.Fatalf("%s: %v", drain.name, err)
			}
			if n := c.staged.len(); n != 2 {
				t.Fatalf("%d staged cycle(s) after draining two held batches; want 2", n)
			}
			ids := map[uint64]bool{}
			for _, name := range []string{"w0", "w1"} {
				select {
				case q := <-c.workers[name].queue:
					var m pb.BatchMeta
					if err := proto.Unmarshal(q.meta, &m); err != nil {
						t.Fatalf("%s meta: %v", name, err)
					}
					ids[m.BatchId] = true
				default:
					t.Fatalf("%s got nothing", name)
				}
			}
			if len(ids) != 2 {
				t.Fatalf("the two held batches went out with cycle ids %v; want two distinct ids", ids)
			}
		})
	}
}
