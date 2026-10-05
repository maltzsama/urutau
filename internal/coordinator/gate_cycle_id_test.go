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

// gateStagedHarness is a two-partition staged table (two owners, w0 and w1,
// keys split by the rendezvous hash) with no committed position yet: a first
// snapshot. Tests that need a key on a given owner use ownerKey.
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
// together by flushWindow (and closeWindow). The drain once passed one shared
// BatchMeta to enqueueBatch, which assigns the cycle id into it: every held
// batch got the first one's id, the first to complete committed the cycle,
// and the other batches' deliveries arrived for a cycle that no longer
// existed ("staged delivery not committable") and were dropped with their
// rows. Since #437 the held batches go out concatenated, as one cycle split
// by owner; what must hold is the same: every delivery belongs to a cycle
// that expects it, and delivering them commits every cycle.
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
			names := []string{"w0", "w1"}
			for _, b := range []*dataplane.Batch{
				gateBatch(t, ownerKey(t, names, 0), "0/10"), // → w0
				gateBatch(t, ownerKey(t, names, 1), "0/20"), // → w1
			} {
				if !c.gateHold(context.Background(), b) {
					t.Fatal("gateHold: the open window did not hold the batch")
				}
			}
			if err := drain.run(c); err != nil {
				t.Fatalf("%s: %v", drain.name, err)
			}
			type delivery struct {
				worker string
				id     uint64
				pos    string
			}
			var got []delivery
			for _, name := range []string{"w0", "w1"} {
				select {
				case q := <-c.workers[name].queue:
					var m pb.BatchMeta
					if err := proto.Unmarshal(q.meta, &m); err != nil {
						t.Fatalf("%s meta: %v", name, err)
					}
					got = append(got, delivery{name, m.BatchId, m.HighPos})
				default:
					t.Fatalf("%s got nothing", name)
				}
			}
			committed := 0
			for _, d := range got {
				cycles, known := c.staged.deliver(stagedRef("raw.orders", d.worker), d.id, []byte{1}, d.pos, "", nil)
				if !known {
					t.Fatalf("%s's delivery for cycle %d is unknown: it would be dropped with its rows", d.worker, d.id)
				}
				committed += len(cycles)
			}
			if committed == 0 || c.staged.len() != 0 {
				t.Fatalf("after every delivery: %d cycle(s) committed, %d still open; want all committed", committed, c.staged.len())
			}
		})
	}
}
