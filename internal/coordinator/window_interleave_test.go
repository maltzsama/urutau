package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// The DBLog window invariant, golden against the current 1-window-per-chunk
// code before the byte-cap reader (PR 1) changes the window granularity: the
// window's position is captured AFTER the last batch was read, never before.
//
// If the position were captured before the read, a live UPDATE E committed
// between the read and the caught-up proof would be missed by the window, and
// the snapshot's pre-E row could be emitted over E's after-image. The two
// variants below are the only ways E can land relative to the window's
// position; both must end with E's after-image in the destination.
//
// The coordinator owns the order and the tags of what reaches the worker, not
// the merge itself (the worker's window does that), so the assertion is on
// the emitted stream: the ordering/tagging that makes the worker's merge
// converge on post-E.

// interleaveReader is a source reader whose caught-up position is fixed. The
// position is injected by construction, so the caught-up proof is a predicate
// over position, never a wait. The window's position now arrives in the
// worker's WindowOpen (captured after the read — the invariant under test),
// so the reader's Master is never consulted here.
type interleaveReader struct {
	source.SourceReader
	pos position.Position
}

func (r interleaveReader) Synced() position.Position { return r.pos }

// interleaveHarness is a one-worker, one-table, unpartitioned, unstaged
// coordinator ready to drive a single snapshotChunk round-trip by hand.
func interleaveHarness(t *testing.T) (*Coordinator, *workerState) {
	t.Helper()
	c, w := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": {
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}}
	w.out = make(chan *pb.CoordinatorMessage, 8)
	w.queue = make(chan queuedBatch, 64)
	c.chunkReady = make(chan *pb.ChunkReady, 8)
	c.windowOpen = make(chan *pb.WindowOpen, 8)
	return c, w
}

// interleaveUpdate is the live UPDATE E: one row, pk=id, after-image v, at
// source position pos.
func interleaveUpdate(t *testing.T, id int64, v, pos string) *dataplane.Batch {
	t.Helper()
	changes := []rowchange.Change{{
		Op: rowchange.OpUpdate, Table: "raw.orders", Key: []any{id},
		After: map[string]any{"id": id, "v": v}, Position: pos, IngestTS: time.Now(),
	}}
	rec, err := transport.RecordFromChanges(changes, transport.InferSchemaFromChanges(changes), nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	return &dataplane.Batch{Table: "raw.orders", Record: rec, Watermark: []byte(pos), Mode: dataplane.UpsertMode}
}

// runSnapshotRoundTrip drives one snapshotChunk round-trip: the worker reads
// the chunk, captures pos AFTER the read (WindowOpen.pos), and reports one
// window (seq 7) then ChunkReady. The coordinator proves caught up to pos and
// flushes + closes the window.
func runSnapshotRoundTrip(t *testing.T, c *Coordinator, w *workerState, rdr interleaveReader, chunkID uint32, pos string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		m := <-w.out
		if req := m.GetChunk(); req != nil {
			c.windowOpen <- &pb.WindowOpen{Table: req.Table, ChunkId: req.ChunkId, Attempt: w.epoch, Seq: 7, Pos: pos}
			c.chunkReady <- &pb.ChunkReady{Table: req.Table, ChunkId: req.ChunkId, Epoch: w.epoch, WindowIds: []uint64{7}}
		}
	}()

	lost := c.lostSignal(w)
	ch := source.Chunk{Low: []any{int64(1)}, High: []any{int64(2)}}
	return c.snapshotChunk(ctx, rdr, c.refs[0], 0, w, snapshot.SnapshotConfig{}, ch, chunkID, 0, w.epoch, lost, nil, map[uint64]int{})
}

// queuedOut is one batch the worker received, with its meta and its Arrow IPC
// body, in arrival order.
type queuedOut struct {
	meta *pb.BatchMeta
	body []byte
}

// drainQueued reads everything queued for the worker right now, in order.
func drainQueued(t *testing.T, w *workerState) []queuedOut {
	t.Helper()
	var out []queuedOut
	for {
		select {
		case q := <-w.queue:
			m := &pb.BatchMeta{}
			if err := proto.Unmarshal(q.meta, m); err != nil {
				t.Fatal(err)
			}
			out = append(out, queuedOut{meta: m, body: q.body})
		default:
			return out
		}
	}
}

// batchRows decodes a queued batch's body into (id, v) pairs.
func batchRows(t *testing.T, body []byte) [][2]string {
	t.Helper()
	if len(body) == 0 {
		return nil
	}
	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("decode batch body: %v", err)
	}
	defer r.Release()
	var out [][2]string
	for r.Next() {
		br, err := transport.NewBatchReader(r.RecordBatch(), []string{"id"})
		if err != nil {
			t.Fatalf("batch reader: %v", err)
		}
		for i := 0; i < br.NumRows(); i++ {
			key := br.Key(i)
			id := ""
			if len(key) > 0 && key[0] != nil {
				id = fmt.Sprintf("%v", key[0])
			}
			v, _ := br.Value("v", i)
			out = append(out, [2]string{id, fmt.Sprintf("%v", v)})
		}
	}
	return out
}

// assertPostEWins is the shared final assertion of both variants: the last
// write the stream delivers for pk=1 is E's after-image. The variant-specific
// ordering below is what makes this true.
func assertPostEWins(t *testing.T, out []queuedOut) {
	t.Helper()
	lastV := ""
	for _, o := range out {
		for _, r := range batchRows(t, o.body) {
			if r[0] == "1" {
				lastV = r[1]
			}
		}
	}
	if lastV != "post-E" {
		t.Fatalf("the last write to pk=1 is %q, want the after-image post-E", lastV)
	}
}

// Variant (a): P_E <= Pos. The read saw pre-E, and E lands inside the
// window's merge — E is InWindow-tagged ahead of the Closes marker, so the
// worker's merge drops the stale pre-E row and keeps post-E.
func TestWindowInterleaveWithinTheWindow(t *testing.T) {
	c, w := interleaveHarness(t)
	defer c.releaseAllGates()

	c.openWindow("raw.orders", 0)
	e := interleaveUpdate(t, 1, "post-E", "0/5") // P_E = 0/5 <= Pos = 0/10
	if !c.gateHold(context.Background(), e) {
		t.Fatal("the open window did not hold the live update")
	}

	rdr := interleaveReader{pos: position.MustLSN("0/10")}
	if err := runSnapshotRoundTrip(t, c, w, rdr, 7, "0/10"); err != nil {
		t.Fatalf("snapshot round-trip: %v", err)
	}

	out := drainQueued(t, w)
	if len(out) != 2 {
		t.Fatalf("worker got %d batches, want the InWindow update then the Closes marker", len(out))
	}
	if !out[0].meta.Window.GetInWindow() || out[0].meta.Window.WindowId != 7 {
		t.Fatalf("first batch window = %+v, want the live update InWindow-tagged for chunk 7", out[0].meta.Window)
	}
	if !out[1].meta.Window.GetCloses() {
		t.Fatalf("second batch window = %+v, want the Closes marker", out[1].meta.Window)
	}
	assertPostEWins(t, out)
}

// Variant (b): P_E > Pos. The flush emits pre-E first, then E applies on the
// live path after it — the Closes marker precedes the untagged E. A flush out
// of order would put pre-E after post-E; the ordering here prevents that.
func TestWindowInterleaveAfterTheWindow(t *testing.T) {
	c, w := interleaveHarness(t)
	defer c.releaseAllGates()

	c.openWindow("raw.orders", 0)
	rdr := interleaveReader{pos: position.MustLSN("0/10")}
	if err := runSnapshotRoundTrip(t, c, w, rdr, 7, "0/10"); err != nil {
		t.Fatalf("snapshot round-trip: %v", err)
	}

	// E commits after the window's position: gated now, released untagged at
	// closeWindow, behind the Closes marker.
	e := interleaveUpdate(t, 1, "post-E", "0/20") // P_E = 0/20 > Pos = 0/10
	if !c.gateHold(context.Background(), e) {
		t.Fatal("the open window did not hold the trailing live update")
	}
	if err := c.closeWindow(context.Background(), "raw.orders", 0); err != nil {
		t.Fatalf("closeWindow: %v", err)
	}

	out := drainQueued(t, w)
	if len(out) != 2 {
		t.Fatalf("worker got %d batches, want the Closes marker then the untagged update", len(out))
	}
	if !out[0].meta.Window.GetCloses() {
		t.Fatalf("first batch window = %+v, want the Closes marker", out[0].meta.Window)
	}
	if w := out[1].meta.Window; w != nil && (w.GetInWindow() || w.GetCloses()) {
		t.Fatalf("second batch window = %+v, want an untagged live update", w)
	}
	assertPostEWins(t, out)
}
