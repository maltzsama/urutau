package coordinator

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"google.golang.org/protobuf/proto"
)

// countingChunkSource is a chunker whose Bounds calls are counted.
type countingChunkSource struct {
	source.ChunkSource
	bounds [][]any
	calls  *atomic.Int64
}

func (f countingChunkSource) Bounds(context.Context) ([][]any, error) {
	f.calls.Add(1)
	return f.bounds, nil
}

// resumeHarness is a single-owner table whose worker answers every chunk at
// once and commits everything it is sent. It records the bounds of each
// chunk it is asked for, and the pending list each Closes marker carries.
type resumeHarness struct {
	c        *Coordinator
	w        *workerState
	snk      *propsSink
	mu       sync.Mutex
	asked    []string
	markers  [][]uint32
	shutdown context.CancelFunc
}

func newResumeHarness(t *testing.T) *resumeHarness {
	c, w := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": {
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}}
	c.setRangesForTest(map[string][]source.Chunk{"raw.orders": {{}}})
	snk := newPropsSink()
	c.snk = snk
	w.out = make(chan *pb.CoordinatorMessage, 64)
	c.chunkReady = make(chan *pb.ChunkReady, 64)
	ctx, cancel := context.WithCancel(context.Background())
	h := &resumeHarness{c: c, w: w, snk: snk, shutdown: cancel}
	go func() {
		for {
			select {
			case m := <-w.out:
				if req := m.GetChunk(); req != nil {
					h.mu.Lock()
					h.asked = append(h.asked, string(req.Bounds))
					h.mu.Unlock()
					c.windowOpen <- &pb.WindowOpen{Table: req.Table, ChunkId: req.ChunkId, Attempt: w.epoch, Seq: uint64(req.ChunkId), Pos: "0/1"}
					c.chunkReady <- &pb.ChunkReady{Table: req.Table, ChunkId: req.ChunkId, Epoch: w.epoch}
				}
			case q := <-w.queue:
				meta := &pb.BatchMeta{}
				if err := proto.Unmarshal(q.meta, meta); err == nil && meta.GetWindow().GetCloses() {
					h.mu.Lock()
					h.markers = append(h.markers, meta.GetWindow().GetSnapshotPending())
					h.mu.Unlock()
				}
				c.budget.release(w.name, int64(len(q.body)+len(q.meta)))
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				commitEverything(c.index["w0"], "raw.orders", position.MustLSN("0/1"))
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		c.releaseAllGates()
	})
	return h
}

func (h *resumeHarness) snapshot(t *testing.T, chunker source.ChunkSource) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- h.c.snapshotTable(context.Background(), caughtUpReader{}, chunker, h.c.refs[0], snapshot.SnapshotConfig{})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("snapshotTable: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the snapshot did not finish")
	}
}

func (h *resumeHarness) askedBounds() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.asked...)
}

var resumeBounds = [][]any{{int64(10)}, {int64(20)}, {int64(30)}} // three chunks, each bound a chunk's low end

// Issue #461: a snapshot records its progress from its start — the bounds,
// the partition count and every chunk pending — and each Closes marker names
// the chunks still to do after its window, which the worker commits with the
// window's rows.
func TestSnapshotRecordsItsProgress(t *testing.T) {
	h := newResumeHarness(t)
	var calls atomic.Int64
	h.snapshot(t, countingChunkSource{bounds: resumeBounds, calls: &calls})

	props, _ := h.snk.Properties(context.Background(), core.TableRef{Target: "raw.orders"})
	sp, err := snapshot.ReadSnapshotProgress(props)
	if err != nil {
		t.Fatal(err)
	}
	if sp.State != snapshot.StateInProgress || len(sp.Bounds) != len(resumeBounds) || props[propSnapshotPartitions] != partitionLayout([]source.Chunk{{}}) {
		t.Fatalf("recorded progress %+v, partitions %q; want in_progress with the bounds of one partition", sp, props[propSnapshotPartitions])
	}
	if !slices.Equal(sp.Pending, []uint32{0, 1, 2}) {
		t.Fatalf("pending at start = %v, want every chunk", sp.Pending)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	want := [][]uint32{{1, 2}, {2}, nil}
	if len(h.markers) != len(want) {
		t.Fatalf("%d Closes markers, want %d", len(h.markers), len(want))
	}
	for i := range want {
		if !slices.Equal(h.markers[i], want[i]) {
			t.Fatalf("marker %d names pending %v, want %v", i, h.markers[i], want[i])
		}
	}
}

// A restarted coordinator resumes an in-progress snapshot from its recorded
// bounds and pending chunks: the committed chunks are not asked for again,
// and the source's bounds are not recomputed.
func TestSnapshotResumesFromItsRecordedProgress(t *testing.T) {
	h := newResumeHarness(t)
	props := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{
		State: snapshot.StateInProgress, Bounds: resumeBounds, Pending: []uint32{1, 2},
	})
	props[propSnapshotPartitions] = partitionLayout([]source.Chunk{{}})
	_ = h.snk.SetProperties(context.Background(), core.TableRef{Target: "raw.orders"}, props)

	var calls atomic.Int64
	h.snapshot(t, countingChunkSource{bounds: [][]any{{int64(99)}}, calls: &calls})

	if calls.Load() != 0 {
		t.Fatalf("Bounds called %d times, want 0: the recorded bounds are reused", calls.Load())
	}
	chunks := snapshot.Chunks(resumeBounds)
	var want []string
	for _, i := range []int{1, 2} {
		b, err := chunkBounds(chunks[i])
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, b)
	}
	if got := h.askedBounds(); !slices.Equal(got, want) {
		t.Fatalf("asked for %d chunks, want only the 2 pending ones", len(got))
	}
}

// Progress recorded under other partition ranges cannot be resumed — its
// chunk ids name chunks clipped to those ranges, and resuming would skip
// rows never read: the snapshot starts over.
func TestSnapshotStartsOverWhenThePartitionsChanged(t *testing.T) {
	h := newResumeHarness(t)
	props := snapshot.EncodeSnapshotProgress(&snapshot.SnapshotProgress{
		State: snapshot.StateInProgress, Bounds: resumeBounds, Pending: []uint32{2},
	})
	props[propSnapshotPartitions] = partitionLayout([]source.Chunk{{High: []any{int64(15)}}, {Low: []any{int64(15)}}})
	_ = h.snk.SetProperties(context.Background(), core.TableRef{Target: "raw.orders"}, props)

	var calls atomic.Int64
	h.snapshot(t, countingChunkSource{bounds: resumeBounds, calls: &calls})

	if calls.Load() != 1 || len(h.askedBounds()) != len(resumeBounds) {
		t.Fatalf("Bounds calls %d, chunks asked %d; want the snapshot started over", calls.Load(), len(h.askedBounds()))
	}
}
