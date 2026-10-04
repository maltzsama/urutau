package coordinator

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// chunkRequest is one ChunkRequest a fake worker received.
type chunkRequest struct {
	bounds string
	window uint64
}

// Issue #461: a worker lost mid-snapshot takes its chunk windows with it: the
// rows of every chunk whose Closes marker it had not committed were only in
// its memory. The coordinator no longer ends the run for it: once the worker
// is back, the partition redoes those chunks under fresh window ids (so a
// Closes marker of a lost window, redelivered, can never close a new one) and
// finishes the snapshot.
func TestSnapshotRedoesTheChunksALostWorkerHeld(t *testing.T) {
	c, w := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": {
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}}
	c.sessionErrs = make(chan error, 4)
	w.out = make(chan *pb.CoordinatorMessage, 64)
	c.chunkReady = make(chan *pb.ChunkReady, 64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.releaseAllGates()

	// The worker answers every chunk at once, under its current epoch, and
	// frees its budget; it commits only once commit is set. Each chunk opens
	// one window under a fresh monotonic seq, as the byte-cap reader would.
	var mu sync.Mutex
	var requests []chunkRequest
	var commit bool
	var seq uint64
	go func() {
		for {
			select {
			case m := <-w.out:
				if req := m.GetChunk(); req != nil {
					c.mu.Lock()
					epoch := w.epoch
					c.mu.Unlock()
					s := atomic.AddUint64(&seq, 1)
					mu.Lock()
					requests = append(requests, chunkRequest{bounds: string(req.Bounds), window: s})
					mu.Unlock()
					c.windowOpen <- &pb.WindowOpen{Table: req.Table, ChunkId: req.ChunkId, Attempt: epoch, Seq: s, Pos: "0/1"}
					c.chunkReady <- &pb.ChunkReady{Table: req.Table, ChunkId: req.ChunkId, Epoch: epoch, WindowIds: []uint64{s}}
				}
			case q := <-w.queue:
				c.budget.release(w.name, int64(len(q.body)+len(q.meta)))
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
				mu.Lock()
				on := commit
				mu.Unlock()
				if on {
					commitEverything(c.index["w0"], "raw.orders", position.MustLSN("0/1"))
				}
			}
		}
	}()
	requested := func() []chunkRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]chunkRequest(nil), requests...)
	}
	waitRequests := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for len(requested()) < n {
			if time.Now().After(deadline) {
				t.Fatalf("%d chunk requests, want %d", len(requested()), n)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	ref := c.refs[0]
	chunks := snapshot.Chunks([][]any{{int64(10)}, {int64(20)}, {int64(30)}, {int64(40)}})
	done := make(chan error, 1)
	go func() {
		done <- c.snapshotPartition(ctx, caughtUpReader{}, chunks, ref, source.Chunk{}, 0, w, snapshot.SnapshotConfig{})
	}()

	// Two chunks are in the worker's windows, their Closes markers not
	// committed, when the worker is lost.
	waitRequests(maxUncommittedChunks)
	c.signalSessionEnd(w.name, context.Canceled)
	c.mu.Lock()
	w.attached = false
	c.mu.Unlock()
	select {
	case err := <-c.sessionErrs:
		t.Fatalf("the loss ended the run: %v", err)
	default:
	}

	// The lost windows' Closes markers are redelivered to the restarted
	// worker and acked as empty windows before the snapshot loop sees it
	// back: the chunks to redo must have been taken at the loss.
	mu.Lock()
	commit = true
	mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for c.uncommittedChunks(w.name) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the lost windows' markers were never acked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.mu.Lock()
	w.attached = true
	c.mu.Unlock()
	c.supervisor.noteAttach(w.name)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the snapshot did not finish after the worker came back: %d requests", len(requested()))
	}

	got := requested()
	if len(got) != maxUncommittedChunks+len(chunks) {
		t.Fatalf("%d chunk requests, want %d: the %d lost chunks redone, then the rest", len(got), maxUncommittedChunks+len(chunks), maxUncommittedChunks)
	}
	lostWindows := map[uint64]bool{}
	for i := 0; i < maxUncommittedChunks; i++ {
		lostWindows[got[i].window] = true
	}
	for i, r := range got[maxUncommittedChunks:] {
		want := got[i%len(chunks)].bounds
		if i < len(chunks) {
			b, err := chunkBounds(chunks[i])
			if err != nil {
				t.Fatal(err)
			}
			want = b
		}
		if r.bounds != want {
			t.Fatalf("request %d after the loss asked for another chunk than chunk %d", maxUncommittedChunks+i, i)
		}
		if lostWindows[r.window] {
			t.Fatalf("chunk %d redone under window %d, a lost window's id: its redelivered Closes marker would close it", i, r.window)
		}
	}
}

// chunkBounds encodes a chunk's bounds as a ChunkRequest carries them.
func chunkBounds(ch source.Chunk) (string, error) {
	b, err := transport.EncodeBounds(ch.Low, ch.High)
	return string(b), err
}
