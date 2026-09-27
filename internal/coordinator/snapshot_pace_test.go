package coordinator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// caughtUpReader is a source reader always caught up at 0/1.
type caughtUpReader struct{ source.SourceReader }

func (caughtUpReader) Master(context.Context) (position.Position, error) {
	return position.MustLSN("0/1"), nil
}
func (caughtUpReader) Synced() position.Position { return position.MustLSN("0/1") }

// Once #447 made chunk SELECTs fast, the coordinator requested a chunk every
// ~1.5 s while the events worker committed far slower: its heap held ~6
// chunks in windows and ~6 more awaiting commit, and it was OOM-killed at 5Gi
// in every snapshot (#452). The next chunk must wait while
// maxUncommittedChunks earlier chunks are not committed.
func TestSnapshotChunksWaitForCommits(t *testing.T) {
	c, w := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": {
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}}
	w.out = make(chan *pb.CoordinatorMessage, 64)
	c.chunkReady = make(chan *pb.ChunkReady, 64)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer c.releaseAllGates()

	// The worker: answers every chunk at once, takes what is queued (freeing
	// its budget), and acks nothing: its commits are slow.
	var requested atomic.Int64
	go func() {
		for {
			select {
			case m := <-w.out:
				if req := m.GetChunk(); req != nil {
					requested.Add(1)
					c.chunkReady <- &pb.ChunkReady{Table: req.Table, ChunkId: req.ChunkId}
				}
			case q := <-w.queue:
				c.budget.release(w.name, int64(len(q.body)+len(q.meta)))
			case <-ctx.Done():
				return
			}
		}
	}()

	ref := c.refs[0]
	chunks := snapshot.Chunks([][]any{{int64(10)}, {int64(20)}, {int64(30)}, {int64(40)}, {int64(50)}})
	done := make(chan error, 1)
	go func() {
		done <- c.snapshotPartition(ctx, caughtUpReader{}, chunks, ref, source.Chunk{}, 0, w, snapshot.SnapshotConfig{})
	}()

	// Wait for the paced requests, then long enough for any extra one to
	// show: a fixed sleep alone fails on a slow machine (review of #453).
	deadline := time.Now().Add(5 * time.Second)
	for requested.Load() < maxUncommittedChunks && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if got := requested.Load(); got != maxUncommittedChunks {
		t.Fatalf("%d of %d chunks requested while none was committed; want %d", got, len(chunks), maxUncommittedChunks)
	}

	// The worker commits everything: the rest of the snapshot proceeds.
	go func() {
		for {
			c.index["w0"].truncate("raw.orders", position.MustLSN("0/1"))
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the snapshot did not finish once the chunks were committed: %d requested", requested.Load())
	}
	if got := requested.Load(); got != int64(len(chunks)) {
		t.Fatalf("%d chunks requested, want %d", got, len(chunks))
	}
}
