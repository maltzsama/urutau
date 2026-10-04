package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// slowCatchUpReader never catches up to the window's position: its synced
// position stays behind, so the caught-up proof polls until the worker's loss
// aborts it.
type slowCatchUpReader struct {
	source.SourceReader
	entered chan struct{} // closed on the first Synced: the caught-up wait started
}

func (r slowCatchUpReader) Synced() position.Position {
	select {
	case <-r.entered:
	default:
		close(r.entered)
	}
	return position.MustLSN("0/100")
}

// A worker lost after its WindowOpen but before the window's Closes marker
// is sent takes the window's rows with it — and, the marker not being sent
// yet, the chunk is not among the lost windows the loss records. The chunk
// went on as done, its marker reached the restarted worker, which held no
// such window, and its rows were never written (chaos-1M-3daffab: chunk 63
// of pr_events, chunks 19 and 20 of pr_items, 18,420 rows). The chunk
// reports the loss, so its partition redoes it.
func TestAWorkerLostBetweenChunkReadyAndClosesRedoesTheChunk(t *testing.T) {
	c, w := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": {
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}}
	c.sessionErrs = make(chan error, 4)
	w.out = make(chan *pb.CoordinatorMessage, 8)
	w.queue = make(chan queuedBatch, 64)
	c.chunkReady = make(chan *pb.ChunkReady, 8)
	c.windowOpen = make(chan *pb.WindowOpen, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer c.releaseAllGates()

	rdr := slowCatchUpReader{entered: make(chan struct{})}
	go func() {
		m := <-w.out
		req := m.GetChunk()
		// The window's position is past the reader's synced position, so the
		// caught-up proof blocks.
		c.windowOpen <- &pb.WindowOpen{Table: req.Table, ChunkId: req.ChunkId, Attempt: w.epoch, Seq: 7, Pos: "0/200"}
		c.chunkReady <- &pb.ChunkReady{Table: req.Table, ChunkId: req.ChunkId, Epoch: w.epoch, WindowIds: []uint64{7}}
	}()
	lost := c.lostSignal(w)
	chunks := snapshot.Chunks([][]any{{int64(10)}})
	done := make(chan error, 1)
	go func() {
		done <- c.snapshotChunk(ctx, rdr, c.refs[0], 0, w, snapshot.SnapshotConfig{}, chunks[0], 7, 0, w.epoch, lost, nil, map[uint64]int{})
	}()

	// The window is open; the worker is lost while the reader catches up.
	select {
	case <-rdr.entered:
	case <-ctx.Done():
		t.Fatal("the window never became ready")
	}
	c.signalSessionEnd(w.name, context.Canceled)

	if err := <-done; !errors.Is(err, errWorkerLost) {
		t.Fatalf("snapshotChunk = %v, want errWorkerLost: the window died with the worker", err)
	}
}
