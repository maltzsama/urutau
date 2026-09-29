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

// slowCatchUpReader holds the high-watermark read until released: the reader
// catching up to a window, which takes minutes while the stream replays.
type slowCatchUpReader struct {
	source.SourceReader
	entered, release chan struct{}
}

func (r slowCatchUpReader) Master(ctx context.Context) (position.Position, error) {
	close(r.entered) // the ChunkReady is consumed: the window is open
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return position.MustLSN("0/1"), nil
}
func (slowCatchUpReader) Synced() position.Position { return position.MustLSN("0/100") }

// A worker lost after its ChunkReady but before the chunk's Closes marker
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer c.releaseAllGates()

	rdr := slowCatchUpReader{entered: make(chan struct{}), release: make(chan struct{})}
	go func() {
		m := <-w.out
		req := m.GetChunk()
		c.chunkReady <- &pb.ChunkReady{Table: req.Table, ChunkId: req.ChunkId, Epoch: w.epoch}
	}()
	lost := c.lostSignal(w)
	chunks := snapshot.Chunks([][]any{{int64(10)}})
	done := make(chan error, 1)
	go func() {
		done <- c.snapshotChunk(ctx, rdr, c.refs[0], 0, w, snapshot.SnapshotConfig{}, chunks[0], 7, w.epoch, lost, nil)
	}()

	// The chunk is ready; the worker is lost while the reader catches up.
	select {
	case <-rdr.entered:
	case <-ctx.Done():
		t.Fatal("the chunk never became ready")
	}
	c.signalSessionEnd(w.name, context.Canceled)
	close(rdr.release)

	if err := <-done; !errors.Is(err, errWorkerLost) {
		t.Fatalf("snapshotChunk = %v, want errWorkerLost: the window died with the worker", err)
	}
}
