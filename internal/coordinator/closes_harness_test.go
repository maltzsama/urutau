package coordinator

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// aheadReader has decoded further than the pump has sent: its events past
// what the coordinator sent are still on their way through the pump.
type aheadReader struct{ source.SourceReader }

func (aheadReader) Master(context.Context) (position.Position, error) {
	return position.MustLSN("0/1"), nil
}
func (aheadReader) Synced() position.Position { return position.MustLSN("0/100") }

// closesMarkerPos runs one chunk's window against a worker that answers at
// once, and returns the position its Closes marker carried.
func closesMarkerPos(t *testing.T, lastSent string) string {
	t.Helper()
	c, w := coordHarness()
	c.refs = []source.TableRef{{Source: "shop.orders", Target: "raw.orders", PrimaryKey: []string{"id"}}}
	c.canonical = map[string]core.Schema{"shop.orders": {
		Columns:    []core.Column{{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}}},
		PrimaryKey: []string{"id"},
	}}
	if lastSent != "" {
		c.noteSent("raw.orders", lastSent)
	}
	w.out = make(chan *pb.CoordinatorMessage, 8)
	w.queue = make(chan queuedBatch, 64)
	c.chunkReady = make(chan *pb.ChunkReady, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	defer c.releaseAllGates()
	go func() {
		for {
			select {
			case m := <-w.out:
				if req := m.GetChunk(); req != nil {
					c.chunkReady <- &pb.ChunkReady{Table: req.Table, ChunkId: req.ChunkId, Epoch: w.epoch}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	chunks := snapshot.Chunks([][]any{{int64(10)}})
	go func() {
		_ = c.snapshotPartition(ctx, aheadReader{}, chunks[:1], c.refs[0], source.Chunk{}, 0, w, snapshot.SnapshotConfig{})
	}()
	for {
		select {
		case q := <-w.queue:
			var meta pb.BatchMeta
			if err := proto.Unmarshal(q.meta, &meta); err != nil {
				t.Fatal(err)
			}
			if meta.GetWindow().GetCloses() {
				return meta.LowPos
			}
		case <-ctx.Done():
			t.Fatal("no Closes marker was queued")
		}
	}
}
