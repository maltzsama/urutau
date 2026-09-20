package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// #206: a worker that attaches but never acks a snapshot chunk must not wedge
// the run forever — the snapshot watchdog fails the chunk within
// SnapshotChunkTimeout instead.
func TestSnapshotPartitionWatchdog(t *testing.T) {
	c, w := coordHarness()
	// The ChunkRequest send must succeed (buffered) so the block is in
	// waitChunkReady, where the watchdog fires.
	w.out = make(chan *pb.CoordinatorMessage, 1)
	c.chunkReady = make(chan *pb.ChunkReady, 1) // never fed
	c.cfg.SnapshotChunkTimeout = 50 * time.Millisecond

	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders"}
	chunker := fakeChunkSource{bounds: [][]any{{int64(50)}}}

	start := time.Now()
	err := c.snapshotPartition(context.Background(), fakeSourceReader{}, chunker, ref, source.Chunk{}, 0, w, snapshot.SnapshotConfig{})
	if err == nil {
		t.Fatal("a wedged worker must fail the snapshot, not hang")
	}
	if !strings.Contains(err.Error(), "wedged") {
		t.Fatalf("snapshotPartition = %v, want the watchdog error", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the watchdog took %v; the deadline was not applied", elapsed)
	}
}

// A parent-context cancellation is not a watchdog expiry: the error must stay
// the context error, not the "worker may be wedged" wrap.
func TestSnapshotPartitionWatchdogParentCancel(t *testing.T) {
	c, w := coordHarness()
	w.out = make(chan *pb.CoordinatorMessage, 1)
	c.chunkReady = make(chan *pb.ChunkReady, 1)
	c.cfg.SnapshotChunkTimeout = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ref := source.TableRef{Source: "shop.orders", Target: "raw.orders"}
	chunker := fakeChunkSource{bounds: [][]any{{int64(50)}}}
	err := c.snapshotPartition(ctx, fakeSourceReader{}, chunker, ref, source.Chunk{}, 0, w, snapshot.SnapshotConfig{})
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("snapshotPartition = %v, want context canceled", err)
	}
}
