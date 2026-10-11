package coordinator

import (
	"context"
	"testing"

	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// A worker's sent list exists to redeliver what was sent and not acked. A
// snapshot window's rows are never acked one by one and are never redelivered:
// a lost window is re-read from the source. Kept on the list, nothing ever
// removes them, and the coordinator holds the serialized bytes of the whole
// snapshot for the rest of the run.
func TestSnapshotRowBatchesAreNotKeptForRedelivery(t *testing.T) {
	c, w := coordHarness()
	ctx := context.Background()
	if err := c.deliverBatch(ctx, w, &pb.BatchMeta{Table: "raw.orders", BatchId: 1}, []byte("rows"), nil, 4, true, ""); err != nil {
		t.Fatalf("deliverBatch (snapshot rows): %v", err)
	}
	if err := c.budget.Acquire(ctx, w.name, 4); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := c.deliverBatch(ctx, w, &pb.BatchMeta{Table: "raw.orders", BatchId: 2}, []byte("live"), nil, 4, false, "0/30"); err != nil {
		t.Fatalf("deliverBatch (live): %v", err)
	}

	for range 2 {
		w.recordSent(<-w.queue) // what DoGet does with each batch it pops
	}

	w.sentMu.Lock()
	defer w.sentMu.Unlock()
	if len(w.sent) != 1 || w.sent[0].id != 2 {
		ids := make([]uint64, len(w.sent))
		for i, qb := range w.sent {
			ids[i] = qb.id
		}
		t.Fatalf("sent list holds batches %v, want only the live batch 2", ids)
	}
}
