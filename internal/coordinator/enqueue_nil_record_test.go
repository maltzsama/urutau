package coordinator

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/dataplane"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// A batch that reaches the coordinator without a record is a caller's bug. It
// must come back as an error the pipeline can report, not as a nil
// dereference that takes the coordinator down.
func TestEnqueueRejectsABatchWithoutARecord(t *testing.T) {
	t.Run("single owner", func(t *testing.T) {
		c, _ := coordHarness()
		err := c.enqueueBatch(context.Background(), &dataplane.Batch{Table: "raw.orders"}, &pb.BatchMeta{Table: "raw.orders"})
		if err == nil {
			t.Fatal("enqueueBatch accepted a batch without a record")
		}
	})
	t.Run("partitioned table", func(t *testing.T) {
		c := stagedReplayHarness(t)
		err := c.enqueueBatch(context.Background(), &dataplane.Batch{Table: "raw.orders"}, &pb.BatchMeta{Table: "raw.orders"})
		if err == nil {
			t.Fatal("enqueueBatch accepted a batch without a record")
		}
	})
}

func TestJobStartedAttrsWithoutASpec(t *testing.T) {
	c := &Coordinator{}
	attrs := c.jobStartedAttrs()
	if attrs["pipeline"] != "" || attrs["source"] != "" {
		t.Fatalf("attrs = %v, want empty names for a coordinator without a spec", attrs)
	}
}
