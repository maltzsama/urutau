package coordinator

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

// positionStagedSink records the position each staged commit carries.
type positionStagedSink struct {
	fakeStagedSink
	positions []string
}

func (s *positionStagedSink) CommitStaged(_ context.Context, _ core.TableRef, _ [][]byte, pos string) error {
	s.positions = append(s.positions, pos)
	return nil
}

// A staged cycle holds every partition's share of the source batch, so once
// it commits the table is durable through the batch's last row. Committing
// the lowest partition's position instead left the chaos smoke run's
// pr_items at 1-8694 with the rows of 1-8696 in the table, and nothing
// later to move it.
func TestStagedCycleCommitsTheBatchPosition(t *testing.T) {
	c := stagedReplayHarness(t)
	snk := &positionStagedSink{}
	c.snk = snk
	// One key per owner: w0's row at 0/30, w1's at 0/40.
	names := []string{"w0", "w1"}
	b := replayBatch(t, []int64{ownerKey(t, names, 0), ownerKey(t, names, 1)}, []string{"0/30", "0/40"})
	meta := &pb.BatchMeta{Table: "raw.orders"}
	if err := c.enqueueBatch(context.Background(), b, meta); err != nil {
		t.Fatalf("enqueueBatch: %v", err)
	}
	if committable, _ := c.staged.deliver(stagedRef("raw.orders", "w0"), meta.BatchId, []byte{1}, "0/30", "", nil); len(committable) != 0 {
		t.Fatal("the cycle completed with one of two owners")
	}
	committable, known := c.staged.deliver(stagedRef("raw.orders", "w1"), meta.BatchId, []byte{2}, "0/40", "", nil)
	if !known || len(committable) != 1 {
		t.Fatalf("committable=%d known=%v; want the cycle complete", len(committable), known)
	}
	c.runCtx = context.Background()
	if err := c.commitStagedCycle(committable[0]); err != nil {
		t.Fatalf("commitStagedCycle: %v", err)
	}
	if len(snk.positions) != 1 || snk.positions[0] != "0/40" {
		t.Fatalf("committed positions = %v, want [0/40]: the cycle holds both rows of the batch", snk.positions)
	}
}

// A cycle's committed position is the batch's last source position, which can
// belong to a key range other than the last partition's: sub-batches are
// ordered by key range while positions follow source order, so the last
// sub-batch need not hold the batch's last position (issue #492).
func TestStagedCycleCommitsTheBatchMaximumNotLastPartition(t *testing.T) {
	c := stagedReplayHarness(t)
	snk := &positionStagedSink{}
	c.snk = snk
	// The batch's last source row belongs to w0 at 0/40; w1 holds only the
	// earlier row at 0/30.
	names := []string{"w0", "w1"}
	b := replayBatch(t, []int64{ownerKey(t, names, 1), ownerKey(t, names, 0)}, []string{"0/30", "0/40"})
	meta := &pb.BatchMeta{Table: "raw.orders"}
	if err := c.enqueueBatch(context.Background(), b, meta); err != nil {
		t.Fatalf("enqueueBatch: %v", err)
	}
	if committable, _ := c.staged.deliver(stagedRef("raw.orders", "w1"), meta.BatchId, []byte{1}, "0/30", "", nil); len(committable) != 0 {
		t.Fatal("the cycle completed with one of two owners")
	}
	committable, known := c.staged.deliver(stagedRef("raw.orders", "w0"), meta.BatchId, []byte{2}, "0/40", "", nil)
	if !known || len(committable) != 1 {
		t.Fatalf("committable=%d known=%v; want the cycle complete", len(committable), known)
	}
	c.runCtx = context.Background()
	if err := c.commitStagedCycle(committable[0]); err != nil {
		t.Fatalf("commitStagedCycle: %v", err)
	}
	if len(snk.positions) != 1 || snk.positions[0] != "0/40" {
		t.Fatalf("committed positions = %v, want [0/40]: the cycle holds the batch's last position", snk.positions)
	}
}

// A snapshot window's cycle carries the reader's position at the window's
// close, past stream batches of the table still in the pump. Committed as the
// table's position, it covered stream cycles never committed, and a crash
// then skipped them on replay: chaos-1M-406f460 lost pr_items transactions
// 25474-25483 under window cycles committed at 1-25495 (#468). A window's
// cycle commits its rows and snapshot progress, never a position: only the
// stream advances the table's position.
func TestAWindowCycleCommitsNoPosition(t *testing.T) {
	c := stagedReplayHarness(t)
	snk := &positionStagedSink{}
	c.snk = snk
	c.runCtx = context.Background()
	ref := stagedRef("raw.orders", "w0")
	c.staged.expectWindow(core.TableRef{Target: "raw.orders"}, 77, []string{"w0"})
	committable, known := c.staged.deliver(ref, 77, []byte{1}, "0/90", "in_progress", []uint32{5, 6})
	if !known || len(committable) != 1 {
		t.Fatalf("committable=%d known=%v", len(committable), known)
	}
	if err := c.commitStagedCycle(committable[0]); err != nil {
		t.Fatalf("commitStagedCycle: %v", err)
	}
	if len(snk.positions) != 1 || snk.positions[0] != "" {
		t.Fatalf("window cycle committed position %v, want none", snk.positions)
	}
}
