package coordinator

import (
	"context"
	"reflect"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/snapshot"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
)

// committedCyclesSink records the first descriptor of every cycle committed.
type committedCyclesSink struct {
	fakeStagedSink
	cycles []string
}

func (s *committedCyclesSink) CommitStaged(_ context.Context, _ core.TableRef, staged [][]byte, _ string) error {
	s.cycles = append(s.cycles, string(staged[0]))
	return nil
}

// fanoutHarness is a two-owner staged table in a fan-out snapshot pass.
func fanoutHarness(t *testing.T) (*Coordinator, *committedCyclesSink, source.TableRef, []*workerState) {
	t.Helper()
	c := stagedReplayHarness(t)
	snk := &committedCyclesSink{}
	c.snk = snk
	c.bootCommitted = nil
	owners, _ := c.loadRouting().OwnersOf("raw.orders")
	c.fanout.begin("raw.orders")
	return c, snk, c.refs[0], owners
}

// closeChunk queues a chunk's Closes marker and returns its cycle.
func closeChunk(t *testing.T, c *Coordinator, ref source.TableRef, owners []*workerState, chunk uint32, pending []uint32) uint64 {
	t.Helper()
	if err := c.closeFanoutChunk(context.Background(), ref, owners, position.MustLSN("0/30"), uint64(chunk), pending); err != nil {
		t.Fatalf("closeFanoutChunk(%d): %v", chunk, err)
	}
	for _, w := range owners {
		<-w.queue
	}
	return c.batchSeq.Load()
}

// held is an owner's delivery of a window it held, lost of one it never did.
func held(seq uint64, desc string, pending []uint32) *pb.StagedBatch {
	return &pb.StagedBatch{Table: "raw.orders", Seq: seq, Descriptor_: []byte(desc),
		SnapshotState: string(snapshot.StateInProgress), SnapshotPending: pending}
}

func lost(seq uint64) *pb.StagedBatch {
	return &pb.StagedBatch{Table: "raw.orders", Seq: seq, Descriptor_: []byte("empty"), WindowLost: true}
}

// A worker restarted mid-snapshot gets the Closes markers of windows whose
// rows went to the process that died. The other owner held its share of each
// chunk and delivers the cycle with the chunk off the pending list: the cycle
// used to commit that, recording the chunk done with the restarted owner's
// rows missing, and so did every chunk queued behind it (#829, 191082 rows of
// pr_accounts in the 1M chaos run). Neither commits, and the pass reports the
// first lost chunk to read again.
func TestFanoutWindowAnOwnerLostIsNotCommitted(t *testing.T) {
	c, snk, ref, owners := fanoutHarness(t)
	seq3 := closeChunk(t, c, ref, owners, 3, []uint32{4, 5})
	seq4 := closeChunk(t, c, ref, owners, 4, []uint32{5})

	c.onStagedBatch("w1", held(seq3, "w1-chunk3", []uint32{4, 5}))
	c.onStagedBatch("w0", lost(seq3))
	if done, _, _ := c.fanout.drained("raw.orders"); done {
		t.Fatal("the pass reads as drained with chunk 4's window still to deliver")
	}
	// Chunk 4 was queued before the loss was known: both owners held it, and
	// its progress no longer names chunk 3.
	c.onStagedBatch("w1", held(seq4, "w1-chunk4", []uint32{5}))
	c.onStagedBatch("w0", held(seq4, "w0-chunk4", []uint32{5}))

	if len(snk.cycles) != 0 {
		t.Fatalf("committed %v: chunk 3 is recorded done without w0's rows", snk.cycles)
	}
	done, from, wasLost := c.fanout.drained("raw.orders")
	if !done || !wasLost || from != 3 {
		t.Fatalf("drained = (%v, from %d, lost %v), want the pass drained with chunk 3 to read again", done, from, wasLost)
	}

	// The next pass reads chunk 3 again, and this time both owners hold it.
	redo := closeChunk(t, c, ref, owners, 3, []uint32{4, 5})
	c.onStagedBatch("w0", held(redo, "w0-chunk3", []uint32{4, 5}))
	c.onStagedBatch("w1", held(redo, "w1-chunk3", []uint32{4, 5}))
	if len(snk.cycles) != 1 {
		t.Fatalf("committed %v, want the redone chunk 3", snk.cycles)
	}
	if done, _, wasLost := c.fanout.drained("raw.orders"); !done || wasLost {
		t.Fatalf("after the redo: done=%v lost=%v, want a clean pass", done, wasLost)
	}
}

// An owner that already delivered its share of a window and then restarted
// reports the redelivered marker's window lost. Its rows are in the cycle:
// nothing was lost.
func TestFanoutWindowDeliveredBeforeTheRestartCommits(t *testing.T) {
	c, snk, ref, owners := fanoutHarness(t)
	seq := closeChunk(t, c, ref, owners, 3, []uint32{4})
	c.onStagedBatch("w0", held(seq, "w0-chunk3", []uint32{4}))
	c.onStagedBatch("w0", lost(seq))
	c.onStagedBatch("w1", held(seq, "w1-chunk3", []uint32{4}))
	if len(snk.cycles) != 1 {
		t.Fatalf("committed %v, want chunk 3's cycle", snk.cycles)
	}
	if _, _, wasLost := c.fanout.drained("raw.orders"); wasLost {
		t.Fatal("a window whose owner had delivered its rows was taken as lost")
	}
}

// Every owner gets a window for every chunk, rows or not: an owner sent
// nothing reports its Closes marker's window as one it never held, which is
// what a lost window looks like, and the chunk would be read again forever.
func TestFanoutOpensTheWindowOnAnOwnerWithNoRows(t *testing.T) {
	c, _, ref, owners := fanoutHarness(t)
	names := []string{"w0", "w1"}
	r := &distributedRelay{c: c, ctx: context.Background(), ref: ref, owners: owners}
	if err := r.fanOut("raw.orders", 7, replayBatch(t, []int64{ownerKey(t, names, 1)}, []string{"0/30"})); err != nil {
		t.Fatalf("fanOut: %v", err)
	}
	for _, w := range owners {
		select {
		case q := <-w.queue:
			if !q.snapshotRows {
				t.Fatalf("%s was sent something other than the window's rows", w.name)
			}
		default:
			t.Fatalf("%s got no window for the chunk: it has no row in it", w.name)
		}
	}
}

func TestPendingFrom(t *testing.T) {
	if got := pendingFrom([]uint32{2, 3, 5, 8}, 5); !reflect.DeepEqual(got, []uint32{5, 8}) {
		t.Fatalf("pendingFrom = %v, want [5 8]", got)
	}
	if got := pendingFrom([]uint32{2, 3}, 9); got != nil {
		t.Fatalf("pendingFrom of a chunk outside the pass = %v, want nil", got)
	}
}
