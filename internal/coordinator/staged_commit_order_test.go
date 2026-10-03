package coordinator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/sink"
)

// orderedStagedSink records commit positions. It can block the first commit
// (until release) and/or fail it, to exercise #543.
type orderedStagedSink struct {
	sink.Sink
	mu        sync.Mutex
	committed []string
	calls     int
	started   chan struct{}
	release   chan struct{}
	failFirst bool
}

func (s *orderedStagedSink) CommitStaged(_ context.Context, _ core.TableRef, _ [][]byte, pos string) error {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n == 1 {
		if s.started != nil {
			close(s.started)
		}
		if s.release != nil {
			<-s.release
		}
		if s.failFirst {
			return fmt.Errorf("commit failed")
		}
	}
	s.mu.Lock()
	s.committed = append(s.committed, pos)
	s.mu.Unlock()
	return nil
}

func (s *orderedStagedSink) positions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.committed...)
}

// #543: cycles of one table must commit in send order even when their last
// deliveries arrive from distinct goroutines and the first commit is blocked.
// Without the per-table take-and-commit lock, a later cycle can barge past a
// sleeping earlier one.
func TestStagedCyclesCommitInSendOrderAcrossGoroutines(t *testing.T) {
	c, _ := coordHarness()
	c.runCtx = context.Background()
	snk := &orderedStagedSink{started: make(chan struct{}), release: make(chan struct{})}
	c.snk = snk
	ref := core.TableRef{Target: "raw.orders"}
	for seq := uint64(1); seq <= 3; seq++ {
		c.staged.expect(ref, seq, []string{"w0"})
	}

	var wg sync.WaitGroup
	deliver := func(seq uint64, pos string) {
		defer wg.Done()
		c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Epoch: 0, Seq: seq, Descriptor_: []byte(pos), Position: pos})
	}

	wg.Add(1)
	go deliver(1, "0/10")
	<-snk.started // c1's commit is in flight, holding the table lock

	wg.Add(2)
	go deliver(2, "0/20")
	go deliver(3, "0/30")
	// Give both a moment to block on the table lock, then release the first.
	time.Sleep(20 * time.Millisecond)
	close(snk.release)
	wg.Wait()

	if got := snk.positions(); len(got) != 3 || got[0] != "0/10" || got[1] != "0/20" || got[2] != "0/30" {
		t.Fatalf("commits = %v, want [0/10 0/20 0/30] in send order", got)
	}
}

// #543: when a cycle's commit fails, no later cycle of the table may commit —
// its durable position would advance past rows that were never written. The
// failing commit poisons the table before the run's asynchronous shutdown.
func TestStagedCycleFailurePoisonsLaterCommits(t *testing.T) {
	c, _ := coordHarness()
	c.runCtx = context.Background()
	c.terminate = make(chan error, 1)
	snk := &orderedStagedSink{failFirst: true}
	c.snk = snk
	ref := core.TableRef{Target: "raw.orders"}
	c.staged.expect(ref, 1, []string{"w0"})
	c.staged.expect(ref, 2, []string{"w0"})

	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Epoch: 0, Seq: 1, Descriptor_: []byte("1"), Position: "0/10"})
	if !c.staged.isPoisoned("raw.orders") {
		t.Fatal("a failed commit must poison the table")
	}
	select {
	case <-c.terminate:
	default:
		t.Fatal("a failed commit must fail the run")
	}

	c.onStagedBatch("w0", &pb.StagedBatch{Table: "raw.orders", Epoch: 0, Seq: 2, Descriptor_: []byte("2"), Position: "0/20"})
	if got := snk.positions(); len(got) != 0 {
		t.Fatalf("committed %v after a failed cycle, want none", got)
	}
}
