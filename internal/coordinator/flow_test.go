package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/position"
)

func TestFlowBudgetBlocksOverCeiling(t *testing.T) {
	b := newFlowBudget(100, 10)
	ctx := context.Background()

	if err := b.acquire(ctx, "a", 60); err != nil {
		t.Fatalf("acquire a1: %v", err)
	}
	if err := b.acquire(ctx, "b", 40); err != nil {
		t.Fatalf("acquire b1: %v", err)
	}

	// Budget is full: acquiring beyond the floor blocks...
	done := make(chan error, 1)
	go func() { done <- b.acquire(ctx, "a", 20) }()
	select {
	case err := <-done:
		t.Fatalf("acquire beyond ceiling returned %v, want block", err)
	case <-time.After(50 * time.Millisecond):
	}

	// ...but the per-worker floor always lets a starving worker through.
	if err := b.acquire(ctx, "c", 10); err != nil {
		t.Fatalf("acquire at floor: %v", err)
	}

	// Releasing unblocks the waiter.
	b.release("b", 40)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("acquire still blocked after release")
	}
}

func TestFlowBudgetCancelUnblocks(t *testing.T) {
	b := newFlowBudget(10, 0)
	ctx, cancel := context.WithCancel(context.Background())
	if err := b.acquire(ctx, "a", 10); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	errc := make(chan error, 1)
	go func() { errc <- b.acquire(ctx, "a", 10) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("cancelled acquire returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled acquire still blocked")
	}
}

func TestPositionIndexTruncatesByHead(t *testing.T) {
	p := newPositionIndex("run-0")
	lo, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-10")
	hi, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-20")
	hi2, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-30")

	p.add(inflightBatch{table: "raw.orders", high: lo, bytes: 10})
	p.add(inflightBatch{table: "raw.items", high: hi, bytes: 20})
	p.add(inflightBatch{table: "raw.orders", high: hi2, bytes: 30})

	// Acking orders past 1-10 pops only the first batch: the unconfirmed
	// raw.items batch blocks the rest.
	if freed, _ := p.truncate("raw.orders", lo); freed != 10 {
		t.Fatalf("first truncate freed %d, want 10", freed)
	}
	// Acking items past 1-20 pops items AND the orders batch behind it? No —
	// head order: items is at the head after the first pop; acking it frees
	// items, then the head becomes orders@1-30 which the orders ack (1-10)
	// does not cover.
	if freed, _ := p.truncate("raw.items", hi); freed != 20 {
		t.Fatalf("second truncate freed %d, want 20", freed)
	}
	if freed, _ := p.truncate("raw.orders", lo); freed != 0 {
		t.Fatalf("stale ack freed %d, want 0", freed)
	}
	if freed, _ := p.truncate("raw.orders", hi2); freed != 30 {
		t.Fatalf("final truncate freed %d, want 30", freed)
	}
}

func TestPositionIndexPositionlessPopsOnAnyAck(t *testing.T) {
	p := newPositionIndex("run-1")
	pos, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-5")

	p.add(inflightBatch{table: "raw.orders", high: nil, bytes: 7}) // snapshot rows
	p.add(inflightBatch{table: "raw.orders", high: pos, bytes: 3}) // closes marker

	if freed, _ := p.truncate("raw.orders", pos); freed != 10 {
		t.Fatalf("freed %d, want 10 (positionless pops once its table acked)", freed)
	}
}

func TestPositionIndexManifest(t *testing.T) {
	p := newPositionIndex("run-abc")
	pos, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-34")
	p.add(inflightBatch{id: 7, table: "raw.orders", high: pos, bytes: 10})
	p.add(inflightBatch{id: 8, table: "raw.items", high: nil, bytes: 5})
	p.truncate("raw.orders", pos)

	m, _ := p.Manifest()
	if m.RunID != "run-abc" {
		t.Fatalf("run_id = %q, want run-abc", m.RunID)
	}
	if m.Acked["raw.orders"] != pos.String() {
		t.Fatalf("acked = %v, want %s", m.Acked, pos.String())
	}
	// Batch 7 popped on ack; batch 8 (unconfirmed items) still in flight.
	if m.FirstBatchID != 8 || m.LastBatchID != 8 {
		t.Fatalf("batch ids = %d..%d, want 8..8", m.FirstBatchID, m.LastBatchID)
	}
}

// #209: a batch larger than the whole budget must not deadlock — it can never
// fit the ceiling, so waiting for room (with nothing in flight to free it)
// would hang forever.
func TestFlowBudgetOversizedFirstBatchDoesNotDeadlock(t *testing.T) {
	b := newFlowBudget(100, 10) // total 100, floor 10; n=200 exceeds both
	done := make(chan error, 1)
	go func() { done <- b.acquire(context.Background(), "a", 200) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("oversized first acquire: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("an oversized first batch deadlocked the worker")
	}
}

// #209 (review): the oversized-first-batch allowance must admit at most one
// oversized batch at a time — once one is charged, sum>0 and a second blocks,
// so total in-flight memory cannot grow by one oversized batch per worker.
func TestFlowBudgetSecondOversizedBatchBlocks(t *testing.T) {
	b := newFlowBudget(100, 10)
	if err := b.acquire(context.Background(), "a", 200); err != nil {
		t.Fatalf("first oversized acquire: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- b.acquire(context.Background(), "b", 200) }()
	select {
	case <-done:
		t.Fatal("a second oversized batch must block while the first is in flight")
	case <-time.After(50 * time.Millisecond):
	}
	b.release("a", 200)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the second oversized batch never unblocked")
	}
}

// #209 (review): the single oversized slot must not require global quiescence.
// An oversized batch proceeds even while another worker holds a small charge,
// so it cannot be starved by continuous normal traffic.
func TestFlowBudgetOversizedProceedsUnderLoad(t *testing.T) {
	b := newFlowBudget(100, 10)
	if err := b.acquire(context.Background(), "a", 50); err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- b.acquire(context.Background(), "b", 200) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("oversized acquire under load: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("an oversized batch starved while another worker held a charge")
	}
}

// #209 (review): the oversized slot must be released by the ack of the
// oversized batch itself, not by the owner draining — otherwise a worker that
// keeps normal batches in flight holds the slot and blocks every later
// oversized acquisition.
func TestOversizedSlotReleasedOnAckNotOnDrain(t *testing.T) {
	b := newFlowBudget(100, 10)
	idx := newPositionIndex("run")
	// A normal batch first (fits the floor), then the oversized one.
	if err := b.acquire(context.Background(), "a", 10); err != nil {
		t.Fatalf("acquire a normal: %v", err)
	}
	if err := b.acquire(context.Background(), "a", 200); err != nil {
		t.Fatalf("acquire a oversized: %v", err)
	}
	idx.add(inflightBatch{id: 1, table: "t", high: positionFixture("p1"), bytes: 200, oversized: true})
	idx.add(inflightBatch{id: 2, table: "t", high: positionFixture("p2"), bytes: 10})

	// The ack covers only the oversized batch (p1), not p2.
	freed, freedOversized := idx.truncate("t", positionFixture("p1"))
	if freed != 200 || !freedOversized {
		t.Fatalf("truncate = %d, %v; want 200, true", freed, freedOversized)
	}
	b.release("a", freed)
	b.clearOversized("a")

	// Worker b's oversized batch must be admitted even though a still holds
	// its normal batch.
	done := make(chan error, 1)
	go func() { done <- b.acquire(context.Background(), "b", 200) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquire b oversized: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("the oversized slot was not released by the oversized batch's ack")
	}
}

// #215: MarkClean must not clear a dirty flag set by a mutation that raced the
// Manifest — otherwise that change is skipped until the next mutation.
func TestMarkCleanOnlyClearsItsGeneration(t *testing.T) {
	p := newPositionIndex("run")
	p.add(inflightBatch{id: 1, table: "t", bytes: 1})
	_, gen := p.Manifest()

	// A mutation after the manifest bumps the generation.
	p.add(inflightBatch{id: 2, table: "t", bytes: 1})
	p.MarkClean(gen) // stale gen: must NOT clear
	if !p.Dirty() {
		t.Fatal("MarkClean with a stale generation must not clear dirty")
	}

	_, gen2 := p.Manifest()
	p.MarkClean(gen2)
	if p.Dirty() {
		t.Fatal("MarkClean with the current generation must clear dirty")
	}
}
