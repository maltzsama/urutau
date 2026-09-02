package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/position"
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
	p := newPositionIndex("w", "run-0")
	lo, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-10")
	hi, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-20")
	hi2, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-30")

	p.add(inflightBatch{table: "raw.orders", high: lo, bytes: 10})
	p.add(inflightBatch{table: "raw.items", high: hi, bytes: 20})
	p.add(inflightBatch{table: "raw.orders", high: hi2, bytes: 30})

	// Acking orders past 1-10 pops only the first batch: the unconfirmed
	// raw.items batch blocks the rest.
	if freed := p.truncate("raw.orders", lo); freed != 10 {
		t.Fatalf("first truncate freed %d, want 10", freed)
	}
	// Acking items past 1-20 pops items AND the orders batch behind it? No —
	// head order: items is at the head after the first pop; acking it frees
	// items, then the head becomes orders@1-30 which the orders ack (1-10)
	// does not cover.
	if freed := p.truncate("raw.items", hi); freed != 20 {
		t.Fatalf("second truncate freed %d, want 20", freed)
	}
	if freed := p.truncate("raw.orders", lo); freed != 0 {
		t.Fatalf("stale ack freed %d, want 0", freed)
	}
	if freed := p.truncate("raw.orders", hi2); freed != 30 {
		t.Fatalf("final truncate freed %d, want 30", freed)
	}
}

func TestPositionIndexPositionlessPopsOnAnyAck(t *testing.T) {
	p := newPositionIndex("w", "run-1")
	pos, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-5")

	p.add(inflightBatch{table: "raw.orders", high: nil, bytes: 7}) // snapshot rows
	p.add(inflightBatch{table: "raw.orders", high: pos, bytes: 3}) // closes marker

	if freed := p.truncate("raw.orders", pos); freed != 10 {
		t.Fatalf("freed %d, want 10 (positionless pops once its table acked)", freed)
	}
}

func TestPositionIndexManifest(t *testing.T) {
	p := newPositionIndex("w", "run-abc")
	pos, _ := position.ParseGTID("00000000-0000-0000-0000-000000000001:1-34")
	p.add(inflightBatch{id: 7, table: "raw.orders", high: pos, bytes: 10})
	p.add(inflightBatch{id: 8, table: "raw.items", high: nil, bytes: 5})
	p.truncate("raw.orders", pos)

	m := p.Manifest()
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
