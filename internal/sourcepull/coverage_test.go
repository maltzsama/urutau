package sourcepull

// Coverage for the Puller's buffering/error surfaces: the ctx/error/closed
// arms of Next, the non-blocking tryNext, and Drain's emit/error handling.

import (
	"context"
	"errors"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

func sampleChange(table string, id int64) rowchange.Change {
	return rowchange.Change{
		Op:       rowchange.OpInsert,
		Table:    table,
		Key:      []any{id},
		After:    map[string]any{"id": id, "v": "x"},
		Position: "p",
	}
}

func schemaFor(tables ...string) map[string]core.Schema {
	m := make(map[string]core.Schema, len(tables))
	for _, tb := range tables {
		m[tb] = core.Schema{
			Columns: []core.Column{
				{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
				{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			},
			PrimaryKey: []string{"id"},
		}
	}
	return m
}

func TestNextContextCancel(t *testing.T) {
	p := New(make(chan rowchange.Change))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(cancelled) = %v, want context.Canceled", err)
	}
}

func TestNextCleanEndViaClosedChannel(t *testing.T) {
	ch := make(chan rowchange.Change)
	close(ch)
	p := New(ch)
	b, err := p.Next(context.Background())
	if b != nil || err != nil {
		t.Fatalf("Next(closed) = %v, %v; want nil, nil", b, err)
	}
}

func TestNextTerminalError(t *testing.T) {
	boom := errors.New("decode failed")
	errCh := make(chan error, 1)
	errCh <- boom
	p := New(make(chan rowchange.Change))
	p.SetErr(errCh)
	if _, err := p.Next(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Next(errCh) = %v, want boom", err)
	}

	// A nil terminal error is a clean end.
	clean := make(chan error, 1)
	clean <- nil
	p2 := New(make(chan rowchange.Change))
	p2.SetErr(clean)
	if b, err := p2.Next(context.Background()); b != nil || err != nil {
		t.Fatalf("Next(nil err) = %v, %v; want nil, nil", b, err)
	}
}

func TestNextDrainsAvailableChanges(t *testing.T) {
	ch := make(chan rowchange.Change, 4)
	ch <- sampleChange("t", 1)
	ch <- sampleChange("t", 2)
	close(ch)
	p := New(ch)
	p.SetSchemas(schemaFor("t"))

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	defer b.Release()
	if b.Record.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", b.Record.NumRows())
	}
}

func TestNextStopsAtBatchTarget(t *testing.T) {
	ch := make(chan rowchange.Change, batchTarget+10)
	for i := range batchTarget + 5 {
		ch <- sampleChange("t", int64(i))
	}
	p := New(ch)
	p.SetSchemas(schemaFor("t"))

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	defer b.Release()
	if b.Record.NumRows() != batchTarget {
		t.Fatalf("rows = %d, want the batch target %d", b.Record.NumRows(), batchTarget)
	}
}

func TestTryNext(t *testing.T) {
	ctx := context.Background()

	// Buffered changes are flushed without touching the channel.
	p := New(make(chan rowchange.Change))
	p.SetSchemas(schemaFor("t"))
	p.buf = []rowchange.Change{sampleChange("t", 1)}
	b, ok, err := p.tryNext(ctx)
	if err != nil || !ok || b == nil {
		t.Fatalf("tryNext(buffered) = %v, %v, %v", b, ok, err)
	}
	b.Release()

	// A pending channel change is consumed.
	ch := make(chan rowchange.Change, 1)
	ch <- sampleChange("t", 2)
	p2 := New(ch)
	p2.SetSchemas(schemaFor("t"))
	if b, ok, err := p2.tryNext(ctx); err != nil || !ok || b == nil {
		t.Fatalf("tryNext(channel) = %v, %v, %v", b, ok, err)
	} else {
		b.Release()
	}

	// Nothing pending: ok=false.
	p3 := New(make(chan rowchange.Change))
	if b, ok, err := p3.tryNext(ctx); err != nil || ok || b != nil {
		t.Fatalf("tryNext(empty) = %v, %v, %v", b, ok, err)
	}

	// Closed channel: ok=false, no error.
	closed := make(chan rowchange.Change)
	close(closed)
	p4 := New(closed)
	if b, ok, err := p4.tryNext(ctx); err != nil || ok || b != nil {
		t.Fatalf("tryNext(closed) = %v, %v, %v", b, ok, err)
	}

	// Terminal error surfaces.
	boom := errors.New("boom")
	errCh := make(chan error, 1)
	errCh <- boom
	p5 := New(make(chan rowchange.Change))
	p5.SetErr(errCh)
	if _, ok, err := p5.tryNext(ctx); ok || !errors.Is(err, boom) {
		t.Fatalf("tryNext(err) = ok %v, err %v", ok, err)
	}

	// Nil terminal error: clean, nothing pending.
	clean := make(chan error, 1)
	clean <- nil
	p6 := New(make(chan rowchange.Change))
	p6.SetErr(clean)
	if b, ok, err := p6.tryNext(ctx); err != nil || ok || b != nil {
		t.Fatalf("tryNext(nil err) = %v, %v, %v", b, ok, err)
	}
}

func TestDrainEmitsEveryBatch(t *testing.T) {
	ch := make(chan rowchange.Change, 3)
	ch <- sampleChange("t", 1)
	ch <- sampleChange("t", 2)
	close(ch)
	p := New(ch)
	p.SetSchemas(schemaFor("t"))

	got := 0
	if err := p.Drain(context.Background(), func(b *dataplane.Batch) error {
		got++
		b.Release()
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got != 2 {
		t.Fatalf("emitted = %d, want 2", got)
	}
}

func TestDrainPropagatesEmitError(t *testing.T) {
	ch := make(chan rowchange.Change, 1)
	ch <- sampleChange("t", 1)
	close(ch)
	p := New(ch)
	p.SetSchemas(schemaFor("t"))

	boom := errors.New("emit failed")
	if err := p.Drain(context.Background(), func(b *dataplane.Batch) error {
		b.Release()
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("Drain(emit err) = %v, want boom", err)
	}
}

func TestDrainPropagatesTerminalError(t *testing.T) {
	boom := errors.New("decode failed")
	errCh := make(chan error, 1)
	errCh <- boom
	p := New(make(chan rowchange.Change))
	p.SetErr(errCh)
	if err := p.Drain(context.Background(), func(*dataplane.Batch) error { return nil }); !errors.Is(err, boom) {
		t.Fatalf("Drain(errCh) = %v, want boom", err)
	}
}

func TestDrainNothingPending(t *testing.T) {
	p := New(make(chan rowchange.Change))
	if err := p.Drain(context.Background(), func(*dataplane.Batch) error { return nil }); err != nil {
		t.Fatalf("Drain(empty) = %v", err)
	}
}
