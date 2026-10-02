package sourcepull

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

func txnSchemas() map[string]core.Schema {
	s := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		},
		PrimaryKey: []string{"id"},
	}
	return map[string]core.Schema{"a": s, "b": s}
}

func txnRows(table, pos string, from, n int) []rowchange.Change {
	out := make([]rowchange.Change, n)
	for i := range out {
		id := int64(from + i)
		out[i] = rowchange.Change{Op: rowchange.OpInsert, Table: table, Key: []any{id}, After: map[string]any{"id": id}, Position: pos}
	}
	return out
}

// Issue #456: every row of a transaction carries the transaction's position,
// so a batch that ends inside a transaction lets a commit record that
// position without the rest of the transaction; a resume from it skips the
// rest. With transaction bounds, a batch holds only whole transactions, a
// large one included.
func TestBatchHoldsWholeTransactions(t *testing.T) {
	ch := make(chan rowchange.Change, 1000)
	p := New(ch)
	p.SetSchemas(txnSchemas())
	p.BoundTransactions()
	for _, c := range txnRows("a", "g:1-5", 0, 250) {
		ch <- c
	}
	ch <- rowchange.Change{Op: rowchange.OpTxnEnd}

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	defer b.Release()
	if n := b.Record.NumRows(); n != 250 {
		t.Fatalf("batch rows = %d, want the whole 250-row transaction", n)
	}
}

// drained collects the rows of every batch Drain emits.
func drained(t *testing.T, p *Puller) []int64 {
	t.Helper()
	var rows []int64
	if err := p.Drain(context.Background(), func(b *dataplane.Batch) error {
		rows = append(rows, b.Record.NumRows())
		b.Release()
		return nil
	}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	return rows
}

// Rows of a transaction still open are never batched; its end releases them
// all in one batch.
func TestOpenTransactionIsNotBatched(t *testing.T) {
	ch := make(chan rowchange.Change, 1000)
	p := New(ch)
	p.SetSchemas(txnSchemas())
	p.BoundTransactions()
	for _, c := range txnRows("a", "g:1-5", 0, 150) {
		ch <- c
	}
	if rows := drained(t, p); len(rows) != 0 {
		t.Fatalf("batches of %v rows emitted while the transaction was open", rows)
	}
	ch <- rowchange.Change{Op: rowchange.OpTxnEnd}
	if rows := drained(t, p); len(rows) != 1 || rows[0] != 150 {
		t.Fatalf("after the transaction ended: batches of %v rows, want one of 150", rows)
	}
}

// A stream that ends inside a transaction batches nothing of it: the
// transaction was never whole, and a resume reads it again.
func TestStreamEndingInsideATransactionBatchesNothing(t *testing.T) {
	ch := make(chan rowchange.Change, 1000)
	p := New(ch)
	p.SetSchemas(txnSchemas())
	p.BoundTransactions()
	for _, c := range txnRows("a", "g:1-5", 0, 150) {
		ch <- c
	}
	close(ch)
	b, err := p.Next(context.Background())
	if err != nil || b != nil {
		t.Fatalf("Next = %v, %v; want no batch at the stream's end", b, err)
	}
}

// A transaction over two tables is split by table at its end, each table's
// rows whole and in order; the next transaction starts a new batch only
// when its table differs.
func TestTransactionOverTwoTablesSplitsByTable(t *testing.T) {
	ch := make(chan rowchange.Change, 1000)
	p := New(ch)
	p.SetSchemas(txnSchemas())
	p.BoundTransactions()
	for _, c := range append(append(txnRows("a", "g:1-5", 0, 3), txnRows("b", "g:1-5", 10, 2)...), txnRows("a", "g:1-5", 3, 2)...) {
		ch <- c
	}
	ch <- rowchange.Change{Op: rowchange.OpTxnEnd}
	close(ch)

	want := []struct {
		table string
		rows  int64
	}{{"a", 5}, {"b", 2}}
	for i, w := range want {
		b, err := p.Next(context.Background())
		if err != nil || b == nil {
			t.Fatalf("batch %d: %v %v", i, b, err)
		}
		if b.Table != w.table || b.Record.NumRows() != w.rows {
			t.Fatalf("batch %d = %s/%d rows, want %s/%d", i, b.Table, b.Record.NumRows(), w.table, w.rows)
		}
		b.Release()
	}
}

// A transaction larger than one batch is split, and only its LAST piece
// carries the transaction's position. Every earlier piece carries the resume
// (safe) position, so acking it cannot free rows a later piece still owes —
// the whole transaction replays from its start on a crash.
func TestHugeTransactionPositionsOnlyTheLastPiece(t *testing.T) {
	const total = maxBatchRows*2 + 500
	ch := make(chan rowchange.Change, total+1)
	p := New(ch)
	p.SetSchemas(txnSchemas())
	p.BoundTransactions()
	p.SetResume("g:0")
	for _, c := range txnRows("a", "g:1-5000", 0, total) {
		ch <- c
	}
	ch <- rowchange.Change{Op: rowchange.OpTxnEnd}
	close(ch)

	var positions []string
	var rows int
	for {
		b, err := p.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		r, rerr := transport.NewBatchReader(b.Record, nil)
		if rerr != nil {
			b.Release()
			t.Fatalf("reader: %v", rerr)
		}
		positions = append(positions, r.Position(r.NumRows()-1))
		rows += r.NumRows()
		b.Release()
	}
	if rows != total {
		t.Fatalf("rows = %d, want %d", rows, total)
	}
	if len(positions) < 2 {
		t.Fatalf("expected the transaction to split, got %d batch(es)", len(positions))
	}
	for i, pos := range positions[:len(positions)-1] {
		if pos != "g:0" {
			t.Fatalf("batch %d position = %q, want the safe resume position g:0 (only the last piece may carry the transaction position)", i, pos)
		}
	}
	if last := positions[len(positions)-1]; last != "g:1-5000" {
		t.Fatalf("last batch position = %q, want g:1-5000", last)
	}
}
