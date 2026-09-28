package sourcepull

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
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
