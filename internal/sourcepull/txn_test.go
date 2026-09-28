package sourcepull

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
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

// Rows of a transaction still open are never batched: Next waits for its end.
func TestOpenTransactionIsNotBatched(t *testing.T) {
	ch := make(chan rowchange.Change, 1000)
	p := New(ch)
	p.SetSchemas(txnSchemas())
	p.BoundTransactions()
	for _, c := range txnRows("a", "g:1-5", 0, 150) {
		ch <- c
	}

	type result struct {
		rows int64
		err  error
	}
	done := make(chan result, 1)
	go func() {
		b, err := p.Next(context.Background())
		if err != nil {
			done <- result{err: err}
			return
		}
		done <- result{rows: b.Record.NumRows()}
		b.Release()
	}()
	select {
	case r := <-done:
		t.Fatalf("Next returned %d rows (err %v) while the transaction was open", r.rows, r.err)
	case <-time.After(200 * time.Millisecond):
	}
	ch <- rowchange.Change{Op: rowchange.OpTxnEnd}
	r := <-done
	if r.err != nil || r.rows != 150 {
		t.Fatalf("after the transaction ended: rows=%d err=%v, want all 150", r.rows, r.err)
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
