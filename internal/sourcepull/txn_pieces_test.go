package sourcepull

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// A transaction over two tables: each table's batch holds all its rows of
// it, so each carries the transaction's position. The first one used to go
// out with the safe position because rows of the same transaction, another
// table's, followed it (#823).
func TestEachTablesBatchCarriesTheTransactionPosition(t *testing.T) {
	ch := make(chan rowchange.Change, 100)
	p := New(ch)
	p.SetSchemas(txnSchemas())
	p.BoundTransactions()
	p.SetResume("g:1-4")
	for _, c := range append(txnRows("a", "g:1-5", 0, 3), txnRows("b", "g:1-5", 10, 2)...) {
		ch <- c
	}
	ch <- rowchange.Change{Op: rowchange.OpTxnEnd}
	close(ch)

	for _, table := range []string{"a", "b"} {
		b, err := p.Next(context.Background())
		if err != nil || b == nil {
			t.Fatalf("%s: %v %v", table, b, err)
		}
		r, err := transport.NewBatchReader(b.Record, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := r.Position(r.NumRows() - 1); b.Table != table || got != "g:1-5" || b.Partial {
			t.Errorf("batch of %s: table %s position %q partial=%v, want the transaction's g:1-5 and not partial", table, b.Table, got, b.Partial)
		}
		b.Release()
	}
}

// A table's rows of one transaction cut into several batches: the earlier
// ones say they are partial.
func TestCutTransactionMarksItsEarlierBatchesPartial(t *testing.T) {
	const total = maxBatchRows + 500
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

	var partial []bool
	for {
		b, err := p.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		partial = append(partial, b.Partial)
		b.Release()
	}
	if len(partial) < 2 {
		t.Fatalf("expected the transaction to be cut, got %d batch(es)", len(partial))
	}
	for i, got := range partial {
		if want := i < len(partial)-1; got != want {
			t.Errorf("batch %d of %d: partial=%v, want %v", i+1, len(partial), got, want)
		}
	}
}
