package postgres

import (
	"context"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/position"
)

// flushTxn hands each buffered row of the committing transaction to the
// channel, stamped with the transaction's commit LSN, then closes the
// transaction with OpTxnEnd when it emitted rows. The puller batches whole
// transactions, so a commit never records a position with only part of a
// transaction.
func (r *Reader) flushTxn(ctx context.Context, pos position.LSN) error {
	emitted := false
	for _, c := range r.txn {
		c.Position = pos.String()
		select {
		case r.out <- *c:
			emitted = true
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.txn = r.txn[:0]
	if !emitted {
		return nil
	}
	select {
	case r.out <- rowchange.Change{Op: rowchange.OpTxnEnd}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
