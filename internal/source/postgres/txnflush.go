package postgres

import (
	"context"

	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/position"
)

// deleteChangedKey enqueues a delete of the old key when an upsert UPDATE
// changes the primary key. The update's own delete is built from the new key,
// so without this the old row survives forever; append targets keep it.
func (r *Reader) deleteChangedKey(entry relEntry, before, after map[string]any) {
	if before == nil || !r.cfg.UpsertTargets[entry.ref.Target] {
		return
	}
	oldKey, newKey := keyFrom(entry.state, entry.ref, before), keyFrom(entry.state, entry.ref, after)
	if rowchange.KeyString(oldKey) != rowchange.KeyString(newKey) {
		r.enqueue(entry, rowchange.OpDelete, nil, entry.proj.project(before))
	}
}

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
