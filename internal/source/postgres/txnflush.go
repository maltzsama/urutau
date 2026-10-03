package postgres

import (
	"context"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/position"
)

// deleteChangedKey appends a delete of the old key when an upsert UPDATE
// changes the primary key. The update's own delete is built from the new key,
// so without this the old row survives forever; append targets keep it.
func (r *Reader) deleteChangedKey(entry relEntry, before, after []any) error {
	if before == nil || !r.cfg.UpsertTargets[entry.ref.Target] {
		return nil
	}
	oldKey := keyTuple(entry.state, entry.ref, before)
	newKey := keyTuple(entry.state, entry.ref, after)
	if rowchange.KeyString(oldKey) != rowchange.KeyString(newKey) {
		return r.appendChange(entry, rowchange.OpDelete, nil, before)
	}
	return nil
}

// closeTxn materializes the committing transaction's remaining rows, stamps
// only its final record with the commit LSN, and emits every record in order.
// Earlier pieces carry the previous safe position, so an ack cannot advance
// the durable checkpoint past rows a later piece still owes (#456).
func (r *Reader) closeTxn(ctx context.Context, pos position.LSN) error {
	for _, target := range r.encoderOrder {
		if rec := r.encoders[target].materialize(); rec != nil {
			r.pending = append(r.pending, heldRec{target: target, rec: rec})
		}
	}
	if len(r.pending) == 0 {
		return nil
	}
	last := &r.pending[len(r.pending)-1]
	rebuilt, err := transport.WithPosition(last.rec, pos.String())
	if err != nil {
		r.releasePending()
		return err
	}
	last.rec.Release()
	last.rec = rebuilt

	pending := r.pending
	r.pending = nil
	for i, hb := range pending {
		b := &dataplane.Batch{Table: hb.target, Record: hb.rec, Mode: dataplane.UpsertMode}
		select {
		case r.batchOut <- b:
		case <-ctx.Done():
			// Release this record and every record not yet sent.
			for _, rest := range pending[i:] {
				if rest.rec != nil {
					rest.rec.Release()
				}
			}
			return ctx.Err()
		}
	}
	r.safePos = pos.String()
	return nil
}
