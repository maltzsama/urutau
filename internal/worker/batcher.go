package worker

import (
	"context"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/columnar"
)

// batcher buffers, collapses and flushes one table's ingest into ready
// batches for the committer. It is the worker's core loop, decomposed into
// one method per event kind (issue #401) so each branch is unit-testable.
type batcher struct {
	w   *Worker
	p   *tablePipeline
	ctx context.Context

	// The batcher is COLUMNAR (G2): pending holds owned wire batches, never
	// decoded rows. Per-row decisions (bootstrap marking, window dedup,
	// drift, append delete-image) read the record through a BatchReader and
	// mutate only side state (guard, windows, counters); the batches flow
	// through to the columnar flush untouched.
	pending      []*dataplane.Batch
	pendingRows  int
	pendingBytes int64
	// pendingSeq is the coordinator cycle key shared by every batch in
	// pending. A batch with a different Seq belongs to a different cycle and
	// must be flushed on its own (WK-001 C5.4): the coordinator tracks each
	// Seq as one cycle and expects exactly one delivery per (table, seq).
	pendingSeq uint64
	// pendingMarkers are the markers of pending's windows (queueMarkers).
	pendingMarkers []uint64
}

// runBatcher collects changes, collapses them, and sends ready batches to
// the committer. When the channel closes, the committer drains and exits.
func (w *Worker) runBatcher(ctx context.Context, p *tablePipeline) error {
	return (&batcher{w: w, p: p, ctx: ctx}).run()
}

// run pumps one table's ingest until the channel closes or an error stops the
// pipeline, dispatching each event to its handler.
func (b *batcher) run() error {
	ticker := time.NewTicker(b.w.cfg.MaxInterval)
	defer ticker.Stop()
	// Release buffered batches on an early error return (issue #490).
	defer b.freePending()
	for {
		select {
		case ing, ok := <-b.p.ch:
			if !ok {
				return b.flush()
			}
			if err := b.handleIngest(ing); err != nil {
				return err
			}
		case <-ticker.C:
			if err := b.flush(); err != nil {
				return err
			}
		case <-b.ctx.Done():
			b.freePending()
			return b.ctx.Err()
		}
	}
}

// handleIngest routes one unit by kind: a Closes marker, a snapshot
// completion, or a data batch.
func (b *batcher) handleIngest(ing Ingest) error {
	switch {
	case ing.Win != nil && ing.Win.Closes:
		return b.handleCloses(ing)
	case ing.SnapshotDone:
		return b.handleSnapshotDone(ing)
	case ing.Batch == nil:
		return nil
	default:
		return b.handleData(ing)
	}
}

// handleCloses emits the chunk's remaining window rows (the stored snapshot
// batch minus the keys live events touched), adopting the marker's position.
func (b *batcher) handleCloses(ing Ingest) error {
	cb, err := closesBatch(b.p, ing, b.deliverEmpty)
	if err != nil {
		return err
	}
	// The window holds the snapshot rows: enrich them too, or the whole
	// backfill lands with NULL reference columns (issue #564).
	cb, err = b.p.enrichWindowRows(b.ctx, cb, b.bufferEmpty)
	if err != nil {
		return err
	}
	if cb == nil {
		// No rows to commit. On a staged table the window still owes its
		// cycle an empty delivery, which the committer ships: the marker is
		// acked behind it, by the committer. Acked here, a worker killed
		// before the ship left a marker the coordinator would never
		// redeliver and a cycle waiting for a delivery nobody would send
		// (#833).
		if ing.MarkerID != 0 {
			return queueMarkers(b.ctx, b.p.readyCh, []uint64{ing.MarkerID})
		}
	} else if ing.MarkerID != 0 {
		b.pendingMarkers = append(b.pendingMarkers, ing.MarkerID)
	}
	if cb != nil {
		return b.addPending(cb, int(cb.Record.NumRows()))
	}
	return nil
}

// handleSnapshotDone commits what came before the completion (the table's
// last windows among it), then the completion itself, in that order. A
// completion committed ahead of a window would let a crash skip the
// re-snapshot that window's rows need (issue #428).
func (b *batcher) handleSnapshotDone(ing Ingest) error {
	if err := b.flush(); err != nil {
		return err
	}
	db, err := snapshotDoneBatch(b.p, ing)
	if err != nil {
		return err
	}
	select {
	case b.p.readyCh <- readyBatch{batch: db, ackPos: []byte(ing.Position)}:
		return nil
	case <-b.ctx.Done():
		db.Release()
		return b.ctx.Err()
	}
}

// handleData processes one live data batch: drift check (on the source shape,
// before enrich), enrichment, per-row side effects, then buffering.
func (b *batcher) handleData(ing Ingest) error {
	batch := ing.Batch
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		batch.Release()
		return nil
	}
	// Schema drift, columnar: any data column the batch carries that the
	// known schema lacks is a spec violation — report once and go terminal.
	// Runs on the SOURCE batch before enrich (enrich adds reference columns
	// that must not trip drift).
	if err := b.w.checkSchemaDrift(b.p, batch); err != nil {
		return err
	}
	// Enrich the whole batch (columnar seam). Deletes bypass the join, so no
	// single-row special case is needed.
	enriched, dropped, err := b.p.enrichStreamBatch(b.ctx, batch, b.bufferEmpty)
	if err != nil {
		return err
	}
	if dropped {
		return nil
	}
	batch = enriched
	// Per-row side effects, read columnar: bootstrap marking for live keys,
	// InWindow dedup against the open windows.
	if err := markBatchSideEffects(b.p, batch, ing); err != nil {
		batch.Release()
		return err
	}
	return b.addPending(batch, int(batch.Record.NumRows()))
}

// freePending releases every buffered batch (ownership returns here on error
// paths).
func (b *batcher) freePending() {
	for _, pb := range b.pending {
		pb.Release()
	}
	b.pending = nil
	b.pendingRows, b.pendingBytes = 0, 0
	b.pendingSeq = 0
}

// ready sends a prepared batch to the committer (ownership transfers).
func (b *batcher) ready(bt *dataplane.Batch, rows, upserts, deletes int) error {
	select {
	case b.p.readyCh <- readyBatch{batch: bt, rows: rows, upserts: upserts, deletes: deletes}:
		return nil
	case <-b.ctx.Done():
		bt.Release()
		return b.ctx.Err()
	}
}

// deliverEmpty ships an empty descriptor for a cycle whose sub-batch produced
// no data; Seq 0 is a worker-generated snapshot batch, not a cycle, and needs
// no delivery.
func (b *batcher) deliverEmpty(bt *dataplane.Batch) error {
	if !b.p.stage(bt) || bt.Seq == 0 {
		return nil
	}
	return b.ready(columnar.EmptyBatch(bt, b.p.mode), 0, 0, 0)
}

// flush commits the pending batches and then queues the markers of the
// windows they carried, in that order.
func (b *batcher) flush() error {
	markers := b.pendingMarkers
	b.pendingMarkers = nil
	if err := b.commitPending(); err != nil {
		return err
	}
	return queueMarkers(b.ctx, b.p.readyCh, markers)
}

// addPending buffers one batch, flushing first when its coordinator cycle
// (Seq) differs from the buffered one; snapshot/window batches carry Seq 0
// and merge freely. Ownership transfers to pending only once appended.
func (b *batcher) addPending(bt *dataplane.Batch, rows int) error {
	// Only a staged batch is a coordinator cycle: for every other batch the
	// flush stays on MaxRows/MaxInterval.
	if b.p.stage(bt) && len(b.pending) > 0 && bt.Seq != b.pendingSeq {
		if err := b.flush(); err != nil {
			bt.Release() // bt was never buffered (issue #490)
			return err
		}
	}
	b.pending = append(b.pending, bt)
	b.pendingSeq = bt.Seq
	b.pendingRows, b.pendingBytes = b.pendingRows+rows, b.pendingBytes+columnar.BatchBytes(bt)
	if (b.w.cfg.MaxRows > 0 && b.pendingRows >= b.w.cfg.MaxRows) || b.pendingBytes >= b.w.cfg.MaxBytes {
		return b.flush()
	}
	return nil
}

// bufferEmpty queues an empty batch so the flush delivers it in Seq order.
func (b *batcher) bufferEmpty(bt *dataplane.Batch) error {
	return b.addPending(columnar.EmptyBatch(bt, b.p.mode), 0)
}
