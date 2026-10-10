package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
)

// runCommitter reads prepared batches and commits them serially.
// The batch is already columnar; the committer hands it straight to the
// sink (commit 3 — the worker's main path produces dataplane.Batch).
func (w *Worker) runCommitter(ctx context.Context, p *tablePipeline) error {
	for rb := range p.readyCh {
		if w.ackMarkers(p, rb) {
			continue
		}
		start := time.Now()
		// A zero-value WriteMode would silently default to upsert semantics.
		// Every batch must carry an explicit mode; reject the unset one.
		if rb.batch.Mode == dataplane.ModeUnset {
			rb.batch.Release()
			return fmt.Errorf("worker: table %s: batch has no write mode set", p.target)
		}
		if p.stage(rb.batch) {
			if err := w.stageBatch(ctx, p, rb.batch); err != nil {
				rb.batch.Release()
				if w.metrics != nil {
					w.metrics.CommitFailures.WithLabelValues(p.target).Inc()
				}
				return err
			}
			faultinject.At(faultinject.WorkerStagedShippedBeforeAck,
				"table", p.target, "seq", rb.batch.Seq, "position", string(rb.batch.Watermark))
		} else {
			faultinject.At(faultinject.WorkerCommitBefore,
				"table", p.target, "seq", rb.batch.Seq, "position", string(rb.batch.Watermark))
			if err := p.committer.Commit(ctx, rb.batch); err != nil {
				rb.batch.Release()
				if w.metrics != nil {
					w.metrics.CommitFailures.WithLabelValues(p.target).Inc()
				}
				return fmt.Errorf("worker: table %s: commit: %w", p.target, err)
			}
			faultinject.At(faultinject.WorkerCommittedBeforeAck,
				"table", p.target, "seq", rb.batch.Seq, "position", string(rb.batch.Watermark))
		}
		// Ack only AFTER the batch is durable (non-staged) or staged for the
		// coordinator's CommitStaged (staged): acking first advances the
		// confirmed position over a batch the commit did not persist, and a
		// crash re-reads past the lost window (issue #260).
		if rb.ackPos != nil {
			rb.batch.Watermark = rb.ackPos
		}
		if w.onCommit != nil {
			w.onCommit(rb.batch, rb.rows)
		}
		rb.batch.Release()
		if w.metrics != nil {
			w.metrics.CommitDuration.WithLabelValues(p.target).Observe(time.Since(start).Seconds())
			w.metrics.CommitLatencyMs.WithLabelValues(p.target).Set(float64(time.Since(start).Milliseconds()))
			w.metrics.RowsWritten.WithLabelValues(p.target, "upsert").Add(float64(rb.upserts))
			w.metrics.EqualityDeletes.WithLabelValues(p.target).Add(float64(rb.deletes))
		}
	}
	return nil
}

// stageBatch writes a batch's data files without committing them and ships
// the descriptor to the coordinator, which commits the whole cycle (WK-001
// C5). Nothing is visible in the table until that commit.
func (w *Worker) stageBatch(ctx context.Context, p *tablePipeline, b *dataplane.Batch) error {
	sw, ok := p.committer.(sink.StagingWriter)
	if !ok {
		return fmt.Errorf("worker: table %s: staged assignment but writer cannot stage", p.target)
	}
	desc, err := sw.WriteStaged(ctx, b)
	if err != nil {
		return fmt.Errorf("worker: table %s: stage: %w", p.target, err)
	}
	if w.onStaged == nil {
		return fmt.Errorf("worker: table %s: staged assignment without a delivery callback", p.target)
	}
	faultinject.At(faultinject.WorkerStagedBeforeShip,
		"table", p.target, "seq", b.Seq, "position", string(b.Watermark))
	if err := w.onStaged(p.target, b.Seq, desc, string(b.Watermark), b.SnapshotState, b.SnapshotPending); err != nil {
		return fmt.Errorf("worker: table %s: ship staged descriptor: %w", p.target, err)
	}
	return nil
}

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
		// No rows to commit: the marker is done now.
		b.w.markerCommitted(b.p.target, ing.MarkerID)
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
	return b.ready(dpint.EmptyBatch(bt, b.p.mode), 0, 0, 0)
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

// commitPending concatenates the pending batches and commits them by mode:
// a snapshot partition (untouched PKs appended, the rest upserted), an append
// flush, or a columnar upsert collapse.
func (b *batcher) commitPending() error {
	if len(b.pending) == 0 {
		return nil
	}
	merged, err := dpint.ConcatBatches(memory.DefaultAllocator, b.pending)
	if err != nil {
		b.freePending()
		return fmt.Errorf("worker: table %s: concat: %w", b.p.target, err)
	}
	rows := int(merged.Record.NumRows())
	pos := dpint.LastRowPos(merged)

	b.p.snapshotMu.Lock()
	inSnapshot := b.p.snapshotState == string(snapshot.StateInProgress)
	guard := b.p.bootstrapGuard
	resumed := b.p.snapshotResumed
	snapState, snapPending := b.p.snapshotState, b.p.snapshotPending
	b.p.snapshotMu.Unlock()
	snapState, snapPending = flushSnapshotProgress(snapState, snapPending, b.pending, merged)

	defer func() {
		merged.Release()
		b.freePending()
	}()

	// Snapshot partition: untouched snapshot PKs are pure-appended (no
	// equality delete); everything else collapses columnar.
	//
	// This emits TWO ready deliveries with the same Seq (append then upsert).
	// It relies on the collapsed runner's per-batch send order; the
	// distributed coordinator's snapshot path must NOT set SetSnapshotState
	// until the two deliveries are unified into one, or it drops the second
	// (issue #559).
	if inSnapshot && !resumed && b.p.mode == dataplane.UpsertMode {
		return b.commitSnapshot(merged, pos, guard, snapState, snapPending)
	}

	switch b.p.mode {
	case dataplane.AppendMode:
		return b.commitAppend(merged, pos, snapState, snapPending)
	default:
		// Upsert: collapse the whole buffer columnar, merge survivors into
		// one batch (the position never separates from its data).
		_, _, err := collapseAndSend(b.ctx, b.p, merged, rows, pos, b.ready)
		return err
	}
}

// commitSnapshot splits a merged snapshot batch into untouched PKs (pure
// append) and the rest (upsert) and sends both.
func (b *batcher) commitSnapshot(merged *dataplane.Batch, pos string, guard *bloom.BloomFilter, snapState string, snapPending []uint32) error {
	untouchedIdx, restIdx, err := partitionSnapshotRows(merged, guard, b.p.knownSchema.PrimaryKey)
	if err != nil {
		return err
	}
	appendPos := pos
	if len(restIdx) > 0 {
		appendPos = ""
	}
	if len(untouchedIdx) > 0 {
		ab, err := dpint.SelectRows(merged, untouchedIdx, appendPos, dataplane.AppendMode, snapState, snapPending)
		if err != nil {
			return fmt.Errorf("worker: table %s: select append: %w", b.p.target, err)
		}
		if err := b.ready(ab, len(untouchedIdx), len(untouchedIdx), 0); err != nil {
			return err
		}
	}
	if len(restIdx) > 0 {
		rest, err := dpint.SelectRows(merged, restIdx, pos, dataplane.UpsertMode, snapState, snapPending)
		if err != nil {
			return fmt.Errorf("worker: table %s: select rest: %w", b.p.target, err)
		}
		_, _, err = collapseAndSend(b.ctx, b.p, rest, int(merged.Record.NumRows()), pos, b.ready)
		rest.Release() // collapseAndSend borrows its input; the caller owns it (issue #489)
		if err != nil {
			return err
		}
	}
	return nil
}

// commitAppend drops deletes the append table does not keep and sends the
// remainder, or an empty descriptor when every row was dropped.
func (b *batcher) commitAppend(merged *dataplane.Batch, pos, snapState string, snapPending []uint32) error {
	keepIdx, err := appendRowsToKeep(b.ctx, b.p, b.w, merged)
	if err != nil {
		return err
	}
	out, err := dpint.SelectRows(merged, keepIdx, pos, dataplane.AppendMode, snapState, snapPending)
	if err != nil {
		return fmt.Errorf("worker: table %s: select append: %w", b.p.target, err)
	}
	// Every row was dropped (e.g. an append flush of delete tombstones with
	// no before image): nothing to commit, but the coordinator's cycle still
	// needs its delivery.
	if out == nil {
		return b.deliverEmpty(merged)
	}
	return b.ready(out, int(merged.Record.NumRows()), len(keepIdx), 0)
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
	b.pendingRows, b.pendingBytes = b.pendingRows+rows, b.pendingBytes+dpint.BatchBytes(bt)
	if (b.w.cfg.MaxRows > 0 && b.pendingRows >= b.w.cfg.MaxRows) || b.pendingBytes >= b.w.cfg.MaxBytes {
		return b.flush()
	}
	return nil
}

// bufferEmpty queues an empty batch so the flush delivers it in Seq order.
func (b *batcher) bufferEmpty(bt *dataplane.Batch) error {
	return b.addPending(dpint.EmptyBatch(bt, b.p.mode), 0)
}

// carrySnapshotPending gives a window's batch the snapshot chunks its Closes
// marker names as still to do, with state in_progress: they commit with the
// window's rows, so a restarted coordinator resumes there (#461).
func carrySnapshotPending(b *dataplane.Batch, ing Ingest) {
	if b != nil && ing.SnapshotPending != nil {
		b.SnapshotState, b.SnapshotPending = string(snapshot.StateInProgress), ing.SnapshotPending
	}
}

// flushSnapshotProgress is the snapshot state a flush commits: the progress
// its windows' Closes markers carried, when they did — the last window's,
// whose rows commit with every earlier one (#461) — else the pipeline's own.
func flushSnapshotProgress(state string, pending []uint32, batches []*dataplane.Batch, merged *dataplane.Batch) (string, []uint32) {
	if markerState, markerPending := batchSnapshotProgress(batches); markerState != "" {
		state, pending = markerState, markerPending
		merged.SnapshotState, merged.SnapshotPending = state, pending
	}
	return state, pending
}

// batchSnapshotProgress returns the snapshot state and pending chunks the last
// of batches carrying them names, or none.
func batchSnapshotProgress(batches []*dataplane.Batch) (string, []uint32) {
	for i := len(batches) - 1; i >= 0; i-- {
		if batches[i].SnapshotState != "" {
			return batches[i].SnapshotState, batches[i].SnapshotPending
		}
	}
	return "", nil
}

// collapseAndSend collapses one batch columnar and sends the merged single
// batch to the committer.
func collapseAndSend(ctx context.Context, p *tablePipeline, b *dataplane.Batch, rows int, pos string, ready func(*dataplane.Batch, int, int, int) error) (upCount, delCount int, err error) {
	p.snapshotMu.Lock()
	snapState := p.snapshotState
	snapPending := p.snapshotPending
	p.snapshotMu.Unlock()
	if b.SnapshotState != "" {
		// A window's marker progress (flushSnapshotProgress) wins (#461).
		snapState, snapPending = b.SnapshotState, b.SnapshotPending
	}
	upserts, deletes, cerr := dpint.Collapse(ctx, nil, b, p.knownSchema.PrimaryKey)
	if cerr != nil {
		return 0, 0, fmt.Errorf("worker: table %s: collapse: %w", p.target, cerr)
	}
	defer func() {
		if upserts != nil {
			upserts.Release()
		}
		if deletes != nil {
			deletes.Release()
		}
	}()
	if upserts != nil {
		upCount = int(upserts.Record.NumRows())
	}
	if deletes != nil {
		delCount = int(deletes.Record.NumRows())
	}
	combined, err := dpint.MergeBatches(nil, upserts, deletes)
	if err != nil {
		return 0, 0, fmt.Errorf("worker: table %s: merge: %w", p.target, err)
	}
	if combined == nil {
		// Nothing survived the collapse: the cycle still needs its delivery
		// so the coordinator does not leave it open (WK-001 C5.4).
		if p.stage(b) && b.Seq != 0 {
			return 0, 0, ready(dpint.EmptyBatch(b, p.mode), 0, 0, 0)
		}
		return 0, 0, nil
	}
	combined.Watermark = []byte(pos)
	combined.Mode = p.mode
	combined.SnapshotState = snapState
	combined.SnapshotPending = snapPending
	if err := ready(combined, rows, upCount, delCount); err != nil {
		return 0, 0, err
	}
	return upCount, delCount, nil
}
