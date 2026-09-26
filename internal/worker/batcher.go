package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/sink"
)

// readyBatch is a collapsed batch ready for commit.
type readyBatch struct {
	batch   *dataplane.Batch
	rows    int // rows fed into the batcher for this flush
	upserts int // surviving upsert rows
	deletes int // equality-delete rows
	// ackPos, when set, is the position acked once the batch is committed,
	// in place of its watermark: a snapshot-done batch commits no position
	// but acks the marker's (#428).
	ackPos []byte
}

// runCommitter reads prepared batches and commits them serially.
// The batch is already columnar; the committer hands it straight to the
// sink (commit 3 — the worker's main path produces dataplane.Batch).
func (w *Worker) runCommitter(ctx context.Context, p *tablePipeline) error {
	for rb := range p.readyCh {
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
		// coordinator's commit (staged). Acking first advanced the
		// coordinator's confirmed position over data that a failed commit
		// never made durable — a crash then re-read past the lost window
		// (issue #260). In staged mode the durable point is the coordinator's
		// CommitStaged, so this ack is a delivery receipt, but it must still
		// follow a successful stage.
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

// runBatcher collects changes, collapses them, and sends ready batches to
// the committer. When the channel closes, the committer drains and exits.
func (w *Worker) runBatcher(ctx context.Context, p *tablePipeline) error {
	// The batcher is COLUMNAR (G2): pending holds owned wire batches, never
	// decoded rows. Per-row decisions (bootstrap marking, window dedup,
	// drift, append delete-image) read the record through a BatchReader and
	// mutate only side state (guard, windows, counters); the batches flow
	// through to the columnar flush untouched.
	var pending []*dataplane.Batch
	pendingRows := 0
	// pendingSeq is the coordinator cycle key shared by every batch in
	// pending. A batch with a different Seq belongs to a different cycle and
	// must be flushed on its own (WK-001 C5.4): the coordinator tracks each
	// Seq as one cycle and expects exactly one delivery per (table, seq).
	pendingSeq := uint64(0)
	ticker := time.NewTicker(w.cfg.MaxInterval)
	defer ticker.Stop()

	// freePending releases every buffered batch (ownership returns here on
	// error paths).
	freePending := func() {
		for _, b := range pending {
			b.Release()
		}
		pending = nil
		pendingRows = 0
		pendingSeq = 0
	}

	// ready sends a prepared batch to the committer (ownership transfers).
	ready := func(b *dataplane.Batch, rows, upserts, deletes int) error {
		select {
		case p.readyCh <- readyBatch{batch: b, rows: rows, upserts: upserts, deletes: deletes}:
			return nil
		case <-ctx.Done():
			b.Release()
			return ctx.Err()
		}
	}

	// deliverEmpty ships an empty descriptor for a cycle whose sub-batch
	// produced no data; Seq 0 is a worker-generated snapshot batch, not a
	// cycle, and needs no delivery.
	deliverEmpty := func(b *dataplane.Batch) error {
		if !p.stage(b) || b.Seq == 0 {
			return nil
		}
		return ready(dpint.EmptyBatch(b, p.mode), 0, 0, 0)
	}

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		merged, err := dpint.ConcatBatches(pending)
		if err != nil {
			freePending()
			return fmt.Errorf("worker: table %s: concat: %w", p.target, err)
		}
		rows := int(merged.Record.NumRows())
		pos := dpint.LastRowPos(merged)

		p.snapshotMu.Lock()
		inSnapshot := p.snapshotState == string(snapshot.StateInProgress)
		guard := p.bootstrapGuard
		resumed := p.snapshotResumed
		snapState := p.snapshotState
		snapPending := p.snapshotPending
		p.snapshotMu.Unlock()

		defer func() {
			merged.Release()
			freePending()
		}()

		// Snapshot partition: untouched snapshot PKs are pure-appended (no
		// equality delete); everything else collapses columnar.
		if inSnapshot && !resumed && p.mode == dataplane.UpsertMode {
			untouchedIdx, restIdx, err := partitionSnapshotRows(merged, guard, p.knownSchema.PrimaryKey)
			if err != nil {
				return err
			}
			appendPos := pos
			delCount := 0
			if len(restIdx) > 0 {
				appendPos = ""
			}
			if len(untouchedIdx) > 0 {
				ab, err := dpint.SelectRows(merged, untouchedIdx, appendPos, dataplane.AppendMode, snapState, snapPending)
				if err != nil {
					return fmt.Errorf("worker: table %s: select append: %w", p.target, err)
				}
				if err := ready(ab, len(untouchedIdx), len(untouchedIdx), 0); err != nil {
					return err
				}
			}
			if len(restIdx) > 0 {
				rest, err := dpint.SelectRows(merged, restIdx, pos, dataplane.UpsertMode, snapState, snapPending)
				if err != nil {
					return fmt.Errorf("worker: table %s: select rest: %w", p.target, err)
				}
				upCount, dCount, err := collapseAndSend(ctx, p, rest, rows, pos, ready)
				delCount = dCount
				if err != nil {
					return err
				}
				_ = upCount
				_ = delCount
			}
			return nil
		}

		switch p.mode {
		case dataplane.AppendMode:
			// Append: keep non-delete rows; delete rows are dropped when
			// onDelete is skip, or when they carry no image (a key-only
			// tombstone must never write an all-null row). A delete WITH an
			// image is kept — its row is the before image (DELETE IMAGE
			// CONTRACT: on the wire the image already sits in the data
			// columns) and the append sink writes it.
			keepIdx, err := appendRowsToKeep(ctx, p, w, merged)
			if err != nil {
				return err
			}
			out, err := dpint.SelectRows(merged, keepIdx, pos, dataplane.AppendMode, snapState, snapPending)
			if err != nil {
				return fmt.Errorf("worker: table %s: select append: %w", p.target, err)
			}
			// Every row was dropped (e.g. an append flush of delete
			// tombstones with no before image): nothing to commit, but the
			// coordinator's cycle still needs its delivery.
			if out == nil {
				return deliverEmpty(merged)
			}
			return ready(out, rows, len(keepIdx), 0)
		default:
			// Upsert: collapse the whole buffer columnar, merge survivors
			// into one batch (the position never separates from its data).
			upCount, delCount, err := collapseAndSend(ctx, p, merged, rows, pos, ready)
			if err != nil {
				return err
			}
			_ = upCount
			_ = delCount
			return nil
		}
	}

	// addPending buffers one batch, flushing first when its coordinator cycle
	// (Seq) differs from the buffered one. Snapshot/window batches carry Seq 0
	// and merge freely; each live binlog batch's sub-batch is its own cycle.
	// Ownership of b transfers to pending.
	addPending := func(b *dataplane.Batch, rows int) error {
		// Only a staged batch is a coordinator cycle: for every other batch
		// the flush stays on MaxRows/MaxInterval.
		if p.stage(b) && len(pending) > 0 && b.Seq != pendingSeq {
			if err := flush(); err != nil {
				return err
			}
		}
		pending = append(pending, b)
		pendingSeq = b.Seq
		pendingRows += rows
		if w.cfg.MaxRows > 0 && pendingRows >= w.cfg.MaxRows {
			return flush()
		}
		return nil
	}

	for {
		select {
		case ing, ok := <-p.ch:
			if !ok {
				return flush()
			}
			// Closes marker: emit the chunk's remaining window rows (the
			// stored snapshot batch minus the keys live events touched),
			// adopting the marker's position.
			if ing.Win != nil && ing.Win.Closes {
				cb, err := closeWindow(p, ing)
				if err != nil {
					return err
				}
				// A marker with a seq is a cycle of the coordinator's send
				// order: the window's rows go out as that cycle, so they
				// commit after every live cycle released ahead of the
				// marker (#416). A window that emits no rows still owes
				// the cycle its delivery.
				if ing.Seq != 0 {
					if cb != nil {
						cb.Seq, cb.Staged = ing.Seq, ing.Staged
					} else {
						eb, err := markerBatch(p, ing)
						if err != nil {
							return err
						}
						err = deliverEmpty(eb)
						eb.Release()
						if err != nil {
							return err
						}
					}
				}
				if cb != nil {
					if err := addPending(cb, int(cb.Record.NumRows())); err != nil {
						return err
					}
				}
				continue
			}
			// Snapshot done: commit what came before it (the table's last
			// windows among it), then the completion, in that order. A
			// completion committed ahead of a window would let a crash skip
			// the re-snapshot that window's rows need (issue #428).
			if ing.SnapshotDone {
				if err := flush(); err != nil {
					return err
				}
				db, err := snapshotDoneBatch(p, ing)
				if err != nil {
					return err
				}
				select {
				case p.readyCh <- readyBatch{batch: db, ackPos: []byte(ing.Position)}:
				case <-ctx.Done():
					db.Release()
					return ctx.Err()
				}
				continue
			}
			if ing.Batch == nil {
				continue
			}
			batch := ing.Batch
			if batch.Record == nil || batch.Record.NumRows() == 0 {
				batch.Release()
				continue
			}
			// Schema drift, columnar: any data column the batch carries that
			// the known schema lacks is a spec violation — report once and
			// go terminal. Runs on the SOURCE batch before enrich (enrich
			// adds reference columns that must not trip drift).
			if len(p.knownSchema.Columns) > 0 {
				d, hit, err := schemaDrift(batch, p.knownSchema)
				if err != nil {
					batch.Release()
					return fmt.Errorf("worker: table %s: %w", p.target, err)
				}
				if hit {
					p.snapshotMu.Lock()
					first := !p.driftReported[d.Column]
					p.driftReported[d.Column] = true
					p.snapshotMu.Unlock()
					if first && w.schemaDrift != nil {
						w.schemaDrift(SchemaDrift{Table: p.target, Column: d.Column, Kind: d.Kind})
					}
					batch.Release()
					return fmt.Errorf("worker: table %s: schema drift: column %q is not in the spec — declare it and resume", p.target, d.Column)
				}
			}
			// Enrich the whole batch (columnar seam). Deletes bypass the
			// join, so no single-row special case is needed.
			origRows := int(batch.Record.NumRows())
			if p.enricher != nil {
				enriched, err := p.enricher.EnrichBatch(ctx, batch, p.knownSchema.PrimaryKey)
				batch.Release()
				if err != nil {
					return fmt.Errorf("worker: table %s: enrich: %w", p.target, err)
				}
				if enriched == nil {
					p.enrichDropped.Add(int64(origRows))
					continue
				}
				p.enrichDropped.Add(int64(origRows - int(enriched.Record.NumRows())))
				batch = enriched
			}

			// Per-row side effects, read columnar: bootstrap marking for
			// live keys, InWindow dedup against the open windows.
			if err := markBatchSideEffects(p, batch, ing); err != nil {
				batch.Release()
				return err
			}

			if err := addPending(batch, int(batch.Record.NumRows())); err != nil {
				return err
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			freePending()
			return ctx.Err()
		}
	}
}

// collapseAndSend collapses one batch columnar and sends the merged single
// batch to the committer.
func collapseAndSend(ctx context.Context, p *tablePipeline, b *dataplane.Batch, rows int, pos string, ready func(*dataplane.Batch, int, int, int) error) (upCount, delCount int, err error) {
	p.snapshotMu.Lock()
	snapState := p.snapshotState
	snapPending := p.snapshotPending
	p.snapshotMu.Unlock()
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
	combined, err := dpint.MergeBatches(upserts, deletes, nil)
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
