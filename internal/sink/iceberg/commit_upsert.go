package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go"

	"github.com/maltzsama/urutau/dataplane"
)

// commitUpsert writes an upsert batch's equality-delete files and its data
// files once, then commits BOTH in one RowDelta snapshot carrying the position
// and snapshot state, retrying only the catalog commit. One atomic snapshot
// means a reader never sees an updated key absent between a delete snapshot
// and a later append snapshot (#550), and the position cannot advance past
// rows that did not land. A delete-only batch commits the deletes and the
// position with no rows; the empty-batch case is handled in Commit.
func (w *TableWriter) commitUpsert(ctx context.Context, keys [][]any, upsertBatch *dataplane.Batch, pos, snapshotState string, snapshotPending []uint32) error {
	hasRows := upsertBatch != nil && upsertBatch.Record.NumRows() > 0
	var delFiles, dataFiles []iceberg.DataFile

	// Write the files ONCE — this is the object-store work; only the catalog
	// commit below is retried, so a retry costs a commit, not a re-upload.
	if len(keys) > 0 || hasRows {
		tbl, err := w.cat.LoadTable(ctx, w.ident)
		if err != nil {
			return fmt.Errorf("iceberg: load %v: %w", w.ident, err)
		}
		if len(keys) > 0 && !w.positional {
			rec, err := w.deleteRecord(keys)
			if err != nil {
				return err
			}
			delFiles, err = tbl.NewTransaction().WriteEqualityDeletes(ctx, w.eqIDs, oneBatch(rec))
			rec.Release()
			if err != nil {
				return fmt.Errorf("iceberg: write equality deletes %v: %w", w.ident, err)
			}
		}
		if hasRows {
			rec, err := w.projectRecord(ctx, upsertBatch)
			if err != nil {
				return err
			}
			dataFiles, err = w.writeDataFiles(ctx, tbl, rec)
			rec.Release()
			if err != nil {
				return err
			}
		}
	}
	resolved := w.positional && len(keys) > 0 // deletes resolved per attempt
	if len(delFiles) == 0 && len(dataFiles) == 0 && !resolved && pos == "" && snapshotState == "" && snapshotPending == nil {
		return nil
	}

	// A retry after a lost catalog response must not re-add the same files
	// (issue #123, same guard the other paths use).
	key := cycleKey(delFiles, dataFiles, pos)
	if resolved {
		var err error
		if key, err = w.keyedCycle(keys, dataFiles, pos); err != nil {
			return err
		}
	}
	p := props(pos)
	addSnapshotProps(p, snapshotState, snapshotPending)
	p[propCycle] = key

	var lastErr error
	for attempt := 0; attempt < w.maxTries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, backoffDuration(w.backoff, attempt)); err != nil {
				return err
			}
		}
		tbl, err := w.cat.LoadTable(ctx, w.ident) // fresh metadata every attempt
		if err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		if cycleCommitted(tbl.Properties(), tbl.CurrentSnapshot(), key) {
			return nil // a previous attempt's commit landed
		}
		txn := tbl.NewTransaction()
		// The row delta stages the row-level update onto the transaction; the
		// single catalog commit is below, so the deletes, the rows and the
		// position land in one atomic snapshot.
		if err := w.stageRowDelta(ctx, tbl, txn, p, keys, delFiles, dataFiles); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		if err := txn.SetProperties(p); err != nil {
			return err
		}
		if _, err := txn.Commit(ctx); err != nil {
			if !isRetryableError(err) {
				return err
			}
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("%w: upsert commit on %v: %w", ErrCommitExhausted, w.ident, lastErr)
}
