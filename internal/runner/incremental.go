package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// runIncremental drains each incremental table in bounded pages (#572),
// pushing the rows through the worker's ingest path — the same path a snapshot
// uses, minus the window. The cursor is read from cdc.cursor and persisted
// post-commit by the OnCommit callback, so the data and the cursor advance
// together.
//
// A source that implements source.BatchIncrementalSource (#733) encodes the
// page straight into Arrow here; the runner passes it the same canonical wire
// schema it installs on the worker (KnownSchema), so the direct path is
// byte-for-byte the map path's RecordFromChanges. A source that does not, or a
// table whose schema is not yet known, falls back to source.IncrementalSource.
func (r *Runner) runIncremental(ctx context.Context, src source.Source, snk sink.Sink, refs []core.TableRef, specBySource map[string]spec.Table, w *worker.Worker, ingest chan worker.Ingest) error {
	inc, incOK := src.(source.IncrementalSource)
	batchInc, batchOK := src.(source.BatchIncrementalSource)
	if !incOK && !batchOK {
		return fmt.Errorf("runner: source does not support incremental mode")
	}
	for _, ref := range refs {
		t := specBySource[ref.Source]
		// The committed cdc.position IS the cursor for an incremental table:
		// the batch watermark was written atomically with the rows.
		after, err := snk.Position(ctx, ref)
		if err != nil {
			return fmt.Errorf("runner: incremental %s: %w", ref.Target, err)
		}
		// The batch path needs the canonical schema to build its encoder; with
		// none (an unregistered table) it cannot match the map path, so it
		// falls back rather than infer a drifting shape.
		known := w.KnownSchema(ref.Target)
		direct := batchOK && len(known.Columns) > 0
		if !direct && !incOK {
			return fmt.Errorf("runner: incremental %s: source implements only the columnar path and the target schema is unknown", ref.Target)
		}
		// Drain the table a page at a time: the source returns one bounded page
		// and says whether more is already available, so a large table never
		// loads entirely into memory (issue #572).
		for {
			next, dpb, more, err := incrementalPage(ctx, inc, batchInc, ref, t, after, known, direct)
			if err != nil {
				return fmt.Errorf("runner: incremental %s: %w", ref.Target, err)
			}
			if dpb == nil {
				break
			}
			select {
			case ingest <- worker.Ingest{Table: ref.Target, Batch: dpb}:
			case <-ctx.Done():
				return ctx.Err()
			}
			r.log.Info("incremental read", "table", ref.Source, "rows", int(dpb.Record.NumRows()), "cursor", next)
			after = next
			if !more {
				break
			}
		}
	}
	return nil
}

// incrementalPage reads one incremental page and returns it as a ready wire
// batch, or (_, nil, false, nil) at the end of the drain. direct selects the
// columnar BatchIncrementalSource path (encoding straight into Arrow); the
// fallback encodes source.IncrementalSource's row maps with RecordFromChanges,
// exactly as before. Both paths share the cursor/tie-break semantics — the
// source computes next the same way in either case.
func incrementalPage(ctx context.Context, inc source.IncrementalSource, batchInc source.BatchIncrementalSource, ref core.TableRef, t spec.Table, after string, known core.Schema, direct bool) (string, *dataplane.Batch, bool, error) {
	if direct {
		return batchInc.IncrementalBatch(ctx, ref, t.Cursor, after, known)
	}
	next, rows, more, err := inc.Incremental(ctx, ref, t.Cursor, after)
	if err != nil {
		return "", nil, false, err
	}
	if len(rows) == 0 {
		return "", nil, false, nil
	}
	changes := make([]rowchange.Change, len(rows))
	for i, row := range rows {
		key := make([]any, 0, len(ref.PrimaryKey))
		for _, pk := range ref.PrimaryKey {
			key = append(key, row[pk])
		}
		changes[i] = rowchange.Change{
			Op:       rowchange.OpInsert,
			Table:    ref.Target,
			Key:      key,
			After:    row,
			Position: next,
			Snapshot: false,
			Phase:    core.PhaseIncremental,
			IngestTS: time.Now(),
		}
	}
	rec, err := transport.RecordFromChanges(changes, transport.MergeSchema(changes, known), nil)
	if err != nil {
		return "", nil, false, err
	}
	dpb := &dataplane.Batch{Table: ref.Target, Record: rec, Mode: dataplane.UpsertMode, Watermark: []byte(next)}
	return next, dpb, more, nil
}

// canonicalForTarget returns the canonical schema for one target table (the
// schema drift reference). Zero schema when the target has no source.
func canonicalForTarget(canonical map[string]core.Schema, refs []core.TableRef, target string) core.Schema {
	for _, ref := range refs {
		if ref.Target == target {
			return canonical[ref.Source]
		}
	}
	return core.Schema{}
}

// markSnapshotsPending marks each table about to be snapshotted not_started,
// unless an earlier run already left it unfinished (its in_progress carries
// resumable bounds). It returns the tables that already hold committed rows:
// the bloom guard of this run never saw the keys an earlier run wrote, so
// their snapshot rows must take the upsert path.
func markSnapshotsPending(ctx context.Context, snk sink.Sink, refs []core.TableRef) (map[string]bool, error) {
	held := map[string]bool{}
	for _, ref := range refs {
		pos, err := snk.Position(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: %w", ref.Target, err)
		}
		held[ref.Target] = pos != ""
		props, err := snk.Properties(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("runner: %s: snapshot state: %w", ref.Target, err)
		}
		if snapshot.Unfinished(props) {
			continue
		}
		mark := map[string]string{snapshot.PropSnapshotState: string(snapshot.StateNotStarted)}
		if err := snk.SetProperties(ctx, ref, mark); err != nil {
			return nil, fmt.Errorf("runner: %s: mark snapshot pending: %w", ref.Target, err)
		}
	}
	return held, nil
}
