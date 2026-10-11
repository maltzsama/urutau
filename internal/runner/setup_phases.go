package runner

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/driver"
	"github.com/maltzsama/urutau/internal/columnar"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/maintenance"
	"github.com/maltzsama/urutau/internal/resume"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// startMaintenance starts one in-process maintenance loop per target table
// (issue #96): there is no separate worker process to hand the pass to. The
// sink is checked through the neutral sink.Maintainable interface; maintenance
// is Iceberg-only and spec.Validate rejects it elsewhere, so a sink lacking
// the capability is a bug, not a config to tolerate.
func (b *setup) startMaintenance() error {
	if !b.s.Sink.MaintenanceEnabled() {
		return nil
	}
	msnk, ok := b.snk.(sink.Maintainable)
	if !ok {
		return fmt.Errorf("runner: sink %q does not support table maintenance (sink.maintenance is Iceberg-only)", b.s.Sink.Type)
	}
	sched := maintenance.NewSchedule()
	for _, ref := range b.refs {
		ref := ref
		// snk.Position reads the table's OWN committed cdc.position rather
		// than an in-memory approximation (compaction runs every few minutes
		// at minimum, so the extra catalog round-trip is not hot-path). A
		// read error just means no position is attached to this pass's
		// compaction commit.
		currentPosition := func() string {
			pos, err := b.snk.Position(b.ctx, ref)
			if err != nil {
				return ""
			}
			return pos
		}
		m := msnk.Maintain(ref, *b.s.Sink.Maintenance, b.log, currentPosition, nil)
		go maintenance.RunLoop(b.runCtx, m, ref.Target, b.s.Sink.Maintenance, sched, b.log)
	}
	return nil
}

// registerCallbacks wires the worker's schema-drift, dropped-delete and commit
// notifications into the audit trail and the confirmed-point bookkeeping.
func (b *setup) registerCallbacks() {
	b.w.OnSchemaDrift(func(d worker.SchemaDrift) {
		b.log.Error("schema drift: pipeline paused", "table", d.Table, "column", d.Column,
			"action", "declare the column in the spec and resume")
		b.r.emit(eventlog.KindSchemaDrift, map[string]any{
			"table": d.Table, "column": d.Column, "kind": d.Kind,
		})
	})
	b.w.OnDroppedDelete(func(table, pos string) {
		b.log.Warn("append-only delete dropped", "table", table, "position", pos,
			"action", "the source carried no before image or the table declares onDelete: skip")
		b.r.emit(eventlog.KindDeleteDropped, map[string]any{
			"table": table, "position": pos,
		})
	})
	b.w.OnCommit(func(bt *dataplane.Batch, rows int) {
		up, del := columnar.CountOps(bt)
		b.log.Info("commit", "table", bt.Table, "rows", rows,
			"upserts", up, "deletes", del, "position", string(bt.Watermark))
		b.r.emit(eventlog.KindCommit, map[string]any{
			"table": bt.Table, "rows": rows,
			"upserts": up, "deletes": del, "position": string(bt.Watermark),
		})
		// An incremental table's cursor is the batch watermark, and never
		// feeds the LSN/confirmed point, so the position parse does not apply.
		if _, ok := b.incrRefByTarget[bt.Table]; ok {
			return
		}
		// A garbage position string never advances the confirmed point.
		p, err := b.src.ParsePosition(string(bt.Watermark))
		if err != nil {
			b.log.Warn("runner: commit position parse", "table", bt.Table, "position", string(bt.Watermark), "err", err)
			return
		}
		b.r.updateCommitted(bt.Table, p)
	})
}

// startWorker launches the worker under the setup context and records its
// completion channels; Run waits for workerDone before releasing the sink.
func (b *setup) startWorker() {
	b.workerErr = make(chan error, 1)
	b.workerDone = make(chan struct{})
	go func() { defer close(b.workerDone); b.workerErr <- b.w.Run(b.runCtx, b.ingest) }()
}

// resolveResume resolves the stream resume point and the snapshot/adopt set
// through the shared resume resolver (internal/resume, the same one the
// coordinator runs — #718 step 3), marks the pending snapshots before the
// stream can commit, and resolves the per-table bootstrap configuration the
// stream start and snapshot loop both consult.
func (b *setup) resolveResume() error {
	// Per-table bootstrap config, resolved once: the shared resolver splits
	// an adopt table out of the snapshot set (its snapshot is marked complete
	// without reading), and the snapshot loop reads the mode back for logging.
	b.bootstrapByTarget = make(map[string]spec.Bootstrap, len(b.s.Tables))
	for _, t := range b.s.Tables {
		if t.Bootstrap != nil {
			b.bootstrapByTarget[t.Target] = *t.Bootstrap
		}
	}

	// The source's snapshot capability gates the snapshot routing: a
	// non-snapshotting source (Kafka) has no query connection and must never
	// be routed into the snapshot phase (issue #394). An unregistered kind
	// has no capabilities.
	caps, _ := driver.CapsForKind(b.s.Source.Kind)
	b.caps = caps

	res, err := resume.Resolve(b.ctx, b.src, b.snk, b.cdcRefs, resume.Options{
		Caps:      caps,
		Bootstrap: b.bootstrapByTarget,
	})
	if err != nil {
		return fmt.Errorf("runner: %w", err)
	}
	b.start = res.Start
	b.snapshotRefs, b.adoptRefs = res.Snapshot, res.Adopt
	// The union marks every table the snapshot phase will touch (snapshot or
	// adopt) not_started before the stream can commit to it.
	b.needsSnapshot = append(append([]core.TableRef{}, res.Snapshot...), res.Adopt...)
	b.log.Info("resume", "from", position.StringOrNone(res.Resume), "snapshot_tables", len(b.needsSnapshot))
	if len(res.Recovery) > 0 {
		b.log.Info("crash recovery: streams ahead of the resume point replay from it",
			"from", position.StringOrNone(res.Resume), "streams", res.Recovery)
	}
	b.r.emit(eventlog.KindResume, map[string]any{
		"from": position.StringOrNone(res.Resume), "snapshot_tables": len(b.needsSnapshot),
	})

	// Before the stream can commit anything: a crash from here on must find
	// these tables unfinished, whatever positions the stream commits to them
	// (#428). Skipped when the source does not snapshot (e.g. Kafka).
	if caps.Snapshot {
		heldRows, err := markSnapshotsPending(b.ctx, b.snk, b.needsSnapshot)
		if err != nil {
			return err
		}
		b.heldRows = heldRows
	}
	return nil
}

// openReaderAndRelay opens the one replication connection (only when there are
// CDC tables), installs the source schemas and confirmed-point callback,
// computes the start position, starts the stream, and launches the relay. An
// all-incremental pipeline opens no replication connection at all.
func (b *setup) openReaderAndRelay() error {
	if len(b.cdcRefs) == 0 {
		return nil
	}
	rdr, err := b.src.Open(b.ctx, b.cdcRefs)
	if err != nil {
		return err
	}
	b.st.add(rdr.Close)
	b.rdr = rdr

	// A source that can take the source schema (optional interface) gets it
	// now so the source boundary gates on drift and encodes stable batches
	// with the source types the sink casts.
	if si, ok := rdr.(source.SchemaSetter); ok {
		byTarget := make(map[string]core.Schema, len(b.refs))
		for _, t := range b.s.Tables {
			if cs, ok := b.wire[t.Source]; ok {
				byTarget[t.Target] = cs
			}
		}
		si.SetSourceSchemas(byTarget)
	}
	b.r.rdr = rdr
	rdr.SetConfirmed(b.r.confirmedPosition)

	// start is the resume point, or an adopted table's explicit bootstrap
	// position when there is none (resolved by the shared resolver); a nil
	// start falls back to the source's initial position.
	start := b.start
	if start == nil {
		if m, err := b.src.InitialPosition(b.ctx); err != nil {
			return fmt.Errorf("runner: initial position: %w", err)
		} else {
			start = m
		}
	}
	if err := rdr.Start(b.ctx, start); err != nil {
		return fmt.Errorf("runner: start stream: %w", err)
	}

	// Pull: the reader is pull-based; the relay starts it and routes batches.
	b.router = newRelay(b.ingest, b.w, b.r.deliverFunc(b.src))
	b.routerDone = make(chan error, 1)
	b.relayDone = make(chan struct{})
	b.st.relayDone = b.relayDone
	go func() {
		defer close(b.relayDone)
		b.routerDone <- b.router.run(b.runCtx, rdr)
	}()
	return nil
}

// runSnapshotPhase runs the DBLog snapshot for tables with no committed
// position (or one an earlier run left unfinished), and the adopt path for a
// table whose bootstrap mode adopts existing data. A worker or relay failure
// must abort the snapshot: its blocking handshakes only observe a context, and
// newRunner has not yet handed workerErr/routerDone to Runner.run (#551).
func (b *setup) runSnapshotPhase() error {
	if !b.caps.Snapshot || (len(b.snapshotRefs) == 0 && len(b.adoptRefs) == 0) {
		return nil
	}
	sw := newSnapshotWatch(b.runCtx, b.workerErr, b.routerDone)
	b.st.add(sw.cancel)
	for _, ref := range b.snapshotRefs {
		if err := b.runSnapshotRef(sw.ctx, ref); err != nil {
			return err
		}
	}
	for _, ref := range b.adoptRefs {
		if err := b.runSnapshotRef(sw.ctx, ref); err != nil {
			return err
		}
	}
	// The snapshot phase finished: stop the failure watch and surface a
	// worker/relay death it caught (Runner.run would otherwise block on a
	// channel the watch already drained).
	return sw.stop()
}

// runSnapshotRef runs the boot snapshot or adopt for one table, taking the
// mode from the table's bootstrap block (snapshot when absent).
func (b *setup) runSnapshotRef(ctx context.Context, ref core.TableRef) error {
	bootstrapMode := spec.BootstrapSnapshot
	if bo, ok := b.bootstrapByTarget[ref.Target]; ok {
		bootstrapMode = bo.Mode
	}
	return b.r.runSnapshot(ctx, ref, bootstrapMode, b.qsrc, b.snk, b.rdr, b.router, b.w, b.cfg, b.heldRows, b.log)
}

// runIncrementalPhase pages each incremental table to exhaustion (#157/#572),
// through the same worker/ingest path as a snapshot. No slot, no window.
func (b *setup) runIncrementalPhase() error {
	if len(b.incrRefs) == 0 {
		return nil
	}
	return b.r.runIncremental(b.ctx, b.src, b.snk, b.incrRefs, b.specBySource, b.w, b.ingest)
}
