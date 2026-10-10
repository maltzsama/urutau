package runner

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/eventlog"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// snapshotWatch cancels the snapshot phase's context when the worker or the
// relay dies, so the snapshot's blocking handshakes (Relay.Release/GateFlush)
// unblock with ctx.Err() instead of wedging boot with no error (issue #551).
// It consumes workerErr/routerDone ONLY while the snapshot runs: on a clean
// snapshot the caller stops it, leaving the channels for Runner.run.
type snapshotWatch struct {
	ctx    context.Context
	cancel context.CancelFunc
	result chan error
}

func newSnapshotWatch(ctx context.Context, workerErr, routerDone <-chan error) *snapshotWatch {
	snapCtx, cancel := context.WithCancel(ctx)
	w := &snapshotWatch{ctx: snapCtx, cancel: cancel, result: make(chan error, 1)}
	go func() {
		var err error
		select {
		case e := <-workerErr:
			err = fmt.Errorf("runner: worker: %w", e)
		case e := <-routerDone:
			err = fmt.Errorf("runner: router: %w", e)
		case <-snapCtx.Done():
		}
		cancel()
		w.result <- err
	}()
	return w
}

// stop cancels the watch and returns the failure it observed, if any.
func (w *snapshotWatch) stop() error {
	w.cancel()
	return <-w.result
}

// runSnapshot runs the boot snapshot (or adopt, which reads nothing) for one
// table. On failure it returns the error and lets the caller's cleanup path
// release the pipeline resources; every wait inside it — the chunk scan,
// GateFlush, Release — observes ctx, which newSnapshotWatch cancels if the
// worker or relay dies.
func (r *Runner) runSnapshot(
	ctx context.Context,
	ref core.TableRef,
	bootstrapMode spec.BootstrapMode,
	qsrc source.QuerySource,
	snk sink.Sink,
	rdr source.Reader,
	router *relay,
	w *worker.Worker,
	cfg Config,
	heldRows map[string]bool,
	log *slog.Logger,
) error {
	switch bootstrapMode {
	case spec.Adopt, spec.AdoptVerify:
		// Adopt: mark snapshot complete without reading data.
		log.Info("adopt", "table", ref.Source, "mode", bootstrapMode)
		r.emit(eventlog.KindSnapshotStarted, map[string]any{
			"table": ref.Source, "target": ref.Target, "mode": bootstrapMode,
		})
		// Write complete state to Iceberg properties.
		if err := snapshot.MarkComplete(ctx, snk, ref); err != nil {
			return fmt.Errorf("runner: adopt %s: %w", ref.Target, err)
		}
		w.SetSnapshotState(ref.Target, string(snapshot.StateComplete), nil)
		log.Info("adopt done", "table", ref.Source)
		r.emit(eventlog.KindSnapshotDone, map[string]any{
			"table": ref.Source, "target": ref.Target, "mode": bootstrapMode,
		})
	default:
		// Snapshot: load all data from source.
		log.Info("snapshot", "table", ref.Source)
		r.emit(eventlog.KindSnapshotStarted, map[string]any{"table": ref.Source, "target": ref.Target})
		chunker, err := qsrc.NewChunker(ref.Source, strings.Join(ref.PrimaryKey, ","), cfg.ChunkSize)
		if err != nil {
			return err
		}
		// Read existing snapshot progress for resumable backfill.
		progress, err := snapshot.ReadProgress(ctx, snk, ref)
		if err != nil {
			return fmt.Errorf("runner: snapshot progress %s: %w", ref.Target, err)
		}
		if progress.State == snapshot.StateInProgress {
			log.Info("snapshot resuming", "table", ref.Source,
				"pending", snapshot.PendingIDs(progress.Pending))
		}
		// Set initial snapshot state on the worker so batches carry it.
		w.SetSnapshotState(ref.Target, string(snapshot.StateInProgress), progress.Pending)
		if progress.State == snapshot.StateInProgress || heldRows[ref.Target] {
			// The bloom guard was recreated empty: keys live events touched
			// before the crash are unknown, so pure appends could duplicate
			// committed rows. Every snapshot row goes through the upsert path
			// on a resumed snapshot — and on one an earlier run left
			// not_started after committing stream rows to the table (#428).
			w.MarkSnapshotResumed(ref.Target)
		}
		if err := snapshot.SnapshotTable(ctx, chunker, rdr, router, ref.Target, snapshot.SnapshotConfig{
			WindowTimeout: cfg.WindowTimeout,
			CaughtUpPoll:  cfg.CaughtUpPoll,
			Progress:      progress,
			Schema:        w.KnownSchema(ref.Target),
			ChunkSize:     cfg.ChunkSize,
			Persist: func(sp snapshot.SnapshotProgress) error {
				props, err := snapshot.EncodeSnapshotProgress(&sp)
				if err != nil {
					return err
				}
				return snk.SetProperties(ctx, ref, props)
			},
		}, func(table string, completedChunkID uint32, remaining []uint32) {
			w.SetSnapshotState(ref.Target, string(snapshot.StateInProgress), remaining)
		}); err != nil {
			return fmt.Errorf("runner: snapshot %s: %w", ref.Source, err)
		}
		// Snapshot complete: mark on the worker.
		w.SetSnapshotState(ref.Target, string(snapshot.StateComplete), nil)
		log.Info("snapshot done", "table", ref.Source)
		r.emit(eventlog.KindSnapshotDone, map[string]any{"table": ref.Source, "target": ref.Target})
	}
	return nil
}
