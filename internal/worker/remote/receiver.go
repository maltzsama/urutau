package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/protobuf/proto"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/faultinject"
	"github.com/maltzsama/urutau/internal/rowchange"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
)

// batchReceiver routes decoded Flight batches into the worker core, skipping
// any batch the Iceberg table already covers (resume idempotence, failure-
// analysis case 4): a batch whose high position is at or before the
// committed position was already applied by an earlier run.
type batchReceiver struct {
	ctx       context.Context
	w         *worker.Worker
	ingest    chan<- worker.Ingest
	committed map[string]position.Position // target table → committed
	parsePos  func(string) (position.Position, error)
	pkByTable map[string][]string // target table → primary key columns
	log       *slog.Logger
	// ack reports a batch as durably applied when the worker skips it as
	// already covered by its committed position. The coordinator still counts
	// such a batch as in-flight until it is acked; skipping without acking
	// leaks the in-flight forever and the supervisor terminates the run for a
	// "stalled" worker (issue #372).
	ack func(table, pos string)
}

// sendIngest delivers one change into the pipeline, aborting when the
// session ends. A bare send could block forever if the worker's internal
// pipeline has already died — the session would then hang in its teardown
// wait instead of exiting with the pipeline error.
func (r *batchReceiver) sendIngest(ing worker.Ingest) error {
	select {
	case r.ingest <- ing:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

// covered reports whether a positioned batch was already committed. Batches
// without a position (snapshot window rows) are never skipped.
func (r *batchReceiver) covered(meta *pb.BatchMeta) bool {
	cp, ok := r.committed[meta.Table]
	if !ok || meta.HighPos == "" {
		return false
	}
	high, err := r.parsePos(meta.HighPos)
	if err != nil {
		return false
	}
	// A batch is covered only when its high position is at or before the
	// committed point. An Incomparable comparison (opaque plugin offsets)
	// cannot decide — never skip, reprocess is safe.
	c := high.Compare(cp)
	return c != position.Incomparable && c <= 0
}

// skipCovered reports whether meta is at or before the table's committed
// position, and, when so, acks it so the coordinator clears the batch from its
// in-flight window. The coordinator counts a delivered batch as in-flight until
// acked; skipping without acking leaks that in-flight forever, the supervisor
// sees a "stalled" worker and terminates the run for a clean replay, which
// redelivers the same batch and crashloops (issue #372). Acking a covered batch
// is safe: it is at or before the position the worker has durably committed.
func (r *batchReceiver) skipCovered(meta *pb.BatchMeta) bool {
	if !r.covered(meta) {
		return false
	}
	r.log.Info("worker skip covered batch", "table", meta.Table, "high", meta.HighPos)
	if r.ack != nil {
		r.ack(meta.Table, meta.HighPos)
	}
	return true
}

// apply routes one Flight batch: the demux. It builds a *dataplane.Batch
// from the IPC record and routes by BatchMeta (four-readers rule: routing
// tags live in app_metadata, consumed here, never on the record). Snapshot
// window rows build windows; closes markers release them; live rows feed
// ingest as columnar batches. The transport no longer materializes rows.
func (r *batchReceiver) apply(fd *flight.FlightData) error {
	reader, err := ipc.NewReader(bytes.NewReader(fd.DataBody))
	if err != nil {
		return fmt.Errorf("worker: ipc reader: %w", err)
	}
	defer reader.Release()
	rec, err := reader.Read()
	if err != nil {
		return fmt.Errorf("worker: read flight record: %w", err)
	}
	if rec == nil {
		return errors.New("worker: empty flight batch")
	}
	rec.Retain() // the demux owns the record; the worker releases the Batch

	meta := &pb.BatchMeta{}
	if err := proto.Unmarshal(fd.AppMetadata, meta); err != nil {
		rec.Release()
		return fmt.Errorf("worker: unmarshal batch meta: %w", err)
	}

	if r.skipCovered(meta) {
		rec.Release()
		return nil
	}
	faultinject.At(faultinject.WorkerBatchReceived,
		"table", meta.Table, "seq", meta.BatchId, "position", meta.HighPos, "staged", meta.Staged)
	r.log.Debug("worker apply batch", "table", meta.Table, "high", meta.HighPos, "rows", rec.NumRows(), "staged", meta.Staged)

	b := &dataplane.Batch{
		Table:     meta.Table,
		Record:    rec,
		Watermark: []byte(meta.HighPos),
		// The coordinator's monotonic batch sequence (WK-001 C2): the sink
		// uses it to order N concurrent writers of a partitioned table.
		Seq: meta.BatchId,
		// The coordinator's per-batch commit-mode decision (WK-001 C5): stage
		// and ship the descriptor, or commit directly.
		Staged: meta.Staged,
	}

	switch {
	case meta.Window != nil && meta.Window.Snapshot:
		// AddWindowRows takes ownership of the batch (the window stores it).
		if err := r.w.AddWindowRows(meta.Table, meta.Window.WindowId, b); err != nil {
			return err
		}
	case meta.Window != nil && meta.Window.Closes:
		b.Release()
		ing := worker.Ingest{
			Table:           meta.Table,
			Win:             &rowchange.Window{Closes: true, WindowID: meta.Window.WindowId},
			Position:        meta.LowPos,
			SnapshotPending: meta.Window.SnapshotPending,
			MarkerID:        meta.BatchId,
		}
		// On a staged table the marker is a cycle of the coordinator's send
		// order, and the window's rows are delivered as that cycle.
		if meta.Staged {
			ing.Seq, ing.Staged = meta.BatchId, true
		}
		return r.sendIngest(ing)
	case meta.Window != nil && meta.Window.SnapshotDone:
		b.Release()
		ing := worker.Ingest{Table: meta.Table, Position: meta.LowPos, SnapshotDone: true}
		if meta.Staged {
			ing.Seq, ing.Staged = meta.BatchId, true
		}
		return r.sendIngest(ing)
	default:
		var win *rowchange.Window
		if meta.Window != nil && meta.Window.InWindow {
			win = &rowchange.Window{InWindow: true, WindowID: meta.Window.WindowId}
		}
		return r.sendIngest(worker.Ingest{Table: meta.Table, Batch: b, Win: win})
	}
	return nil
}
