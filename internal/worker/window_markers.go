package worker

import (
	"fmt"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
)

// markerBatch is a 0-row batch in the table's wire schema that carries a
// Closes marker's cycle, for a window that emitted no rows: the cycle still
// owes the coordinator a delivery.
func markerBatch(p *tablePipeline, ing Ingest) (*dataplane.Batch, error) {
	schema, err := transport.CoreSchemaToArrow(p.knownSchema)
	if err != nil {
		return nil, fmt.Errorf("worker: table %s: marker batch: %w", p.target, err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	rec := bld.NewRecordBatch()
	bld.Release()
	b := &dataplane.Batch{Table: p.target, Record: rec, Watermark: []byte(ing.Position), Mode: p.mode, Seq: ing.Seq, Staged: ing.Staged}
	carrySnapshotPending(b, ing)
	return b, nil
}

// snapshotDoneBatch is the 0-row batch that commits a table's snapshot
// completion: cdc.snapshot.state=complete and no position. The marker's
// position is the coordinator's latest sent when it queued the marker; the
// stream may have committed past it since, and the completion must not move
// the table's position back. The committer acks the marker's position.
func snapshotDoneBatch(p *tablePipeline, ing Ingest) (*dataplane.Batch, error) {
	b, err := markerBatch(p, ing)
	if err != nil {
		return nil, err
	}
	b.Watermark = nil
	b.SnapshotState = string(snapshot.StateComplete)
	return b, nil
}

// closesBatch handles a Closes marker: it returns the window's remaining
// rows for the caller to buffer, or nil when there are none.
//
// A marker with a seq is a cycle of the coordinator's send order: the
// window's rows go out as that cycle, so they commit after every live cycle
// released ahead of the marker (#416). A window that emits no rows still
// owes the cycle its delivery, made here through deliverEmpty. A window this
// process never held — a lost one, its rows gone with the previous process —
// owes that delivery too, but has done no chunk: its snapshot progress must
// not commit, or a restarted coordinator resumes past the chunk's rows.
func closesBatch(p *tablePipeline, ing Ingest, deliverEmpty func(*dataplane.Batch) error) (*dataplane.Batch, error) {
	cb, held, err := closeWindow(p, ing)
	if err != nil {
		return nil, err
	}
	if !held {
		ing.SnapshotPending = nil
	}
	var rows int64
	if cb != nil {
		rows = cb.Record.NumRows()
	}
	// One line per window closed: which chunk, whether this worker held it,
	// how many rows it emits and under which cycle — what shows a chunk
	// recorded done with none of its rows.
	slog.Debug("worker: window closed", "table", p.target, "chunk", ing.Win.ChunkID,
		"held", held, "rows", rows, "seq", ing.Seq, "position", ing.Position, "pending", len(ing.SnapshotPending))
	if ing.Seq == 0 {
		return cb, nil
	}
	if cb != nil {
		cb.Seq, cb.Staged = ing.Seq, ing.Staged
		return cb, nil
	}
	eb, err := markerBatch(p, ing)
	if err != nil {
		return nil, err
	}
	err = deliverEmpty(eb)
	eb.Release()
	return nil, err
}
