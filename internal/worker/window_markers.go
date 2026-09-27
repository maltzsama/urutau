package worker

import (
	"fmt"

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
	return &dataplane.Batch{Table: p.target, Record: rec, Watermark: []byte(ing.Position), Mode: p.mode, Seq: ing.Seq, Staged: ing.Staged}, nil
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
