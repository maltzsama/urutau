// Package dataplane — the row-to-wire encoder at the CDC decoder boundary.
//
// BatchFromChangeBatch converts a rowchange.Batch (the CDC decoders' output
// — binlog/JSON events are row-shaped) into a columnar wire batch via
// transport.RecordFromChanges (typed, tested). The worker is fully columnar
// since commit a6cd459 (it no longer calls this); the encoder survives here
// because the row universe ends at the source decoders, not in the worker.
//
// Callers encode against the introspected schema when it is reachable (the
// snapshot builders use the worker's known schema; the enrich seam carries
// its own). transport.MergeSchema is the FALLBACK for schema-less row
// producers (the live CDC puller), which upstream owns the resolved schema
// for — sources gain it when the reader contract carries it.
//
// SCHEDULED FOR REMOVAL (BRIEF-ARROW Onda 1, S8): every caller is being
// migrated to build the RecordBatch directly. This file goes when the last
// caller does.
package dataplane

import (
	"fmt"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// BatchFromChangeBatch converts a rowchange.Batch into a columnar
// dataplane.Batch. The RecordBatch carries the wire schema (data + __op +
// __pos + __commit_ts + __ingest_ts + __snapshot). The caller owns the
// returned Batch.
//
// cs is the canonical schema to encode against; an empty cs falls back to
// per-batch inference (transport.MergeSchema) for schema-less row producers.
func BatchFromChangeBatch(b rowchange.Batch, cs core.Schema) (*Batch, error) {
	// D-6: the batch's single arrival-ordered slice IS the wire order.
	all := b.Changes
	if len(all) == 0 {
		return &Batch{Table: b.Table, Watermark: []byte(b.Position), Mode: rowchange.ToDataplaneMode(b.Mode)}, nil
	}

	// Schema: known schema, plus any columns the changes carry that the
	// known schema lacks (enriched columns, e.g. join output). Enrichment
	// adds columns after registration, so the known schema alone would drop
	// them on the wire. When cs is empty (cold path — first bridge call
	// before schema registration), the schema is inferred from row values.
	cs = transport.MergeSchema(all, cs)

	// C-8 fail-fast: deletes without a PK produce orphaned NULL tuples in the
	// sink. The enrich path cannot reconstitute a key from After (it may be
	// nil or empty). When PK is empty the pump must not send deletes.
	if len(cs.PrimaryKey) == 0 {
		nDeletes := 0
		for _, c := range all {
			if c.Op == rowchange.OpDelete {
				nDeletes++
			}
		}
		if nDeletes > 0 {
			return nil, fmt.Errorf("dataplane: batch %q has %d delete(s) but schema has no PrimaryKey — deletes would become orphaned NULLs in the sink", b.Table, nDeletes)
		}
	}

	rec, err := transport.RecordFromChanges(all, cs, nil)
	if err != nil {
		return nil, fmt.Errorf("bridge: encode: %w", err)
	}

	return &Batch{
		Table:           b.Table,
		Record:          rec,
		Watermark:       []byte(b.Position),
		Mode:            rowchange.ToDataplaneMode(b.Mode),
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
	}, nil
}
