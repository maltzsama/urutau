package enrich

import (
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// EnrichBatch applies the reference joins to a whole columnar batch. The
// seam is columnar: it takes *dataplane.Batch and returns *dataplane.Batch
// with the reference columns appended.
//
// QUARANTINE: the join itself is still row-based — the batch is decoded to
// rows, joined, and re-encoded. This establishes the columnar seam so the
// worker can consume batches; the join becomes truly columnar (CR-069 §3.4)
// when the rowchange.Batch bridge dies. The extra round-trip is the price of
// the seam during the transition.
//
// OWNERSHIP: the input batch is NOT released; the caller owns it. Returns
// nil when inner joins dropped every row.
func (s *Stage) EnrichBatch(b *dataplane.Batch) (*dataplane.Batch, error) {
	if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		return b, nil
	}
	rows, err := transport.DecodeBatch(b.Record, b.Table, nil)
	if err != nil {
		return nil, fmt.Errorf("enrich: decode: %w", err)
	}
	enriched, err := s.Enrich(rows)
	if err != nil {
		return nil, err
	}
	if len(enriched) == 0 {
		return nil, nil // inner joins dropped everything
	}
	// Pass the input batch's schema so mergeSchema picks up the enriched
	// columns. An empty schema caused per-batch inference drift: different
	// batches inferred different types for the same column (audit #6).
	cs, err := transport.SchemaFromArrow(b.Record.Schema())
	if err != nil {
		return nil, fmt.Errorf("enrich: input schema: %w", err)
	}
	cb := rowchange.Batch{
		Table:           b.Table,
		Upserts:         enriched,
		Mode:            rowchange.ToRowMode(b.Mode),
		Position:        string(b.Watermark),
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
	}
	dpb, err := dpint.BatchFromChangeBatch(cb, cs)
	if err != nil {
		return nil, fmt.Errorf("enrich: re-encode: %w", err)
	}
	// Carry the batch's watermark and snapshot state through: the sink
	// needs them to persist position atomically (audit #6).
	dpb.Watermark = b.Watermark
	dpb.SnapshotState = b.SnapshotState
	dpb.SnapshotPending = b.SnapshotPending
	return dpb, nil
}
