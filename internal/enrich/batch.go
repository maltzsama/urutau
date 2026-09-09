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
// primaryKey carries the table's PK (from the Assignment). The wire record
// does not carry it, and the re-encode bridge REJECTS deletes on a schema
// without PK (C-8 fail-fast) — without it, any enriched batch containing a
// delete errors out.
//
// The join itself is row-based BY DESIGN until the columnar broadcast join
// lands (CR-069 §3.4): the batch is decoded to rows, joined, and re-encoded.
// The worker consumes the seam's output columnar; the extra round-trip is
// the deliberate price until CR-069 replaces the join kernel. Not dead code
// and not transitional-pending-M4 — M4 passed with the worker columnar and
// this seam remained, so the deferral is to the columnar JOIN, not to the
// columnar worker.
//
// OWNERSHIP: the input batch is NOT released; the caller owns it. Returns
// nil when inner joins dropped every row.
func (s *Stage) EnrichBatch(b *dataplane.Batch, primaryKey []string) (*dataplane.Batch, error) {
	if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		return b, nil
	}
	rows, err := transport.DecodeBatch(b.Record, b.Table, primaryKey)
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
	// The wire record carries columns only; PK rides from the Assignment
	// so delete-key projection and the bridge C-8 validation hold.
	cs.PrimaryKey = primaryKey
	cb := rowchange.Batch{
		Table:           b.Table,
		Changes:         enriched,
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
