package enrich

import (
	"context"

	"github.com/maltzsama/urutau/dataplane"
)

// EnrichBatch applies the reference joins to a whole columnar batch. The
// seam is columnar end to end: Arrow in, Arrow out, no rowchange, no
// map[string]any (CR-069 / BRIEF-PERF v10).
//
// primaryKey is accepted for call-site compatibility; ColumnarJoin does not
// need it (the wire record carries the key columns and deletes bypass the
// join without a key lookup).
//
// OWNERSHIP: the input batch is NOT released; the caller owns it. Returns
// nil when the join dropped every row.
func (s *Stage) EnrichBatch(ctx context.Context, b *dataplane.Batch, _ []string) (*dataplane.Batch, error) {
	return s.ColumnarJoin(ctx, b)
}
