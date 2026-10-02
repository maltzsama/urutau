package coordinator

import "github.com/maltzsama/urutau/internal/transport"

// batchPosition returns the source position of a batch's last row — the
// position that covers every row it carries, whichever key range each row is
// routed to. The coordinator records it as a staged cycle's committed
// position. It must read the batch's own reader, not a partition sub-batch's:
// sub-batches are ordered by key range while positions follow source order, so
// the last sub-batch need not hold the batch's last position (issue #492).
// It is "" for an empty batch.
//
// A maximum over the per-partition highs is NOT equivalent in general: a source
// whose positions are opaque (incomparable across partitions, e.g. a plugin
// adapter) has no ordering to take a maximum over, and picking one partition's
// high would not cover the others' rows. The batch's last row is the
// source-provided position that does.
func batchPosition(r *transport.BatchReader) string {
	if r.NumRows() == 0 {
		return ""
	}
	return r.Position(r.NumRows() - 1)
}
