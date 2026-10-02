package worker

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
)

// applyEnrich runs a batch through the pipeline's enricher. It owns the input
// batch: it always releases it, returning the enriched batch, or nil with
// dropped=true when the join dropped every row. In that case it first delivers
// an empty descriptor for the staged cycle — otherwise the cycle stays open and
// blocks the table's send-order drain.
func (p *tablePipeline) applyEnrich(ctx context.Context, batch *dataplane.Batch, origRows int, deliverEmpty func(*dataplane.Batch) error) (enriched *dataplane.Batch, dropped bool, err error) {
	enriched, err = p.enricher.EnrichBatch(ctx, batch, p.knownSchema.PrimaryKey)
	if err != nil {
		batch.Release()
		return nil, false, fmt.Errorf("worker: table %s: enrich: %w", p.target, err)
	}
	if enriched == nil {
		p.enrichDropped.Add(int64(origRows))
		if derr := deliverEmpty(batch); derr != nil {
			batch.Release()
			return nil, false, derr
		}
		batch.Release()
		return nil, true, nil
	}
	p.enrichDropped.Add(int64(origRows - int(enriched.Record.NumRows())))
	batch.Release()
	return enriched, false, nil
}
