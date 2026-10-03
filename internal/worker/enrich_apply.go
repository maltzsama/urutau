package worker

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
)

// applyEnrich runs a batch through the pipeline's enricher. It owns the input
// batch: it releases it, returning the enriched batch, or nil with
// dropped=true when the join dropped every row. In that case it first calls
// bufferEmpty to queue an empty batch for the staged cycle — through the normal
// pending/flush path, so the empty delivery keeps its place in the cycle's send
// order rather than jumping ahead of buffered data.
func (p *tablePipeline) applyEnrich(ctx context.Context, batch *dataplane.Batch, origRows int, bufferEmpty func(*dataplane.Batch) error) (enriched *dataplane.Batch, dropped bool, err error) {
	enriched, err = p.enricher.EnrichBatch(ctx, batch, p.knownSchema.PrimaryKey)
	if err != nil {
		batch.Release()
		return nil, false, fmt.Errorf("worker: table %s: enrich: %w", p.target, err)
	}
	if enriched == nil {
		p.enrichDropped.Add(int64(origRows))
		// A staged cycle needs a delivery; buffer an empty batch through the
		// normal flush so it keeps its Seq order. A non-staged batch (Seq 0 or
		// direct-commit mode) needs none.
		if p.stage(batch) && batch.Seq != 0 {
			if berr := bufferEmpty(batch); berr != nil {
				batch.Release()
				return nil, false, berr
			}
		}
		batch.Release()
		return nil, true, nil
	}
	p.enrichDropped.Add(int64(origRows - int(enriched.Record.NumRows())))
	batch.Release()
	return enriched, false, nil
}
