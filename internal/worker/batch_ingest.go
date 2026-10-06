package worker

import (
	"context"
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
)

// checkSchemaDrift runs the columnar drift check on a source batch before
// enrich: any data column the batch carries that the known schema lacks is a
// spec violation — report once and go terminal. It releases b and returns an
// error when the batch is terminal.
func (w *Worker) checkSchemaDrift(p *tablePipeline, b *dataplane.Batch) error {
	if len(p.knownSchema.Columns) == 0 {
		return nil
	}
	d, hit, err := schemaDrift(b, p.knownSchema)
	if err != nil {
		b.Release()
		return fmt.Errorf("worker: table %s: %w", p.target, err)
	}
	if !hit {
		return nil
	}
	p.snapshotMu.Lock()
	first := !p.driftReported[d.Column]
	p.driftReported[d.Column] = true
	p.snapshotMu.Unlock()
	if first && w.schemaDrift != nil {
		w.schemaDrift(SchemaDrift{Table: p.target, Column: d.Column, Kind: d.Kind})
	}
	b.Release()
	return fmt.Errorf("worker: table %s: schema drift: column %q is not in the spec — declare it and resume", p.target, d.Column)
}

// enrichStreamBatch runs a live batch through the enricher. It owns batch;
// dropped=true means the join dropped every row (the caller skips it).
func (p *tablePipeline) enrichStreamBatch(ctx context.Context, batch *dataplane.Batch, bufferEmpty func(*dataplane.Batch) error) (*dataplane.Batch, bool, error) {
	if p.enricher == nil {
		return batch, false, nil
	}
	enriched, dropped, err := p.applyEnrich(ctx, batch, int(batch.Record.NumRows()), bufferEmpty)
	if err != nil {
		return nil, false, err
	}
	if dropped {
		return nil, true, nil
	}
	return enriched, false, nil
}
