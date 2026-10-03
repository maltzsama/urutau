package worker

import (
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
	"github.com/maltzsama/urutau/internal/transport"
)

// markBatchSideEffects applies the per-row, side-effect-only decisions for a
// live batch: bootstrap-guard marking (a live key during snapshot takes the
// upsert path) and InWindow dedup (a live key removes its snapshot row from
// every open window). Pure reads of the record; the batch itself is not
// modified.
func markBatchSideEffects(p *tablePipeline, batch *dataplane.Batch, ing Ingest) error {
	// Build the reader first so a malformed record still surfaces its wrapped
	// error, exactly as before the early return.
	reader, err := transport.NewBatchReader(batch.Record, p.knownSchema.PrimaryKey)
	if err != nil {
		return fmt.Errorf("worker: table %s: %w", p.target, err)
	}
	// Steady state — snapshot done, no window open — has no side effect to
	// apply; read the state once so the common case skips the per-row key
	// allocation, KeyString and two mutex operations entirely (#577).
	inWindow := ing.Win != nil && ing.Win.InWindow
	p.snapshotMu.Lock()
	marking := p.snapshotState == string(snapshot.StateInProgress) && p.bootstrapGuard != nil
	p.snapshotMu.Unlock()
	if !marking && !inWindow {
		return nil
	}

	for i := range reader.NumRows() {
		key := reader.Key(i)
		if len(key) == 0 {
			continue
		}
		k := rowchange.KeyString(key)
		if !reader.Snapshot(i) {
			p.snapshotMu.Lock()
			if p.snapshotState == string(snapshot.StateInProgress) && p.bootstrapGuard != nil {
				p.bootstrapGuard.AddString(k)
			}
			p.snapshotMu.Unlock()
		}
		if inWindow {
			p.winMu.Lock()
			for _, win := range p.windows {
				if _, hit := win.touched[k]; hit {
					continue
				}
				// Only touch a key the window actually holds.
				if _, held := win.keys[k]; held {
					win.touched[k] = struct{}{}
					p.dropped++
				}
			}
			p.winMu.Unlock()
		}
	}
	return nil
}
