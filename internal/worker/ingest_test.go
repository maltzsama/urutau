package worker

// Test-only row->batch feed, mirroring the former production helper of the
// same name (removed in G2: sources produce batches natively, so the worker
// no longer owns a row batcher). The worker is granularity-insensitive
// (see granularity_test.go), so feeding per-change batches is a faithful
// test surface for the pre-M4 row producers.

import (
	"context"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// IngestFromChanges wraps a change-oriented channel into Ingest batches.
func IngestFromChanges(ctx context.Context, changes <-chan rowchange.Change, schema core.Schema) <-chan Ingest {
	out := make(chan Ingest, 64)
	go func() {
		defer close(out)
		bufs := map[string][]rowchange.Change{}
		flush := func() {
			for table, buf := range bufs {
				if len(buf) == 0 {
					continue
				}
				cb := rowchange.Batch{Table: table, Changes: buf, Mode: rowchange.ToRowMode(dataplane.UpsertMode)}
				dpb, err := dpint.BatchFromChangeBatch(cb, schema)
				if err != nil {
					bufs[table] = bufs[table][:0]
					continue
				}
				select {
				case out <- Ingest{Table: table, Batch: dpb}:
				case <-ctx.Done():
					return
				}
				bufs[table] = bufs[table][:0]
			}
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case c, ok := <-changes:
				if !ok {
					flush()
					return
				}
				if c.Window != nil {
					flush()
					if c.Window.Closes {
						select {
						case out <- Ingest{Table: c.Table, Win: c.Window, Position: c.Position}:
						case <-ctx.Done():
							return
						}
						continue
					}
					cb := rowchange.Batch{Table: c.Table, Changes: []rowchange.Change{c}, Mode: rowchange.ToRowMode(dataplane.UpsertMode)}
					dpb, err := dpint.BatchFromChangeBatch(cb, schema)
					if err != nil {
						continue
					}
					select {
					case out <- Ingest{Table: c.Table, Batch: dpb, Win: c.Window}:
					case <-ctx.Done():
						return
					}
					continue
				}
				bufs[c.Table] = append(bufs[c.Table], c)
				if len(bufs[c.Table]) >= 100 {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
