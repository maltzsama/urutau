package worker

import (
	"fmt"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// snapshotWindow is one DBLog chunk's SELECT rows, stored as the batch the
// snapshot source produced (no row decode). Live InWindow events mark keys
// in touched; Closes emits the batch minus the touched rows.
type snapshotWindow struct {
	batch *dataplane.Batch
	// keys is the set of PK key-strings the window holds, computed once when
	// the chunk is stored. markBatchSideEffects tests membership here instead
	// of re-scanning the batch per live row (issue #266).
	keys    map[string]struct{}
	touched map[string]struct{}
}

// AddWindowRows stores one chunk's SELECT batch for the snapshot window.
// The window TAKES OWNERSHIP of the batch; the Closes handler releases it.
func (w *Worker) AddWindowRows(target string, windowID uint64, batch *dataplane.Batch) error {
	p, ok := w.tables[target]
	if !ok {
		batch.Release()
		return fmt.Errorf("worker: window rows for unregistered table %s", target)
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	if _, dup := p.windows[windowID]; dup {
		batch.Release()
		return fmt.Errorf("worker: window rows: duplicate window %d for %s", windowID, target)
	}
	// Precompute the window's key set once: the live path tests membership per
	// row, and re-scanning the whole chunk per row was O(rows × windows ×
	// window-rows) during the snapshot (issue #266).
	keys := make(map[string]struct{}, batch.Record.NumRows())
	if r, err := transport.NewBatchReader(batch.Record, p.knownSchema.PrimaryKey); err == nil {
		for i := range r.NumRows() {
			keys[rowchange.KeyString(r.Key(i))] = struct{}{}
		}
	}
	p.windows[windowID] = &snapshotWindow{batch: batch, keys: keys, touched: make(map[string]struct{})}
	return nil
}

// signalWindowClosed wakes the chunk reader's backpressure wait; the send is
// non-blocking so a reader not yet waiting never blocks the ingest loop.
func signalWindowClosed(p *tablePipeline) {
	select {
	case p.winClosed <- struct{}{}:
	default:
	}
}

// openWindows returns the number of DBLog windows the target table currently
// holds open (populated, not yet Closes'd). The chunk reader's backpressure
// caps this so a worker never holds a whole chunk of byte-capped windows at
// once (issue #622).
func (w *Worker) openWindows(target string) int {
	p := w.tables[target]
	if p == nil {
		return 0
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	return len(p.windows)
}

// windowClosed returns the channel closed (signaled) when a window of target
// closes; nil when the table is unknown.
func (w *Worker) windowClosed(target string) <-chan struct{} {
	p := w.tables[target]
	if p == nil {
		return nil
	}
	return p.winClosed
}
