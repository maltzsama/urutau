package worker

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/maltzsama/urutau/dataplane"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
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
func (w *Worker) AddWindowRows(target string, chunkID uint32, batch *dataplane.Batch) error {
	p, ok := w.tables[target]
	if !ok {
		batch.Release()
		return fmt.Errorf("worker: window rows for unregistered table %s", target)
	}
	p.winMu.Lock()
	defer p.winMu.Unlock()
	if _, dup := p.windows[chunkID]; dup {
		batch.Release()
		return fmt.Errorf("worker: window rows: duplicate chunk %d for %s", chunkID, target)
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
	p.windows[chunkID] = &snapshotWindow{batch: batch, keys: keys, touched: make(map[string]struct{})}
	return nil
}

// closeWindow returns the stored chunk batch minus the keys live InWindow
// events touched, with every row's __pos adopted to the marker position. The
// caller owns the returned batch (nil when the window is empty or fully
// touched) and buffers it through the cycle-aware path.
func closeWindow(p *tablePipeline, ing Ingest) (*dataplane.Batch, error) {
	p.winMu.Lock()
	win := p.windows[ing.Win.ChunkID]
	if win == nil {
		p.winMu.Unlock()
		return nil, nil
	}
	delete(p.windows, ing.Win.ChunkID)
	p.winMu.Unlock()

	if win.batch.Record == nil || win.batch.Record.NumRows() == 0 {
		win.batch.Release()
		return nil, nil
	}
	reader, err := transport.NewBatchReader(win.batch.Record, p.knownSchema.PrimaryKey)
	if err != nil {
		win.batch.Release()
		return nil, fmt.Errorf("worker: window %d: %w", ing.Win.ChunkID, err)
	}
	// Keep every row whose key was not touched by a live InWindow event.
	var keepIdx []int32
	for i := range reader.NumRows() {
		k := rowchange.KeyString(reader.Key(i))
		if _, hit := win.touched[k]; !hit {
			keepIdx = append(keepIdx, int32(i))
		}
	}
	if len(keepIdx) == 0 {
		win.batch.Release()
		return nil, nil
	}
	sel, err := dpint.SelectRows(win.batch, keepIdx, "", dataplane.AppendMode, "", nil)
	win.batch.Release() // the window is consumed; selectRows retained its columns
	if err != nil {
		return nil, err
	}
	out, err := adoptWindowPos(sel, ing.Position)
	sel.Release()
	if err != nil {
		return nil, err
	}
	return out, nil
}

// adoptWindowPos rebuilds a batch with __pos replaced by a constant — the
// Closes rows adopt the marker's position (the safe resume point).
func adoptWindowPos(b *dataplane.Batch, pos string) (*dataplane.Batch, error) {
	rec := b.Record
	posIdx := -1
	for i := range rec.Schema().NumFields() {
		if rec.Schema().Field(i).Name == "__pos" {
			posIdx = i
			break
		}
	}
	if posIdx < 0 {
		return nil, fmt.Errorf("worker: window batch has no __pos column")
	}
	bld := array.NewStringBuilder(memory.DefaultAllocator)
	defer bld.Release()
	for range int(rec.NumRows()) {
		bld.Append(pos)
	}
	posArr := bld.NewStringArray()
	defer posArr.Release()

	cols := make([]arrow.Array, rec.NumCols())
	for i := range int(rec.NumCols()) {
		if i == posIdx {
			posArr.Retain()
			cols[i] = posArr
		} else {
			rec.Column(i).Retain()
			cols[i] = rec.Column(i)
		}
	}
	newRec := array.NewRecordBatch(rec.Schema(), cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return &dataplane.Batch{Table: b.Table, Record: newRec, Watermark: []byte(pos), Mode: dataplane.AppendMode, Seq: b.Seq, Staged: b.Staged}, nil
}

// markBatchSideEffects applies the per-row, side-effect-only decisions for a
// live batch: bootstrap-guard marking (a live key during snapshot takes the
// upsert path) and InWindow dedup (a live key removes its snapshot row from
// every open window). Pure reads of the record; the batch itself is not
// modified.
func markBatchSideEffects(p *tablePipeline, batch *dataplane.Batch, ing Ingest) error {
	reader, err := transport.NewBatchReader(batch.Record, p.knownSchema.PrimaryKey)
	if err != nil {
		return fmt.Errorf("worker: table %s: %w", p.target, err)
	}
	inWindow := ing.Win != nil && ing.Win.InWindow
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

// partitionSnapshotRows splits a merged batch's row indices into untouched
// snapshot PKs (pure append) and the rest.
func partitionSnapshotRows(b *dataplane.Batch, guard *bloom.BloomFilter, pk []string) (untouched, rest []int32, err error) {
	reader, err := transport.NewBatchReader(b.Record, pk)
	if err != nil {
		return nil, nil, err
	}
	for i := range reader.NumRows() {
		if reader.Snapshot(i) {
			key := reader.Key(i)
			if len(key) > 0 && !guard.TestString(rowchange.KeyString(key)) {
				untouched = append(untouched, int32(i))
				continue
			}
		}
		rest = append(rest, int32(i))
	}
	return untouched, rest, nil
}

// appendRowsToKeep returns the row indices an append-mode flush keeps:
// every non-delete row, plus delete rows that carry a real before image (a
// non-PK data column set). Dropped deletes are counted and reported.
func appendRowsToKeep(ctx context.Context, p *tablePipeline, w *Worker, b *dataplane.Batch) ([]int32, error) {
	reader, err := transport.NewBatchReader(b.Record, p.knownSchema.PrimaryKey)
	if err != nil {
		return nil, fmt.Errorf("worker: table %s: %w", p.target, err)
	}
	nonPK := make([]string, 0, len(reader.DataColumns()))
	for _, name := range reader.DataColumns() {
		isPK := false
		for _, pkn := range p.knownSchema.PrimaryKey {
			if pkn == name {
				isPK = true
				break
			}
		}
		if !isPK {
			nonPK = append(nonPK, name)
		}
	}
	var keep []int32
	for i := range reader.NumRows() {
		if reader.Op(i) != rowchange.OpDelete {
			keep = append(keep, int32(i))
			continue
		}
		// A delete: drop on skip; else keep only if it carries an image.
		if p.appendDropDeletes || !rowHasImage(reader, nonPK, i) {
			p.droppedDeletes.Add(1)
			if w.metrics != nil {
				w.metrics.DeletesDropped.WithLabelValues(p.target).Inc()
			}
			if w.onDroppedDelete != nil {
				w.onDroppedDelete(p.target, reader.Position(i))
			}
			continue
		}
		keep = append(keep, int32(i))
	}
	return keep, nil
}

// rowHasImage reports whether the row carries a value in a non-PK column —
// a delete with an image is appendable; a key-only tombstone is not. It is
// only called for delete rows and returns on the first non-null column.
func rowHasImage(r *transport.BatchReader, nonPK []string, i int) bool {
	for _, name := range nonPK {
		// IsNull is O(1) and allocates nothing; Value decodes the cell.
		if null, ok := r.IsNull(name, i); ok && !null {
			return true
		}
	}
	return false
}

// markerBatch is a 0-row batch in the table's wire schema that carries a
// Closes marker's cycle, for a window that emitted no rows: the cycle still
// owes the coordinator a delivery.
func markerBatch(p *tablePipeline, ing Ingest) (*dataplane.Batch, error) {
	schema, err := transport.CoreSchemaToArrow(p.knownSchema)
	if err != nil {
		return nil, fmt.Errorf("worker: table %s: marker batch: %w", p.target, err)
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	rec := bld.NewRecordBatch()
	bld.Release()
	return &dataplane.Batch{Table: p.target, Record: rec, Watermark: []byte(ing.Position), Mode: p.mode, Seq: ing.Seq, Staged: ing.Staged}, nil
}

// snapshotDoneBatch is the 0-row batch that commits a table's snapshot
// completion: cdc.snapshot.state=complete and no position. The marker's
// position is the coordinator's latest sent when it queued the marker; the
// stream may have committed past it since, and the completion must not move
// the table's position back. The committer acks the marker's position.
func snapshotDoneBatch(p *tablePipeline, ing Ingest) (*dataplane.Batch, error) {
	b, err := markerBatch(p, ing)
	if err != nil {
		return nil, err
	}
	b.Watermark = nil
	b.SnapshotState = string(snapshot.StateComplete)
	return b, nil
}
