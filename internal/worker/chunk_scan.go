package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// chunkPartBytes bounds the scanned rows a chunk read holds as row maps
// before encoding them to Arrow. The whole chunk used to sit in memory as
// maps while Arrow builders grew by doubling over all of it: a 65 MB chunk of
// the full profile's events swung the worker's heap to ~0.9 GB.
const chunkPartBytes = 4 << 20

// scanChunkRecord reads one chunk into a single record, encoding the rows in
// parts of about chunkPartBytes as they arrive, and returns the record and
// its row count. The record is built against the introspected schema (the
// worker's known schema for this target), never a per-batch inference:
// window rows must carry the stable table shape the sink expects. MergeSchema
// keeps that shape and only appends columns a row carries that the schema
// lacks. Snapshot chunk rows are inserts only, so the bridge C-8 delete guard
// does not apply here.
func scanChunkRecord(ctx context.Context, chunker source.ChunkSource, ch source.Chunk, ta *pb.TableAssignment, known core.Schema) (arrow.RecordBatch, int, error) {
	var (
		parts   []arrow.RecordBatch
		schema  *core.Schema
		pending []rowchange.Change
		bytes   int
		total   int
	)
	release := func() {
		for _, p := range parts {
			p.Release()
		}
	}
	encode := func() error {
		if len(pending) == 0 {
			return nil
		}
		s := transport.MergeSchema(pending, known)
		if schema == nil {
			schema = &s
		} else if len(s.Columns) != len(schema.Columns) {
			return fmt.Errorf("encode: the chunk's columns changed mid-scan (%d then %d)", len(schema.Columns), len(s.Columns))
		}
		rec, err := transport.RecordFromChanges(pending, *schema, nil)
		if err != nil {
			return fmt.Errorf("encode: %w", err)
		}
		parts = append(parts, rec)
		pending, bytes = pending[:0], 0
		return nil
	}
	err := chunker.Scan(ctx, ch, func(row map[string]any) error {
		key := make([]any, 0, len(ta.PrimaryKey))
		for _, col := range ta.PrimaryKey {
			key = append(key, row[col])
		}
		pending = append(pending, rowchange.Change{
			Op:       rowchange.OpInsert,
			Table:    ta.TargetTable,
			Key:      key,
			After:    row,
			Snapshot: true,
			Phase:    core.PhaseSnapshot,
			IngestTS: time.Now(),
		})
		total++
		if bytes += rowBytes(row); bytes >= chunkPartBytes {
			return encode()
		}
		return nil
	})
	if err == nil {
		err = encode()
	}
	if err != nil {
		release()
		return nil, 0, err
	}
	if len(parts) == 0 {
		// An empty chunk still carries the table's shape.
		rec, err := transport.RecordFromChanges(nil, known, nil)
		return rec, 0, err
	}
	if len(parts) == 1 {
		return parts[0], total, nil
	}
	rec, err := concatRecords(parts)
	release()
	if err != nil {
		return nil, 0, fmt.Errorf("encode: %w", err)
	}
	return rec, total, nil
}

// rowBytes estimates a scanned row's size: its strings' and byte slices'
// lengths, eight bytes for any other cell.
func rowBytes(row map[string]any) int {
	n := 0
	for _, v := range row {
		switch t := v.(type) {
		case string:
			n += len(t)
		case []byte:
			n += len(t)
		default:
			n += 8
		}
	}
	return n
}

// concatRecords concatenates same-schema records, in order, into one. The
// inputs stay the caller's.
func concatRecords(parts []arrow.RecordBatch) (arrow.RecordBatch, error) {
	schema := parts[0].Schema()
	var rows int64
	for _, p := range parts {
		rows += p.NumRows()
	}
	cols := make([]arrow.Array, schema.NumFields())
	for i := range cols {
		arrs := make([]arrow.Array, len(parts))
		for j, p := range parts {
			arrs[j] = p.Column(i)
		}
		col, err := array.Concatenate(arrs, memory.DefaultAllocator)
		if err != nil {
			for _, done := range cols[:i] {
				done.Release()
			}
			return nil, err
		}
		cols[i] = col
	}
	rec := array.NewRecordBatch(schema, cols, rows)
	for _, col := range cols {
		col.Release()
	}
	return rec, nil
}
