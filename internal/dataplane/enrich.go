package dataplane

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CastPolicy maps column names to Arrow target types.
type CastPolicy map[string]arrow.DataType

// Cast applies per-column type widening to a batch. Columns not in the
// policy are passed through unchanged. Cast errors (incompatible types)
// surface as errors — no silent truncation.
//
// OWNERSHIP: the input batch is NOT Released. The caller owns both input
// and output. Columns from the input are Retained for the output; cast
// columns are newly allocated. Input always exits valid.
func Cast(ctx context.Context, _ memory.Allocator, batch *Batch, policy CastPolicy) (*Batch, error) {
	if len(policy) == 0 || batch.Record == nil || batch.Record.NumRows() == 0 {
		return batch, nil
	}

	nrows := batch.Record.NumRows()
	ncols := int(batch.Record.NumCols())
	cols := make([]arrow.Array, ncols)
	fields := make([]arrow.Field, ncols)
	castCount := 0

	// Retain source columns — NewRecordBatch takes ownership, we must
	// not release them afterwards.
	for i := range ncols {
		col := batch.Record.Column(i)
		name := batch.Record.ColumnName(i)

		targetType, needsCast := policy[name]
		if !needsCast {
			col.Retain()
			cols[i] = col
			fields[i] = batch.Record.Schema().Field(i)
			continue
		}

		castResult, err := compute.CastToType(ctx, col, targetType)
		if err != nil {
			if castResult != nil {
				castResult.Release()
			}
			for j := range i {
				cols[j].Release()
			}
			return nil, fmt.Errorf("dataplane: cast %q: %w", name, err)
		}
		cols[i] = castResult
		fields[i] = arrow.Field{Name: name, Type: targetType, Nullable: batch.Record.Schema().Field(i).Nullable}
		castCount++
	}

	if castCount == 0 {
		return batch, nil
	}

	schema := arrow.NewSchema(fields, nil)
	newRecord := array.NewRecordBatch(schema, cols, int64(nrows))
	// NewRecordBatch retains each col but does NOT consume our ref.
	// Release our refs now; the record holds its own retained refs.
	for _, c := range cols {
		c.Release()
	}

	return &Batch{
		Table:     batch.Table,
		Record:    newRecord,
		Watermark: batch.Watermark,
	}, nil
}

// MetadataColumns are the system columns injected by the metadata stage.
type MetadataColumns struct {
	CommitTS time.Time
	IngestTS time.Time
	Snapshot bool
}

// AddMetadata injects system columns (__ingest_ts, __snapshot, __phase)
// into the batch. The __commit_ts column is projected from the existing
// wire schema (CR-069 §3.6: project, don't compute, what already exists).
//
// OWNERSHIP: the input batch is NOT Released. Input always exits valid.
func AddMetadata(ctx context.Context, alloc memory.Allocator, batch *Batch, meta MetadataColumns) (*Batch, error) {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		return batch, nil
	}

	nrows := int(batch.Record.NumRows())
	tmpl := batch.Record

	// Check if __commit_ts already exists — if so, project it; if not, add it.
	hasCommitTS := colIndex(tmpl.Schema(), "__commit_ts") >= 0

	// __ingest_ts (Int64, epoch millis)
	it := array.NewInt64Builder(alloc)
	defer it.Release()
	for range nrows {
		it.Append(meta.IngestTS.UnixMilli())
	}
	itArr := it.NewInt64Array()

	// __snapshot (Boolean)
	sb := array.NewBooleanBuilder(alloc)
	defer sb.Release()
	for range nrows {
		sb.Append(meta.Snapshot)
	}
	sbArr := sb.NewBooleanArray()

	// __phase (Utf8) — derived from Snapshot, not from MetadataColumns.Phase.
	pb := array.NewStringBuilder(alloc)
	defer pb.Release()
	phase := "live"
	if meta.Snapshot {
		phase = "snapshot"
	}
	for range nrows {
		pb.Append(phase)
	}
	pbArr := pb.NewStringArray()

	// Build new schema: original fields + system columns not yet present.
	srcFields := tmpl.Schema().Fields()
	extraFields := []arrow.Field{
		{Name: "__ingest_ts", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "__phase", Type: &arrow.StringType{}, Nullable: true},
	}
	if !hasCommitTS {
		extraFields = append([]arrow.Field{
			{Name: "__commit_ts", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		}, extraFields...)
	}
	newFields := make([]arrow.Field, 0, len(srcFields)+len(extraFields))
	newFields = append(newFields, srcFields...)
	newFields = append(newFields, extraFields...)
	newSchema := arrow.NewSchema(newFields, nil)

	// Build columns: Retain original columns + append new ones.
	cols := make([]arrow.Array, 0, len(srcFields)+len(extraFields))
	for i := range len(srcFields) {
		tmpl.Column(i).Retain()
		cols = append(cols, tmpl.Column(i))
	}
	if !hasCommitTS {
		// __commit_ts (Int64, epoch millis) — only add if not already in wire schema.
		ct := array.NewInt64Builder(alloc)
		defer ct.Release()
		for range nrows {
			ct.Append(meta.CommitTS.UnixMilli())
		}
		cols = append(cols, ct.NewInt64Array())
	}
	cols = append(cols, itArr, sbArr, pbArr)

	newRecord := array.NewRecordBatch(newSchema, cols, int64(nrows))
	// NewRecordBatch retains each col but does NOT consume our ref.
	// Release our refs now; the record holds its own retained refs.
	for _, c := range cols {
		c.Release()
	}

	return &Batch{
		Table:     batch.Table,
		Record:    newRecord,
		Watermark: batch.Watermark,
	}, nil
}
