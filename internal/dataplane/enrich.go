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
func Cast(ctx context.Context, batch *Batch, policy CastPolicy) (*Batch, error) {
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
		// No columns matched the policy — release the Retains from the
		// passthrough loop; the early return skips the record that would
		// own them.
		for _, c := range cols {
			if c != nil {
				c.Release()
			}
		}
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
		Table:           batch.Table,
		Record:          newRecord,
		Watermark:       batch.Watermark,
		Mode:            batch.Mode,
		SnapshotState:   batch.SnapshotState,
		SnapshotPending: batch.SnapshotPending,
	}, nil
}

// MetadataColumns are the system columns injected by the metadata stage.
type MetadataColumns struct {
	CommitTS time.Time
	IngestTS time.Time
	Snapshot bool
}

// AddMetadata injects the __phase system column into the batch. The
// __commit_ts, __ingest_ts, __snapshot columns are expected on the wire
// (CoreSchemaToArrow emits them) — AddMetadata does NOT re-add them.
//
// OWNERSHIP: the input batch is NOT Released. Input always exits valid.
func AddMetadata(ctx context.Context, alloc memory.Allocator, batch *Batch, meta MetadataColumns) (*Batch, error) {
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		return batch, nil
	}

	// Validate that the wire schema carries the 5 metadata columns.
	for _, w := range []string{"__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot"} {
		if colIndex(batch.Record.Schema(), w) < 0 {
			return nil, fmt.Errorf("dataplane: addmetadata: %q ausente — requer batch wire-schema", w)
		}
	}
	// Idempotent: __phase already present → no-op.
	if colIndex(batch.Record.Schema(), "__phase") >= 0 {
		return batch, nil
	}

	nrows := int(batch.Record.NumRows())
	tmpl := batch.Record

	// __phase (Utf8) — derived from Snapshot.
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

	// Build new schema: original fields + __phase.
	srcFields := tmpl.Schema().Fields()
	newFields := make([]arrow.Field, 0, len(srcFields)+1)
	newFields = append(newFields, srcFields...)
	newFields = append(newFields, arrow.Field{Name: "__phase", Type: &arrow.StringType{}, Nullable: true})
	newSchema := arrow.NewSchema(newFields, nil)

	// Build columns: Retain original columns + append __phase.
	cols := make([]arrow.Array, 0, len(srcFields)+1)
	for i := range len(srcFields) {
		tmpl.Column(i).Retain()
		cols = append(cols, tmpl.Column(i))
	}
	cols = append(cols, pbArr)

	newRecord := array.NewRecordBatch(newSchema, cols, int64(nrows))
	for _, c := range cols {
		c.Release()
	}

	return &Batch{
		Table:           batch.Table,
		Record:          newRecord,
		Watermark:       batch.Watermark,
		Mode:            batch.Mode,
		SnapshotState:   batch.SnapshotState,
		SnapshotPending: batch.SnapshotPending,
	}, nil
}
