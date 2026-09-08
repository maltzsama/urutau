package dataplane

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

// CastPolicy maps column names to Arrow target types.
// This is the columnar equivalent of core.CastPolicy for the Arrow-native
// path. Each column is cast independently via compute.CastToType.
type CastPolicy map[string]arrow.DataType

// Cast applies per-column type widening to a batch. Columns not in the
// policy are passed through unchanged. Cast errors (incompatible types)
// surface as errors — no silent truncation.
//
// The batch is modified in place (new columns replace old via schema
// replacement). The original batch is NOT Released — the caller owns
// the returned batch.
func Cast(ctx context.Context, batch *Batch, policy CastPolicy) (*Batch, error) {
	if len(policy) == 0 || batch.Record == nil || batch.Record.NumRows() == 0 {
		return batch, nil
	}

	nrows := batch.Record.NumRows()
	cols := make([]arrow.Array, batch.Record.NumCols())
	fields := make([]arrow.Field, batch.Record.NumCols())
	castCount := 0

	for i := range int(batch.Record.NumCols()) {
		col := batch.Record.Column(i)
		name := batch.Record.ColumnName(i)

		targetType, needsCast := policy[name]
		if !needsCast {
			cols[i] = col
			fields[i] = arrow.Field{Name: name, Type: col.DataType(), Nullable: true}
			continue
		}

		castResult, err := compute.CastToType(ctx, col, targetType)
		if err != nil {
			// Release any columns we already cast.
			for j := range i {
				if j != i {
					cols[j].Release()
				}
			}
			return nil, fmt.Errorf("dataplane: cast %q: %w", name, err)
		}
		col.Release()
		cols[i] = castResult
		fields[i] = arrow.Field{Name: name, Type: targetType, Nullable: true}
		castCount++
	}

	if castCount == 0 {
		return batch, nil
	}

	schema := arrow.NewSchema(fields, nil)
	schemaFields := make([]arrow.ArrayData, len(cols))
	for i, c := range cols {
		schemaFields[i] = c.Data()
	}
	newRecord := newRecordBatchFromData(schema, schemaFields, int(nrows))
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
	CommitTS  time.Time
	IngestTS  time.Time
	Snapshot  bool
	Phase     string // "live" or "snapshot"
}

// AddMetadata injects system columns (__commit_ts, __ingest_ts, __snapshot,
// __phase) into the batch. These are always nullable Int64 (epoch millis
// for timestamps) or Boolean/Utf8 for snapshot/phase.
func AddMetadata(ctx context.Context, batch *Batch, meta MetadataColumns) (*Batch, error) {
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		return batch, nil
	}

	nrows := int(batch.Record.NumRows())
	alloc := memoryAllocator()
	tmpl := batch.Record

	// __commit_ts (Int64, epoch millis)
	ct := array.NewInt64Builder(alloc)
	defer ct.Release()
	for range nrows {
		ct.Append(meta.CommitTS.UnixMilli())
	}
	ctArr := ct.NewInt64Array()
	defer ctArr.Release()

	// __ingest_ts (Int64, epoch millis)
	it := array.NewInt64Builder(alloc)
	defer it.Release()
	for range nrows {
		it.Append(meta.IngestTS.UnixMilli())
	}
	itArr := it.NewInt64Array()
	defer itArr.Release()

	// __snapshot (Boolean)
	sb := array.NewBooleanBuilder(alloc)
	defer sb.Release()
	for range nrows {
		sb.Append(meta.Snapshot)
	}
	sbArr := sb.NewBooleanArray()
	defer sbArr.Release()

	// __phase (Utf8)
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
	defer pbArr.Release()

	// Build new schema with system columns appended.
	srcFields := tmpl.Schema().Fields()
	newFields := make([]arrow.Field, 0, len(srcFields)+4)
	newFields = append(newFields, srcFields...)
	newFields = append(newFields,
		arrow.Field{Name: "__commit_ts", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "__ingest_ts", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "__snapshot", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "__phase", Type: &arrow.StringType{}, Nullable: true},
	)
	newSchema := arrow.NewSchema(newFields, nil)

	// Build new columns slice: original + 4 system columns.
	cols := make([]arrow.Array, 0, len(srcFields)+4)
	for i := range len(srcFields) {
		cols = append(cols, tmpl.Column(i))
	}
	cols = append(cols, ctArr, itArr, sbArr, pbArr)

	fieldData := make([]arrow.ArrayData, len(cols))
	for i, c := range cols {
		fieldData[i] = c.Data()
	}
	newRecord := newRecordBatchFromData(newSchema, fieldData, nrows)

	return &Batch{
		Table:     batch.Table,
		Record:    newRecord,
		Watermark: batch.Watermark,
	}, nil
}

// newRecordBatchFromData builds a RecordBatch from schema + column arrays.
// Caller must release the returned RecordBatch.
func newRecordBatchFromData(schema *arrow.Schema, fieldData []arrow.ArrayData, nrows int) arrow.RecordBatch {
	cols := make([]arrow.Array, len(fieldData))
	for i, d := range fieldData {
		cols[i] = array.MakeFromData(d)
	}
	rb := array.NewRecordBatch(schema, cols, int64(nrows))
	for _, c := range cols {
		c.Release()
	}
	return rb
}
