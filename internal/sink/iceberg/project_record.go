// Columnar projection: maps a dataplane.Batch's wire Record to the
// Iceberg table schema (data columns + declared metadata columns).
// Data columns are retained or cast columnar (zero copy); metadata
// columns are built from the wire columns or constants (CR-069 §3.6).
package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
)

// projectRecord maps a batch's wire Record into the Iceberg data schema.
// Data columns are retained when the types match, cast otherwise. Metadata
// columns are built columnar from the wire columns (__op, __pos,
// __commit_ts, __ingest_ts, __snapshot) or constants (source_table, phase,
// stream, seq).
//
// The caller owns the returned Record.
func (w *TableWriter) projectRecord(ctx context.Context, b *dataplane.Batch) (arrow.RecordBatch, error) {
	src := b.Record
	nrows := src.NumRows()
	fields := w.dataSchema.Fields()
	cols := make([]arrow.Array, len(fields))

	for i, f := range fields {
		if m, ok := w.metaByName[f.Name]; ok {
			col, err := w.buildMetaColumn(src, m.From, nrows)
			if err != nil {
				releaseCols(cols, i)
				return nil, fmt.Errorf("iceberg: metadata %q: %w", f.Name, err)
			}
			cols[i] = col
			continue
		}
		col, err := w.projectDataColumn(ctx, src, f)
		if err != nil {
			releaseCols(cols, i)
			return nil, fmt.Errorf("iceberg: column %q: %w", f.Name, err)
		}
		cols[i] = col
	}

	rec := array.NewRecordBatch(w.dataSchema, cols, nrows)
	for _, c := range cols {
		c.Release()
	}
	return rec, nil
}

// projectDataColumn retains a source column when the type matches the
// target field, otherwise casts columnar to the target type.
func (w *TableWriter) projectDataColumn(ctx context.Context, src arrow.RecordBatch, field arrow.Field) (arrow.Array, error) {
	idx := -1
	for i := range src.Schema().NumFields() {
		if src.Schema().Field(i).Name == field.Name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nullColumn(field.Type, src.NumRows()), nil
	}
	col := src.Column(idx)
	if arrow.TypeEqual(col.DataType(), field.Type) {
		col.Retain()
		return col, nil
	}
	casted, err := compute.CastToType(ctx, col, field.Type)
	if err != nil {
		return nil, err
	}
	return casted, nil
}

// buildMetaColumn builds one metadata column from the wire columns or a
// constant. Value mapping mirrors the row-based metaValue (CR-069 §3.6:
// project, don't compute, what already exists).
func (w *TableWriter) buildMetaColumn(src arrow.RecordBatch, key core.MetadataKey, nrows int64) (arrow.Array, error) {
	switch key {
	case core.MetaOp:
		opCol, err := uint8Column(src, "__op")
		if err != nil {
			return nil, err
		}
		bb := array.NewStringBuilder(memory.DefaultAllocator)
		defer bb.Release()
		for i := range opCol.Len() {
			bb.Append(changeOpString(opCol.Value(i)))
		}
		return bb.NewStringArray(), nil

	case core.MetaPosition, core.MetaSeq:
		posCol, err := stringColumn(src, "__pos")
		if err != nil {
			return nil, err
		}
		posCol.Retain()
		return posCol, nil

	case core.MetaCommitTS:
		return timestampColumn(src, "__commit_ts")

	case core.MetaIngestTS:
		return timestampColumn(src, "__ingest_ts")

	case core.MetaPhase:
		snapCol, err := boolColumn(src, "__snapshot")
		if err != nil {
			return nil, err
		}
		bb := array.NewStringBuilder(memory.DefaultAllocator)
		defer bb.Release()
		for i := range snapCol.Len() {
			if snapCol.Value(i) {
				bb.Append("snapshot")
			} else {
				bb.Append("stream")
			}
		}
		return bb.NewStringArray(), nil

	case core.MetaSourceTable, core.MetaStream:
		return constString(src.NumRows(), w.sourceTable), nil

	default:
		// shard, msg_ts, msg_key, headers, enrich_miss: null (CDC sources
		// carry none of these).
		return nullString(src.NumRows()), nil
	}
}

func releaseCols(cols []arrow.Array, upTo int) {
	for i := range upTo {
		if cols[i] != nil {
			cols[i].Release()
		}
	}
}

func colIndexByName(s *arrow.Schema, name string) int {
	for i := range s.NumFields() {
		if s.Field(i).Name == name {
			return i
		}
	}
	return -1
}

func uint8Column(src arrow.RecordBatch, name string) (*array.Uint8, error) {
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return nil, fmt.Errorf("column %q not on wire", name)
	}
	col, ok := src.Column(idx).(*array.Uint8)
	if !ok {
		return nil, fmt.Errorf("column %q type %T, want *array.Uint8", name, src.Column(idx))
	}
	return col, nil
}

func stringColumn(src arrow.RecordBatch, name string) (*array.String, error) {
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return nil, fmt.Errorf("column %q not on wire", name)
	}
	col, ok := src.Column(idx).(*array.String)
	if !ok {
		return nil, fmt.Errorf("column %q type %T, want *array.String", name, src.Column(idx))
	}
	return col, nil
}

func boolColumn(src arrow.RecordBatch, name string) (*array.Boolean, error) {
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return nil, fmt.Errorf("column %q not on wire", name)
	}
	col, ok := src.Column(idx).(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("column %q type %T, want *array.Boolean", name, src.Column(idx))
	}
	return col, nil
}

func timestampColumn(src arrow.RecordBatch, name string) (arrow.Array, error) {
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return nullString(src.NumRows()), nil
	}
	col := src.Column(idx)
	if col.DataType().ID() != arrow.TIMESTAMP {
		return nullString(src.NumRows()), nil
	}
	tsCol := col.(*array.Timestamp)
	unit := tsCol.DataType().(*arrow.TimestampType).Unit
	bb := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
	defer bb.Release()
	for i := range tsCol.Len() {
		if tsCol.IsNull(i) {
			bb.AppendNull()
			continue
		}
		bb.Append(arrow.Timestamp(tsCol.Value(i).ToTime(unit).UnixNano()))
	}
	return bb.NewTimestampArray(), nil
}

func changeOpString(op uint8) string {
	switch op {
	case 0:
		return "insert"
	case 1:
		return "update"
	default:
		return "delete"
	}
}

func nullColumn(dt arrow.DataType, n int64) arrow.Array {
	bb := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "x", Type: dt, Nullable: true}}, nil))
	defer bb.Release()
	for range n {
		bb.Field(0).AppendNull()
	}
	return bb.Field(0).NewArray()
}

func constString(n int64, v string) arrow.Array {
	bb := array.NewStringBuilder(memory.DefaultAllocator)
	defer bb.Release()
	for range n {
		bb.Append(v)
	}
	return bb.NewStringArray()
}

func nullString(n int64) arrow.Array {
	bb := array.NewStringBuilder(memory.DefaultAllocator)
	defer bb.Release()
	for range n {
		bb.AppendNull()
	}
	return bb.NewStringArray()
}

// splitByOp splits a batch into upsert rows (op != delete) and delete rows
// (op == delete) using columnar masks.
func splitByOp(ctx context.Context, b *dataplane.Batch) (upserts, deletes *dataplane.Batch, err error) {
	if b.Record == nil || b.Record.NumRows() == 0 {
		return nil, nil, nil
	}
	opIdx := colIndexByName(b.Record.Schema(), "__op")
	if opIdx < 0 {
		return nil, nil, fmt.Errorf("iceberg: __op column not found")
	}
	opCol, ok := b.Record.Column(opIdx).(*array.Uint8)
	if !ok {
		return nil, nil, fmt.Errorf("iceberg: __op type %T", b.Record.Column(opIdx))
	}
	up := array.NewBooleanBuilder(memory.DefaultAllocator)
	del := array.NewBooleanBuilder(memory.DefaultAllocator)
	for i := range opCol.Len() {
		isDel := opCol.Value(i) == uint8(rowchange.OpDelete)
		up.Append(!isDel)
		del.Append(isDel)
	}
	upArr := up.NewBooleanArray()
	delArr := del.NewBooleanArray()
	defer upArr.Release()
	defer delArr.Release()
	opts := compute.DefaultFilterOptions()
	fUp, err := compute.FilterRecordBatch(ctx, b.Record, upArr, opts)
	if err != nil {
		return nil, nil, fmt.Errorf("iceberg: filter upserts: %w", err)
	}
	fDel, err := compute.FilterRecordBatch(ctx, b.Record, delArr, opts)
	if err != nil {
		fUp.Release()
		return nil, nil, fmt.Errorf("iceberg: filter deletes: %w", err)
	}
	if fUp.NumRows() > 0 {
		upserts = &dataplane.Batch{Table: b.Table, Record: fUp, Watermark: b.Watermark}
	} else {
		fUp.Release()
	}
	if fDel.NumRows() > 0 {
		deletes = &dataplane.Batch{Table: b.Table, Record: fDel, Watermark: b.Watermark}
	} else {
		fDel.Release()
	}
	return upserts, deletes, nil
}

// extractKeys extracts the primary-key values of every row across the given
// batches as [][]any — the equality-delete key boundary (CR-069 §4.1):
// iceberg-go takes delete keys in its own form, so conversion happens on the
// already-filtered key batch, never on the data.
func extractKeys(batches []*dataplane.Batch, pkCols []string) ([][]any, error) {
	var keys [][]any
	for _, b := range batches {
		if b == nil || b.Record == nil {
			continue
		}
		idxs := make([]int, len(pkCols))
		for i, c := range pkCols {
			idx := colIndexByName(b.Record.Schema(), c)
			if idx < 0 {
				return nil, fmt.Errorf("iceberg: PK column %q not in batch", c)
			}
			idxs[i] = idx
		}
		for r := 0; r < int(b.Record.NumRows()); r++ {
			key := make([]any, len(pkCols))
			for i, idx := range idxs {
				key[i] = scalarValue(b.Record.Column(idx), r)
			}
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// scalarValue extracts one scalar from an Arrow array at row. Used only for
// the equality-delete key boundary (§4.1) — never on the data path.
func scalarValue(col arrow.Array, row int) any {
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(row)
	case *array.Int32:
		return c.Value(row)
	case *array.Uint8:
		return c.Value(row)
	case *array.Uint64:
		return c.Value(row)
	case *array.Float64:
		return c.Value(row)
	case *array.String:
		return c.Value(row)
	case *array.Binary:
		return c.Value(row)
	case *array.FixedSizeBinary:
		return c.Value(row)
	case *array.Boolean:
		return c.Value(row)
	case *array.Timestamp:
		return c.Value(row).ToTime(c.DataType().(*arrow.TimestampType).Unit)
	default:
		return nil
	}
}
