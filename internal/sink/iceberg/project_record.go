// Columnar projection: maps a dataplane.Batch's wire Record to the
// Iceberg table schema (data columns + declared metadata columns).
// Data columns are retained or cast columnar (zero copy); metadata
// columns are built from the wire columns or constants (CR-069 §3.6).
package iceberg

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
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

	// Canonical per-row reads power the cast path; built once per batch.
	reader, err := transport.NewBatchReader(src, nil)
	if err != nil {
		return nil, fmt.Errorf("iceberg: %w", err)
	}

	for i, f := range fields {
		if m, ok := w.metaByName[f.Name]; ok {
			col, err := w.buildMetaColumn(src, m.From, f, nrows)
			if err != nil {
				releaseCols(cols, i)
				return nil, fmt.Errorf("iceberg: metadata %q: %w", f.Name, err)
			}
			cols[i] = col
			continue
		}
		col, err := w.projectDataColumn(ctx, reader, src, f)
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
// target field, casts columnar otherwise. A declared cast policy takes
// precedence and is applied VALUE-level: encodings like hex/base64 and the
// uuid canonical form are value transformations the arrow kernel cannot
// express. The row-based path always applied the cast; the columnar path
// silently skipped it until the test-verification helpers caught it.
func (w *TableWriter) projectDataColumn(ctx context.Context, reader *transport.BatchReader, src arrow.RecordBatch, field arrow.Field) (arrow.Array, error) {
	// The source Kind (from the wire) lets the kernel disambiguate values
	// whose Go type alone is ambiguous. A cast column must be present on the
	// wire: a missing column would silently bypass the matrix and write the
	// raw value.
	ct, hasCast := w.cast.Target(field.Name)
	from, kindOK := reader.ColumnKind(field.Name)
	if hasCast && !kindOK {
		return nil, fmt.Errorf("iceberg: column %q: kind not found in wire schema — cast cannot be applied", field.Name)
	}

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

	if hasCast {
		bld := array.NewBuilder(memory.DefaultAllocator, field.Type)
		defer bld.Release()
		values := make([]any, reader.NumRows())
		for i := range reader.NumRows() {
			v, ok := reader.Value(field.Name, i)
			if !ok || v == nil {
				values[i] = nil
				continue
			}
			cv, err := ct.Convert(from, v)
			if err != nil {
				return nil, fmt.Errorf("value %d: %w", i, err)
			}
			values[i] = cv
		}
		if err := appendColumn(bld, field, values); err != nil {
			return nil, err
		}
		return bld.NewArray(), nil
	}

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
// project, don't compute, what already exists). The target field carries the
// Arrow type the column MUST have: array.NewRecordBatch panics on any
// column/field type mismatch, so every branch — including the nulls — is
// typed from field, never from a fixed guess.
func (w *TableWriter) buildMetaColumn(src arrow.RecordBatch, key core.MetadataKey, field arrow.Field, nrows int64) (arrow.Array, error) {
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
		return timestampColumn(src, "__commit_ts", field.Type)

	case core.MetaIngestTS:
		return timestampColumn(src, "__ingest_ts", field.Type)

	case core.MetaPhase:
		// __phase rides the wire (born at the source); project it straight.
		phaseCol, err := stringColumn(src, "__phase")
		if err != nil {
			return nil, err
		}
		phaseCol.Retain()
		return phaseCol, nil

	case core.MetaSourceTable, core.MetaStream:
		return constString(src.NumRows(), w.sourceTable), nil

	default:
		// shard, msg_ts, msg_key, headers, enrich_miss: null. msg_ts is a
		// timestamp and enrich_miss a bool, so the null must be built in the
		// FIELD's type — a utf8 null would panic NewRecordBatch. (msg_ts is
		// NULL for CDC by design; enrich_miss's value is not on the wire yet,
		// so this is a typed NULL rather than the change's flag.)
		return nullColumn(field.Type, src.NumRows()), nil
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

func timestampColumn(src arrow.RecordBatch, name string, dt arrow.DataType) (arrow.Array, error) {
	target, ok := dt.(*arrow.TimestampType)
	if !ok {
		return nil, fmt.Errorf("iceberg: metadata %q: target field is %s, not a timestamp", name, dt)
	}
	idx := colIndexByName(src.Schema(), name)
	if idx < 0 {
		return nullColumn(target, src.NumRows()), nil
	}
	col := src.Column(idx)
	if col.DataType().ID() != arrow.TIMESTAMP {
		return nullColumn(target, src.NumRows()), nil
	}
	tsCol := col.(*array.Timestamp)
	unit := tsCol.DataType().(*arrow.TimestampType).Unit
	// Build in the TARGET field's unit and zone: the wire carries __commit_ts
	// as ns and __ingest_ts as us, while an Iceberg Timestamptz field is
	// timestamp[us, UTC]. A hardcoded unit mismatches the schema, which makes
	// array.NewRecordBatch panic.
	bb := array.NewTimestampBuilder(memory.DefaultAllocator, target)
	defer bb.Release()
	for i := range tsCol.Len() {
		if tsCol.IsNull(i) {
			bb.AppendNull()
			continue
		}
		// ToTime interprets the raw value with the SOURCE unit; AppendTime
		// stores it in the target's — the absolute instant is preserved.
		bb.AppendTime(tsCol.Value(i).ToTime(unit))
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
				v, err := scalarValue(b.Record.Column(idx), r)
				if err != nil {
					return nil, fmt.Errorf("iceberg: PK column %q: %w", pkCols[i], err)
				}
				key[i] = v
			}
			keys = append(keys, key)
		}
	}
	return keys, nil
}

// scalarValue extracts one scalar from an Arrow array at row. Used only for
// the equality-delete key boundary (§4.1) — never on the data path.
//
// The value it returns must be one that appendColumn accepts for the PK
// column's Iceberg type — deleteRecord feeds it straight back through
// appendColumn. Iceberg has no unsigned integer, so a KindUInt64 column is
// Decimal(20,0) in the schema (typemap) and on the data path; the delete key
// must carry the SAME canonical decimal form, not the raw uint64, or the
// delete path diverges from the data path (one value, one wire
// representation — the RV-11 family). Decimal, date and time keys travel as
// their canonical text, which is what appendColumn's decimal/date/time
// builders parse. An unhandled array type is an ERROR: returning nil here
// would write a NULL key that matches no row, so the delete would silently
// never apply.
func scalarValue(col arrow.Array, row int) (any, error) {
	switch c := col.(type) {
	case *array.Int64:
		return c.Value(row), nil
	case *array.Int32:
		return c.Value(row), nil
	case *array.Uint8:
		return c.Value(row), nil
	case *array.Uint64:
		return strconv.FormatUint(c.Value(row), 10), nil // canonical decimal(20,0) text
	case *array.Float64:
		return c.Value(row), nil
	case *array.Float32:
		return c.Value(row), nil
	case *array.String:
		return c.Value(row), nil
	case *array.Binary:
		return c.Value(row), nil
	case *array.FixedSizeBinary:
		return c.Value(row), nil
	case *array.Boolean:
		return c.Value(row), nil
	case *array.Timestamp:
		return c.Value(row).ToTime(c.DataType().(*arrow.TimestampType).Unit), nil
	case *array.Decimal128:
		return c.Value(row).ToString(c.DataType().(*arrow.Decimal128Type).Scale), nil
	case *array.Date32:
		return time.Unix(int64(c.Value(row))*86400, 0).UTC().Format("2006-01-02"), nil
	case *array.Time64:
		return formatTimeOfDay(int64(c.Value(row))), nil
	default:
		return nil, fmt.Errorf("unsupported primary-key column type %s", col.DataType())
	}
}

// formatTimeOfDay renders microseconds since midnight as the
// "15:04:05.ffffff" text timeToMicros parses back.
func formatTimeOfDay(micros int64) string {
	h := micros / 3_600_000_000
	micros %= 3_600_000_000
	m := micros / 60_000_000
	micros %= 60_000_000
	s := micros / 1_000_000
	micros %= 1_000_000
	return fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, micros)
}
