// Columnar projection: maps a dataplane.Batch's wire Record to the
// Iceberg table schema (data columns + declared metadata columns).
// Data columns are retained or cast columnar (zero copy); metadata
// columns are built from the wire columns or constants (CR-069 §3.6).
package iceberg

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/transport"

	"github.com/maltzsama/urutau/internal/sink/iceberg/convert"
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
				convert.ReleaseCols(cols, i)
				return nil, fmt.Errorf("iceberg: metadata %q: %w", f.Name, err)
			}
			cols[i] = col
			continue
		}
		col, err := w.projectDataColumn(ctx, reader, src, f)
		if err != nil {
			convert.ReleaseCols(cols, i)
			return nil, fmt.Errorf("iceberg: column %q: %w", f.Name, err)
		}
		// A NULL has no representation in a required column: Parquet
		// would write the value slot, landing it as "" or 0. A table
		// created before its source reported nullability (the MySQL
		// source did not) has every column required.
		if !f.Nullable && col.NullN() > 0 {
			col.Release()
			convert.ReleaseCols(cols, i)
			return nil, fmt.Errorf("iceberg: column %q is required in %v but %d row(s) carry NULL — "+
				"make the column optional (e.g. Trino: ALTER TABLE ... ALTER COLUMN %s DROP NOT NULL) and resume",
				f.Name, w.ident, col.NullN(), f.Name)
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
		// A missing column is a null for a nullable field, but an error for a
		// required one: silently nulling a required column would write data
		// the schema says cannot be null.
		if !field.Nullable {
			return nil, fmt.Errorf("iceberg: required column %q is missing from the wire", field.Name)
		}
		return convert.NullColumn(field.Type, src.NumRows()), nil
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
			// Iceberg has no unsigned integer: a uint64 cast lands in a
			// decimal(20,0) column, as canonical decimal text, the same
			// form scalarValue gives the delete path.
			if u, ok := cv.(uint64); ok {
				cv = strconv.FormatUint(u, 10)
			}
			values[i] = cv
		}
		if err := convert.AppendColumn(bld, field, values); err != nil {
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
		opCol, err := convert.Uint8Column(src, "__op")
		if err != nil {
			return nil, err
		}
		bb := array.NewStringBuilder(memory.DefaultAllocator)
		defer bb.Release()
		for i := range opCol.Len() {
			if opCol.IsNull(i) {
				return nil, fmt.Errorf("iceberg: __op is NULL at row %d", i)
			}
			s, err := convert.ChangeOpString(opCol.Value(i))
			if err != nil {
				return nil, err
			}
			bb.Append(s)
		}
		return bb.NewStringArray(), nil

	case core.MetaPosition, core.MetaSeq:
		posCol, err := convert.StringColumn(src, "__pos")
		if err != nil {
			return nil, err
		}
		posCol.Retain()
		return posCol, nil

	case core.MetaCommitTS:
		return convert.TimestampColumn(src, "__commit_ts", field.Type)

	case core.MetaIngestTS:
		return convert.TimestampColumn(src, "__ingest_ts", field.Type)

	case core.MetaPhase:
		// __phase rides the wire (born at the source); project it straight.
		phaseCol, err := convert.StringColumn(src, "__phase")
		if err != nil {
			return nil, err
		}
		phaseCol.Retain()
		return phaseCol, nil

	case core.MetaSourceTable, core.MetaStream:
		return convert.ConstString(src.NumRows(), w.sourceTable), nil

	default:
		// shard, msg_ts, msg_key, headers: null. msg_ts is a timestamp, so
		// the null must be built in the FIELD's type — a utf8 null would
		// panic NewRecordBatch. (msg_ts is NULL for CDC by design.)
		return convert.NullColumn(field.Type, src.NumRows()), nil
	}
}
