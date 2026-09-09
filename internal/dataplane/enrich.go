package dataplane

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
)

// CastPolicy maps column names to canonical cast targets (W-1/D-4): the
// core matrix is the POLICY — it vetoes before any kernel runs — and the
// arrow kernel is the single EXECUTOR.
type CastPolicy map[string]core.CastTarget

// Cast applies per-column casts to a batch. Columns not in the policy are
// passed through unchanged.
//
// Gate: every policy entry is validated against the batch's CURRENT
// canonical type via core.CheckCast — the matrix, not the arrow kernel,
// decides what is allowed (a narrowing arrow could technically do is
// still an error). A policy column missing from the batch is an error.
//
// Execution families: numeric/temporal/string via compute.CastToType with
// the target from transport.KindToArrow; binary → string(hex|base64) via a
// per-value kernel (arrow has none); composite → string is an explicit
// error (the matrix allows it, the columnar executor does not exist yet —
// honest lie over silent dump); timestamp → timestamptz requires
// assume_utc and yields a warning.
//
// OWNERSHIP: the input batch is NOT Released. The caller owns both input
// and output. Input always exits valid.
func Cast(ctx context.Context, batch *Batch, policy CastPolicy) (*Batch, []core.Warning, error) {
	if len(policy) == 0 || batch.Record == nil || batch.Record.NumRows() == 0 {
		return batch, nil, nil
	}

	// Current canonical types of the batch's DATA columns (metadata
	// columns are skipped by SchemaFromArrow — they are never cast).
	current, err := transport.SchemaFromArrow(batch.Record.Schema())
	if err != nil {
		return nil, nil, fmt.Errorf("dataplane: cast: input schema: %w", err)
	}
	srcType := make(map[string]core.ColumnType, len(current.Columns))
	for _, c := range current.Columns {
		srcType[c.Name] = c.Type
	}

	// 1. GATE — the matrix vetoes here, before any kernel.
	var warns []core.Warning
	for name, target := range policy {
		from, ok := srcType[name]
		if !ok {
			return nil, nil, fmt.Errorf("dataplane: cast: column %q does not exist", name)
		}
		if err := core.CheckCast(from, target); err != nil {
			return nil, nil, fmt.Errorf("dataplane: cast %q: %w", name, err)
		}
		if w := core.CastWarning(from.Kind, target); w != "" {
			warns = append(warns, core.Warning{Message: fmt.Sprintf("dataplane: cast %q: %s", name, w)})
		}
	}

	nrows := batch.Record.NumRows()
	ncols := int(batch.Record.NumCols())
	cols := make([]arrow.Array, ncols)
	fields := make([]arrow.Field, ncols)
	castCount := 0

	for i := range ncols {
		col := batch.Record.Column(i)
		name := batch.Record.ColumnName(i)

		target, needsCast := policy[name]
		if !needsCast {
			col.Retain()
			cols[i] = col
			fields[i] = batch.Record.Schema().Field(i)
			continue
		}

		castResult, err := castColumn(ctx, col, srcType[name], target)
		if err != nil {
			if castResult != nil {
				castResult.Release()
			}
			for j := range i {
				if cols[j] != nil {
					cols[j].Release()
				}
			}
			return nil, nil, fmt.Errorf("dataplane: cast %q: %w", name, err)
		}
		cols[i] = castResult
		fields[i] = arrow.Field{Name: name, Type: castResult.DataType(), Nullable: batch.Record.Schema().Field(i).Nullable}
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
		return batch, warns, nil
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
	}, warns, nil
}

// castColumn executes one cast on one column (W-1 step 3).
func castColumn(ctx context.Context, col arrow.Array, from core.ColumnType, target core.CastTarget) (arrow.Array, error) {
	switch target.Type.Kind {
	case core.KindString:
		switch from.Kind {
		case core.KindBinary, core.KindUUID, core.KindFixedBinary:
			// Custom kernel: arrow has no hex/base64 encode.
			return encodeBinaryToString(col, from.Kind, target.Encoding)
		case core.KindStruct, core.KindList, core.KindMap:
			return nil, fmt.Errorf("cast composite-to-string awaits a columnar executor (CR-069) — declare a native type or drop the cast")
		}
	case core.KindTimestampTZ:
		// assume_utc semantics are policy (validated by the matrix); the
		// kernel only retags the type — never shifts the value.
		return compute.CastToType(ctx, col, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	}
	at, err := transport.KindToArrow(target.Type)
	if err != nil {
		return nil, err
	}
	return compute.CastToType(ctx, col, at)
}

// encodeBinaryToString renders binary-family values as hex or base64 text.
func encodeBinaryToString(col arrow.Array, from core.Kind, encoding string) (arrow.Array, error) {
	switch encoding {
	case "hex", "base64":
	default:
		return nil, fmt.Errorf("binary → string is ambiguous; declare string(hex) or string(base64)")
	}
	bld := array.NewStringBuilder(memory.NewGoAllocator())
	defer bld.Release()
	appendText := func(b []byte) {
		if encoding == "hex" {
			bld.Append(hex.EncodeToString(b))
		} else {
			bld.Append(base64.StdEncoding.EncodeToString(b))
		}
	}
	switch a := col.(type) {
	case *array.Binary:
		for i := range a.Len() {
			if a.IsNull(i) {
				bld.AppendNull()
				continue
			}
			appendText(a.Value(i))
		}
	case *array.FixedSizeBinary:
		for i := range a.Len() {
			if a.IsNull(i) {
				bld.AppendNull()
				continue
			}
			appendText(a.Value(i))
		}
	default:
		return nil, fmt.Errorf("encodeBinaryToString: tipo fonte inesperado %T", col)
	}
	_ = from
	return bld.NewStringArray(), nil
}

// AddMetadata injects the __phase system column into the batch. The
// __commit_ts, __ingest_ts, __snapshot columns are expected on the wire
// (CoreSchemaToArrow emits them) — AddMetadata does NOT re-add them.
// Valid phases: "live", "snapshot".
//
// OWNERSHIP: the input batch is NOT Released. Input always exits valid.
func AddMetadata(ctx context.Context, alloc memory.Allocator, batch *Batch, phase string) (*Batch, error) {
	switch phase {
	case "live", "snapshot":
	default:
		return nil, fmt.Errorf("dataplane: addmetadata: phase %q unknown (want live | snapshot)", phase)
	}
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}
	if batch.Record == nil || batch.Record.NumRows() == 0 {
		return batch, nil
	}

	// Validate that the wire schema carries the 5 metadata columns.
	for _, w := range []string{"__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot"} {
		if colIndex(batch.Record.Schema(), w) < 0 {
			return nil, fmt.Errorf("dataplane: addmetadata: %q missing — wire-schema batch required", w)
		}
	}
	// Idempotent: __phase already present → no-op.
	if colIndex(batch.Record.Schema(), "__phase") >= 0 {
		return batch, nil
	}

	nrows := int(batch.Record.NumRows())
	tmpl := batch.Record

	// __phase (Utf8) — caller-provided phase.
	pb := array.NewStringBuilder(alloc)
	defer pb.Release()
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
