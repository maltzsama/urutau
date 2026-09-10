package enrich

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// ColumnarJoin applies the reference joins to a whole columnar batch: Arrow
// in, Arrow out, no rowchange in the path. Each reference is a broadcast
// hash lookup against its snapshot image; the reference columns are appended
// to the record as new typed columns and inner-join misses are filtered out.
//
// Semantics preserved from the row path:
//   - references are applied in declaration order;
//   - a delete (__op == OpDelete) bypasses every reference — never looked
//     up, never dropped by an inner miss;
//   - a NULL join key is a miss, never a lookup of the "nil" key;
//   - a left-join miss passes the row with the reference columns NULL and
//     bumps the miss counter; an inner-join miss removes the row;
//   - a sticky first-load error on any reference fails the batch.
//
// Cold start is decided PER BATCH, not per row (the row-mode per-row buffer
// is gone — see docs/semantics.md): with no snapshot yet loaded,
// onColdStart=drop returns (nil, nil) (whole batch dropped), and
// buffer/pass both miss every row.
//
// OWNERSHIP: the input batch is NOT released; the caller owns it. Returns
// nil when inner joins dropped every row.
func (s *Stage) ColumnarJoin(b *dataplane.Batch) (*dataplane.Batch, error) {
	if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		return b, nil
	}
	for _, rj := range s.refs {
		if err := rj.stickyErr(); err != nil {
			return nil, fmt.Errorf("enrich: reference %q: %w", rj.cfg.Table, err)
		}
	}

	ctx := context.Background()
	alloc := memory.DefaultAllocator
	rec := b.Record
	nrows := int(rec.NumRows())
	schema := rec.Schema()

	// Wire layout: data columns, then the fixed metadata tail. New reference
	// columns are inserted between them.
	numMeta := len(transport.WireMetadataFields())
	numData := schema.NumFields() - numMeta
	if numData < 0 {
		return nil, fmt.Errorf("enrich: batch is not wire schema (%d columns)", schema.NumFields())
	}
	opCol, ok := rec.Column(numData).(*array.Uint8) // __op is first metadata column
	if !ok {
		return nil, fmt.Errorf("enrich: __op column is %T, want *array.Uint8", rec.Column(numData))
	}
	isDelete := func(i int) bool { return rowchange.Op(opCol.Value(i)) == rowchange.OpDelete }

	// keep[i] survives; starts all-true, an inner miss clears it.
	keep := make([]bool, nrows)
	for i := range keep {
		keep[i] = true
	}

	// Working copy of the data region. A reference destination that already
	// exists as a data column (the caller pre-declared it via AddRefColumns
	// so the wire shape is stable) is REPLACED in place; a new one is
	// appended. `owned` marks arrays this function created and must release.
	dataFields := make([]arrow.Field, numData)
	dataArrs := make([]arrow.Array, numData)
	owned := make([]bool, numData)
	fieldIdx := make(map[string]int, numData)
	for j := 0; j < numData; j++ {
		dataFields[j] = schema.Field(j)
		dataArrs[j] = rec.Column(j)
		fieldIdx[schema.Field(j).Name] = j
	}
	colByName := func(name string) (arrow.Array, bool) {
		if j, ok := fieldIdx[name]; ok {
			return dataArrs[j], true
		}
		return nil, false
	}
	release := func() {
		for j := range dataArrs {
			if owned[j] {
				dataArrs[j].Release()
			}
		}
	}

	for _, rj := range s.refs {
		keyArr, ok := colByName(rj.onKey)
		if !ok {
			release()
			return nil, fmt.Errorf("enrich: reference %q: join column %q not in batch", rj.cfg.Table, rj.onKey)
		}
		snap := rj.snap.Load()
		if snap == nil && rj.policy == coldDrop {
			release()
			return nil, nil // whole batch dropped; caller treats nil as "all rows dropped"
		}

		var dests []dest
		var refTypes map[string]arrow.DataType
		if snap != nil {
			dests, refTypes = snap.dests, snap.refTypes
		}
		// With a wildcard select and no load yet, dests is empty and no
		// column is injected (the documented drift exception). With an
		// explicit select, fall back to the construction-time refDests
		// typed as String so the batch keeps a stable shape.
		if len(dests) == 0 && len(rj.refDests) > 0 {
			dests = make([]dest, len(rj.refDests))
			refTypes = make(map[string]arrow.DataType, len(rj.refDests))
			for i, name := range rj.refDests {
				dests[i] = dest{as: name}
				refTypes[name] = arrow.BinaryTypes.String
			}
		}

		inner := rj.cfg.JoinType == "inner"
		builders := make([]array.Builder, len(dests))
		for i, d := range dests {
			builders[i] = array.NewBuilder(alloc, refTypes[d.as])
		}
		relBuilders := func() {
			for _, bl := range builders {
				if bl != nil {
					bl.Release()
				}
			}
		}

		for i := 0; i < nrows; i++ {
			if !keep[i] {
				for _, bl := range builders {
					bl.AppendNull()
				}
				continue
			}
			var row map[string]any
			if !isDelete(i) && snap != nil {
				if kv := readArrowValue(keyArr, i); kv != nil {
					row = snap.image[joinKey(kv)]
				}
			}
			switch {
			case isDelete(i):
				// A delete bypasses the join entirely: dests NULL, row kept.
				for _, bl := range builders {
					bl.AppendNull()
				}
			case row != nil:
				for bi, d := range dests {
					if err := appendRefValue(builders[bi], refTypes[d.as], row[d.as]); err != nil {
						relBuilders()
						release()
						return nil, fmt.Errorf("enrich: reference %q column %q: %w", rj.cfg.Table, d.as, err)
					}
				}
			default: // miss (cold, NULL key, or no reference row)
				s.misses.Add(1)
				if s.metrics != nil {
					s.metrics.misses(b.Table, rj.cfg.Table)
				}
				if inner {
					keep[i] = false
					s.innerDropped.Add(1)
					if s.metrics != nil {
						s.metrics.dropped(b.Table, rj.cfg.Table)
					}
				}
				for _, bl := range builders {
					bl.AppendNull()
				}
			}
		}

		for bi, d := range dests {
			arr := builders[bi].NewArray()
			builders[bi] = nil
			f := arrow.Field{Name: d.as, Type: refTypes[d.as], Nullable: true}
			if j, exists := fieldIdx[d.as]; exists {
				if owned[j] {
					dataArrs[j].Release()
				}
				dataFields[j], dataArrs[j], owned[j] = f, arr, true
			} else {
				fieldIdx[d.as] = len(dataArrs)
				dataFields = append(dataFields, f)
				dataArrs = append(dataArrs, arr)
				owned = append(owned, true)
			}
		}
	}

	// Assemble: data columns (some replaced/appended), then the metadata tail.
	fields := make([]arrow.Field, 0, len(dataFields)+numMeta)
	cols := make([]arrow.Array, 0, len(dataArrs)+numMeta)
	fields = append(fields, dataFields...)
	cols = append(cols, dataArrs...)
	for j := numData; j < schema.NumFields(); j++ {
		fields = append(fields, schema.Field(j))
		cols = append(cols, rec.Column(j))
	}
	joined := array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, int64(nrows))
	release() // joined retains its own refs

	// Any inner miss? filter the record down to the kept rows.
	allKept := true
	for _, k := range keep {
		if !k {
			allKept = false
			break
		}
	}
	out := joined
	if !allKept {
		mb := array.NewBooleanBuilder(alloc)
		mb.AppendValues(keep, nil)
		mask := mb.NewBooleanArray()
		mb.Release()
		filtered, err := compute.FilterRecordBatch(ctx, joined, mask, compute.DefaultFilterOptions())
		mask.Release()
		joined.Release()
		if err != nil {
			return nil, fmt.Errorf("enrich: filter inner misses: %w", err)
		}
		if filtered.NumRows() == 0 {
			filtered.Release()
			return nil, nil // inner joins dropped everything
		}
		out = filtered
	}

	return &dataplane.Batch{
		Table:           b.Table,
		Record:          out,
		Watermark:       b.Watermark,
		Mode:            b.Mode,
		SnapshotState:   b.SnapshotState,
		SnapshotPending: b.SnapshotPending,
	}, nil
}

// readArrowValue reads the canonical Go value at row i for join-key lookup,
// nil when null. Only the types a join key can carry are handled; anything
// else returns nil (a miss) rather than panicking.
func readArrowValue(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}
	switch a := col.(type) {
	case *array.String:
		return a.Value(i)
	case *array.Binary:
		return a.Value(i)
	case *array.Int32:
		return int64(a.Value(i))
	case *array.Int64:
		return a.Value(i)
	case *array.Uint64:
		return a.Value(i)
	case *array.Float32:
		return float64(a.Value(i))
	case *array.Float64:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	default:
		return nil
	}
}

// appendRefValue appends one reference image value into the destination
// builder. The image holds raw Go values from the SQL loader; the builder's
// type came from refValueArrowType over the same value space, so the type
// switch mirrors it. A nil value appends null.
func appendRefValue(bld array.Builder, dt arrow.DataType, v any) error {
	if v == nil {
		bld.AppendNull()
		return nil
	}
	switch b := bld.(type) {
	case *array.StringBuilder:
		if s, ok := v.(string); ok {
			b.Append(s)
			return nil
		}
	case *array.BinaryBuilder:
		if bs, ok := v.([]byte); ok {
			b.Append(bs)
			return nil
		}
	case *array.BooleanBuilder:
		if bv, ok := v.(bool); ok {
			b.Append(bv)
			return nil
		}
	case *array.Int64Builder:
		switch t := v.(type) {
		case int64:
			b.Append(t)
			return nil
		case int:
			b.Append(int64(t))
			return nil
		case int32:
			b.Append(int64(t))
			return nil
		}
	case *array.Uint64Builder:
		if u, ok := v.(uint64); ok {
			b.Append(u)
			return nil
		}
	case *array.Float64Builder:
		switch t := v.(type) {
		case float64:
			b.Append(t)
			return nil
		case float32:
			b.Append(float64(t))
			return nil
		}
	case *array.TimestampBuilder:
		if tv, ok := v.(interface{ UnixMicro() int64 }); ok {
			b.Append(arrow.Timestamp(tv.UnixMicro()))
			return nil
		}
	}
	return fmt.Errorf("value %T does not fit destination %s (reference changed type between loads)", v, dt)
}
