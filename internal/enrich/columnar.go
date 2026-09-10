package enrich

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// joinKind is the normalized join grammar.
type joinKind int

const (
	joinLeftOuter joinKind = iota // "" | "left" | "left outer" — miss passes with NULL ref columns
	joinInner                     // "inner" — miss drops the row
	joinLeftSemi                  // "left semi" — hit kept, NO ref columns in output
	joinLeftAnti                  // "left anti" — miss kept, NO ref columns in output
)

func normalizeJoinType(s string) joinKind {
	switch s {
	case "inner":
		return joinInner
	case "left semi":
		return joinLeftSemi
	case "left anti":
		return joinLeftAnti
	default: // "", "left", "left outer"
		return joinLeftOuter
	}
}

// ColumnarJoin applies the reference joins to a whole columnar batch: Arrow
// in, Arrow out, no rowchange in the path, and no map[string]any. Each
// reference is a hash-membership test (is_in) against its snapshot's join
// column, plus one Go pass — marked //allow:rowloop, the only one in the
// executor — that gathers the matched reference columns (arrow-go v18.7.0
// has no index_in / if_else, see api_probe_test.go). Everything else is a
// compute kernel: __op == delete, is_valid, and/or/not, FilterRecordBatch.
//
// Semantics:
//   - references applied in declaration order;
//   - a delete (__op == OpDelete) bypasses every reference — never looked
//     up, never dropped, ref columns NULL;
//   - a NULL join key is a miss;
//   - left outer: a miss passes with the ref columns NULL and bumps the
//     miss counter; inner: a miss drops the row;
//   - left semi: hits are kept WITHOUT ref columns; left anti: misses are
//     kept WITHOUT ref columns; a delete always survives both;
//   - a sticky first-load error on any reference fails the batch;
//   - cold start is per batch: onColdStart=drop drops the whole batch,
//     buffer/pass miss every row.
//
// OWNERSHIP: the input batch is NOT released; the caller owns it. Returns
// nil when the join dropped every row.
func (s *Stage) ColumnarJoin(ctx context.Context, b *dataplane.Batch) (*dataplane.Batch, error) {
	if b == nil || b.Record == nil || b.Record.NumRows() == 0 {
		return b, nil
	}
	for _, rj := range s.refs {
		if err := rj.stickyErr(); err != nil {
			return nil, fmt.Errorf("enrich: reference %q: %w", rj.cfg.Table, err)
		}
	}

	alloc := memory.DefaultAllocator
	ctx = compute.WithAllocator(ctx, alloc)
	rec := b.Record
	nrows := int(rec.NumRows())
	schema := rec.Schema()
	numMeta := len(transport.WireMetadataFields())
	numData := schema.NumFields() - numMeta
	if numData < 0 {
		return nil, fmt.Errorf("enrich: batch is not wire schema (%d columns)", schema.NumFields())
	}

	// 0. COLD FIRST: any coldDrop reference with no snapshot kills the batch
	//    before any kernel touches a nil snapshot.
	for _, rj := range s.refs {
		if rj.snap.Load() == nil && rj.policy == coldDrop {
			return nil, nil
		}
	}

	opCol, ok := rec.Column(numData).(*array.Uint8) // __op is the first metadata column
	if !ok {
		return nil, fmt.Errorf("enrich: __op column is %T, want *array.Uint8", rec.Column(numData))
	}

	// delMask: __op == OpDelete. Kernel.
	delMask, err := mustEqualScalar(ctx, opCol, uint8(rowchange.OpDelete))
	if err != nil {
		return nil, err
	}
	defer delMask.Release()

	// keep: all-true. __op is non-null by wire contract, so is_not_null(__op)
	// IS the all-true mask — never a []bool, never a loop.
	keep, err := mustCallBool(ctx, "is_not_null", opCol)
	if err != nil {
		return nil, err
	}
	// keep is reassigned per reference; track and release the current one.
	replaceKeep := func(next *array.Boolean) {
		keep.Release()
		keep = next
	}
	defer func() { keep.Release() }()

	// The working data region. A reference dest that already exists as a
	// data column (pre-declared via AddRefColumns for a stable wire shape)
	// is replaced in place; a new one is appended before the metadata tail.
	dataFields := make([]arrow.Field, numData) // config-sized
	dataArrs := make([]arrow.Array, numData)
	owned := make([]bool, numData)
	fieldIdx := make(map[string]int, numData) // P4-config: field name → column index
	for j := 0; j < numData; j++ {
		dataFields[j] = schema.Field(j)
		dataArrs[j] = rec.Column(j)
		fieldIdx[schema.Field(j).Name] = j
	}
	relOwned := func() {
		for j := range dataArrs {
			if owned[j] {
				dataArrs[j].Release()
			}
		}
	}

	var localMisses, localDropped int64

	for _, rj := range s.refs {
		j, ok := fieldIdx[rj.onKey]
		if !ok {
			relOwned()
			return nil, fmt.Errorf("enrich: reference %q: join column %q not in batch", rj.cfg.Table, rj.onKey)
		}
		keyArr := dataArrs[j]
		snap := rj.snap.Load()
		kind := normalizeJoinType(rj.cfg.JoinType)
		emitRefCols := kind == joinLeftOuter || kind == joinInner

		// participation = keep AND NOT delMask. Retained the whole
		// iteration; a delete does not participate in the lookup.
		notDel, err := mustNot(ctx, delMask)
		if err != nil {
			relOwned()
			return nil, err
		}
		participation, err := mustAnd(ctx, keep, notDel)
		notDel.Release()
		if err != nil {
			relOwned()
			return nil, err
		}

		// Resolve the dest set and their types.
		var dests []dest
		typeOf := func(string) arrow.DataType { return arrow.BinaryTypes.String }
		if snap != nil && len(snap.dests) > 0 {
			dests = snap.dests
			typeOf = snap.refType
		} else if len(rj.refDests) > 0 {
			dests = make([]dest, len(rj.refDests))
			for i, name := range rj.refDests {
				dests[i] = dest{as: name}
			}
		}

		// effHit and the gathered ref columns.
		var effHit *array.Boolean
		refCols := make([]arrow.Array, len(dests))

		if snap == nil || snap.refTable == nil {
			// Cold / empty reference: every participating row misses.
			effHit, err = mustAllFalse(ctx, nrows)
			if err != nil {
				participation.Release()
				relOwned()
				return nil, err
			}
			if emitRefCols {
				for i, d := range dests {
					refCols[i] = array.MakeArrayOfNull(alloc, typeOf(d.as), nrows)
				}
			}
		} else {
			hitMask, herr := mustIsIn(ctx, keyArr, snap.refTable.Column(0))
			if herr != nil {
				participation.Release()
				relOwned()
				return nil, herr
			}
			effHit, herr = mustAnd(ctx, hitMask, participation)
			hitMask.Release()
			if herr != nil {
				participation.Release()
				relOwned()
				return nil, herr
			}
			if emitRefCols {
				//allow:rowloop ref-column gather: arrow-go v18.7.0 has no index_in; one pass, effHit-gated.
				refCols, herr = gatherRefColumns(alloc, keyArr, effHit, snap, dests, typeOf)
				if herr != nil {
					effHit.Release()
					participation.Release()
					relOwned()
					return nil, fmt.Errorf("enrich: reference %q: %w", rj.cfg.Table, herr)
				}
			}
		}

		// Insert the ref columns (replace-in-place or append).
		if emitRefCols {
			for i, d := range dests {
				f := arrow.Field{Name: d.as, Type: typeOf(d.as), Nullable: true}
				if k, exists := fieldIdx[d.as]; exists {
					if owned[k] {
						dataArrs[k].Release()
					}
					dataFields[k], dataArrs[k], owned[k] = f, refCols[i], true
				} else {
					fieldIdx[d.as] = len(dataArrs)
					dataFields = append(dataFields, f)
					dataArrs = append(dataArrs, refCols[i])
					owned = append(owned, true)
				}
			}
		}

		// realMiss = participation AND NOT effHit — the rows this reference
		// counts as a left-join miss.
		notHit, err := mustNot(ctx, effHit)
		if err != nil {
			effHit.Release()
			participation.Release()
			relOwned()
			return nil, err
		}
		realMiss, err := mustAnd(ctx, participation, notHit)
		if err != nil {
			notHit.Release()
			effHit.Release()
			participation.Release()
			relOwned()
			return nil, err
		}
		misses := countTrue(realMiss)
		localMisses += misses
		if s.metrics != nil && misses > 0 {
			s.metrics.misses(b.Table, rj.cfg.Table)
		}

		// Refine keep.
		notPart, err := mustNot(ctx, participation)
		if err != nil {
			realMiss.Release()
			notHit.Release()
			effHit.Release()
			participation.Release()
			relOwned()
			return nil, err
		}
		switch kind {
		case joinInner, joinLeftSemi:
			// keep AND (hit OR does-not-participate). A delete does not
			// participate, so it survives; a hit survives; a miss drops.
			survive, e := mustOr(ctx, effHit, notPart)
			if e == nil {
				var nk *array.Boolean
				nk, e = mustAnd(ctx, keep, survive)
				survive.Release()
				if e == nil {
					replaceKeep(nk)
				}
			}
			err = e
		case joinLeftAnti:
			// keep AND (miss OR does-not-participate). A hit drops; a miss
			// and a delete survive.
			survive, e := mustOr(ctx, realMiss, notPart)
			if e == nil {
				var nk *array.Boolean
				nk, e = mustAnd(ctx, keep, survive)
				survive.Release()
				if e == nil {
					replaceKeep(nk)
				}
			}
			err = e
		case joinLeftOuter:
			// keep unchanged — a miss passes with NULL ref columns.
		}
		notPart.Release()
		realMiss.Release()
		notHit.Release()
		effHit.Release()
		participation.Release()
		if err != nil {
			relOwned()
			return nil, err
		}
	}

	kept := countTrue(keep)
	localDropped = int64(nrows) - kept
	s.misses.Add(localMisses)
	s.innerDropped.Add(localDropped)
	if s.metrics != nil && localDropped > 0 {
		s.metrics.dropped(b.Table, "")
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
	relOwned() // joined retains its own refs

	if kept == int64(nrows) {
		return &dataplane.Batch{
			Table: b.Table, Record: joined, Watermark: b.Watermark, Mode: b.Mode,
			SnapshotState: b.SnapshotState, SnapshotPending: b.SnapshotPending,
		}, nil
	}
	filtered, err := compute.FilterRecordBatch(ctx, joined, keep, compute.DefaultFilterOptions())
	joined.Release()
	if err != nil {
		return nil, fmt.Errorf("enrich: filter dropped rows: %w", err)
	}
	if filtered.NumRows() == 0 {
		filtered.Release()
		return nil, nil
	}
	return &dataplane.Batch{
		Table: b.Table, Record: filtered, Watermark: b.Watermark, Mode: b.Mode,
		SnapshotState: b.SnapshotState, SnapshotPending: b.SnapshotPending,
	}, nil
}

// gatherRefColumns builds one Arrow array per destination: where effHit is
// true, the value from the reference row keyIndex points at; elsewhere
// null. arrow-go v18.7.0 has no index_in kernel, so this is one Go pass —
// the executor's only //allow:rowloop.
func gatherRefColumns(alloc memory.Allocator, keyArr arrow.Array, effHit *array.Boolean,
	snap *snapshot, dests []dest, typeOf func(string) arrow.DataType) ([]arrow.Array, error) {
	n := keyArr.Len()
	builders := make([]array.Builder, len(dests))
	for i, d := range dests {
		builders[i] = array.NewBuilder(alloc, typeOf(d.as))
	}
	release := func() {
		for _, bl := range builders {
			if bl != nil {
				bl.Release()
			}
		}
	}
	for i := 0; i < n; i++ {
		if !effHit.Value(i) {
			for _, bl := range builders {
				bl.AppendNull()
			}
			continue
		}
		idx := snap.keyIndex[normalizeKey(readArrowValue(keyArr, i))]
		for bi, d := range dests {
			// dest bi is refTable column bi+1.
			col := snap.refTable.Column(bi + 1)
			_ = d
			if err := appendArrowValue(builders[bi], col, int(idx)); err != nil {
				release()
				return nil, err
			}
		}
	}
	out := make([]arrow.Array, len(builders))
	for i, bl := range builders {
		out[i] = bl.NewArray()
		bl.Release()
		builders[i] = nil
	}
	return out, nil
}

// appendArrowValue copies row `row` of `src` into `bld` — same type on both
// sides (the builder was created from src's type).
func appendArrowValue(bld array.Builder, src arrow.Array, row int) error {
	if src.IsNull(row) {
		bld.AppendNull()
		return nil
	}
	switch b := bld.(type) {
	case *array.StringBuilder:
		b.Append(src.(*array.String).Value(row))
	case *array.BinaryBuilder:
		b.Append(src.(*array.Binary).Value(row))
	case *array.BooleanBuilder:
		b.Append(src.(*array.Boolean).Value(row))
	case *array.Int64Builder:
		b.Append(src.(*array.Int64).Value(row))
	case *array.Uint64Builder:
		b.Append(src.(*array.Uint64).Value(row))
	case *array.Float64Builder:
		b.Append(src.(*array.Float64).Value(row))
	case *array.TimestampBuilder:
		b.Append(src.(*array.Timestamp).Value(row))
	default:
		return fmt.Errorf("enrich: no copy case for builder %T", bld)
	}
	return nil
}

// readArrowValue reads the canonical Go value at row i for the keyIndex
// lookup, nil when null.
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
	case *array.Timestamp:
		return a.Value(i).ToTime(arrow.Microsecond)
	default:
		return nil
	}
}

// ── kernel helpers ──────────────────────────────────────────────────────

func boolFromDatum(d compute.Datum) *array.Boolean {
	return d.(*compute.ArrayDatum).MakeArray().(*array.Boolean)
}

func mustEqualScalar(ctx context.Context, col arrow.Array, scalar any) (*array.Boolean, error) {
	out, err := compute.CallFunction(ctx, "equal", nil,
		&compute.ArrayDatum{Value: col.Data()}, compute.NewDatum(scalar))
	if err != nil {
		return nil, fmt.Errorf("enrich: kernel equal: %w", err)
	}
	defer out.Release()
	return boolFromDatum(out), nil
}

func mustCallBool(ctx context.Context, fn string, col arrow.Array) (*array.Boolean, error) {
	out, err := compute.CallFunction(ctx, fn, nil, &compute.ArrayDatum{Value: col.Data()})
	if err != nil {
		return nil, fmt.Errorf("enrich: kernel %s: %w", fn, err)
	}
	defer out.Release()
	return boolFromDatum(out), nil
}

func mustNot(ctx context.Context, m *array.Boolean) (*array.Boolean, error) {
	out, err := compute.CallFunction(ctx, "not", nil, &compute.ArrayDatum{Value: m.Data()})
	if err != nil {
		return nil, fmt.Errorf("enrich: kernel not: %w", err)
	}
	defer out.Release()
	return boolFromDatum(out), nil
}

func mustAnd(ctx context.Context, a, bm *array.Boolean) (*array.Boolean, error) {
	out, err := compute.CallFunction(ctx, "and", nil,
		&compute.ArrayDatum{Value: a.Data()}, &compute.ArrayDatum{Value: bm.Data()})
	if err != nil {
		return nil, fmt.Errorf("enrich: kernel and: %w", err)
	}
	defer out.Release()
	return boolFromDatum(out), nil
}

func mustOr(ctx context.Context, a, bm *array.Boolean) (*array.Boolean, error) {
	out, err := compute.CallFunction(ctx, "or", nil,
		&compute.ArrayDatum{Value: a.Data()}, &compute.ArrayDatum{Value: bm.Data()})
	if err != nil {
		return nil, fmt.Errorf("enrich: kernel or: %w", err)
	}
	defer out.Release()
	return boolFromDatum(out), nil
}

func mustIsIn(ctx context.Context, values arrow.Array, valueSet arrow.Array) (*array.Boolean, error) {
	out, err := compute.CallFunction(ctx, "is_in",
		&compute.SetOptions{
			ValueSet:     &compute.ArrayDatum{Value: valueSet.Data()},
			NullBehavior: compute.NullMatchingSkip,
		},
		&compute.ArrayDatum{Value: values.Data()})
	if err != nil {
		return nil, fmt.Errorf("enrich: kernel is_in: %w", err)
	}
	defer out.Release()
	return boolFromDatum(out), nil
}

func mustAllFalse(_ context.Context, n int) (*array.Boolean, error) {
	b := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(make([]bool, n), nil)
	return b.NewBooleanArray(), nil
}

// countTrue counts the set bits of a boolean mask — no loop, no builder.
// Every mask in this executor derives from is_not_null(__op) (all-valid,
// __op is non-null by wire contract) through and/or/not, so nulls never
// appear and a raw set-bit count is exact.
func countTrue(m *array.Boolean) int64 {
	if m.Len() == 0 {
		return 0
	}
	valuesBuf := m.Data().Buffers()[1]
	if valuesBuf == nil {
		return 0
	}
	return int64(bitutil.CountSetBits(valuesBuf.Bytes(), m.Data().Offset(), m.Len()))
}
