package enrich

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/core"
)

// resolveDests returns the destination columns a reference injects and a
// function giving each one's Arrow type. A destination's type is what the
// batch schema declares for it (AddColumns pre-declares the column so cold and
// hot batches share one shape; a mismatch would make the worker's pending
// concat fail, issue #564). The reference's own type is only a fallback for a
// destination the batch does not pre-declare.
func resolveDests(rj *refJoin, snap *snapshot, schema *arrow.Schema, fieldIdx map[string]int) ([]dest, func(string) arrow.DataType) {
	typeOf := func(as string) arrow.DataType {
		if k, exists := fieldIdx[as]; exists {
			return schema.Field(k).Type
		}
		if snap != nil {
			if t := snap.refType(as); t != nil {
				return t
			}
		}
		return arrow.BinaryTypes.String
	}
	var dests []dest
	if snap != nil && len(snap.dests) > 0 {
		dests = snap.dests
	} else if names := rj.refDestNames(); len(names) > 0 {
		dests = make([]dest, len(names))
		for i, name := range names {
			dests[i] = dest{as: name}
		}
	}
	return dests, typeOf
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
	//allow:rowloop ref-column gather: arrow-go v18.7.0 has no index_in; one pass over the batch key, effHit-gated.
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
			if err := appendRefValue(builders[bi], col, int(idx)); err != nil {
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

// appendRefValue appends the reference's value at row into bld, which was
// built for the destination's DECLARED type. When the reference column shares
// that type it is copied; when it differs — the registered string contract
// holding a non-string reference — the canonical value is rendered into the
// declared builder (issue #564).
func appendRefValue(bld array.Builder, src arrow.Array, row int) error {
	if src.IsNull(row) {
		bld.AppendNull()
		return nil
	}
	if arrow.TypeEqual(bld.Type(), src.DataType()) {
		return appendArrowValue(bld, src, row)
	}
	if sb, ok := bld.(*array.StringBuilder); ok {
		s, err := core.StringifyScalar(readArrowValue(src, row))
		if err != nil {
			return err
		}
		sb.Append(s)
		return nil
	}
	return fmt.Errorf("enrich: destination column %s cannot hold reference column %s", bld.Type(), src.DataType())
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
