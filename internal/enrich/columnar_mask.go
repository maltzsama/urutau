package enrich

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

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

// membershipMask builds the participation hit mask from the snapshot's
// keyIndex — built once per refresh — instead of the is_in kernel, which
// rebuilds an Arrow hash set of the whole reference key column on every batch
// (#589: a 100k-row reference costs ~7 ms / ~9 MB per 2000-row batch; a 5M-row
// one ~1.3 s / ~557 MB). The keyIndex is the same source gatherRefColumns
// already reads, so membership and the gather agree by construction. A NULL or
// unknown key normalizes to a value the index never holds, so it is a miss —
// the same verdict as is_in with NullMatchingSkip.
func membershipMask(alloc memory.Allocator, keyArr arrow.Array, snap *snapshot) (*array.Boolean, error) {
	n := keyArr.Len()
	b := array.NewBooleanBuilder(alloc)
	b.Reserve(n)
	//allow:rowloop ref membership: reuse the once-per-refresh keyIndex, not a per-batch set rebuild (#589).
	for i := 0; i < n; i++ {
		_, ok := snap.keyIndex[normalizeKey(readArrowValue(keyArr, i))]
		b.Append(ok)
	}
	out := b.NewBooleanArray()
	b.Release()
	return out, nil
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
