package columnar

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// SplitByOwner filters rec into len(nOwners) sub-records, one per
// partition index in owner (owner[i] is the partition row i belongs to).
// A partition with zero matching rows gets a nil entry (skipped by the
// caller) rather than an empty-but-non-nil record: a zero-row batch is a
// marker to the coordinator's send path, not an empty data batch.
func SplitByOwner(ctx context.Context, rec arrow.RecordBatch, owner []int, nOwners int) ([]arrow.RecordBatch, error) {
	nRows := int(rec.NumRows())
	// One pass bucketing the row indices by owner (was one full scan per
	// partition, O(owners × rows)).
	builders := make([]*array.Int64Builder, nOwners)
	for i, o := range owner {
		if o < 0 || o >= nOwners {
			continue
		}
		if builders[o] == nil {
			builders[o] = array.NewInt64Builder(memory.DefaultAllocator)
		}
		builders[o].Append(int64(i))
	}
	releaseBuilders := func(from int) {
		for j := from; j < nOwners; j++ {
			if builders[j] != nil {
				builders[j].Release()
				builders[j] = nil
			}
		}
	}

	out := make([]arrow.RecordBatch, nOwners)
	for p := 0; p < nOwners; p++ {
		b := builders[p]
		if b == nil {
			continue
		}
		builders[p] = nil
		// Every row belongs to this one partition: retain the record instead
		// of materializing an identity Take.
		if b.Len() == nRows {
			b.Release()
			rec.Retain()
			out[p] = rec
			continue
		}
		idxArr := b.NewInt64Array()
		b.Release()
		datum, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			&compute.RecordDatum{Value: rec}, &compute.ArrayDatum{Value: idxArr.Data()})
		idxArr.Release()
		if err != nil {
			ReleaseRecords(out)
			releaseBuilders(p + 1)
			return nil, fmt.Errorf("partition %d: %w", p, err)
		}
		// Take returns a *RecordDatum for a record input; guard the assertion
		// so a future kernel change cannot panic the coordinator (issue #211).
		rd, ok := datum.(*compute.RecordDatum)
		if !ok {
			datum.Release()
			ReleaseRecords(out)
			releaseBuilders(p + 1)
			return nil, fmt.Errorf("partition %d: unexpected Take datum %T", p, datum)
		}
		out[p] = rd.Value
	}
	return out, nil
}

// ReleaseRecords releases every non-nil record in a SplitByOwner result.
func ReleaseRecords(recs []arrow.RecordBatch) {
	for _, r := range recs {
		if r != nil {
			r.Release()
		}
	}
}
