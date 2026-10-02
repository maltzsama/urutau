package worker

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
)

// nilEnricher drops every row, like an inner join with no match.
type nilEnricher struct{}

func (nilEnricher) EnrichBatch(context.Context, *dataplane.Batch, []string) (*dataplane.Batch, error) {
	return nil, nil
}

// A batch the join drops entirely still needs its staged-cycle delivery, and
// applyEnrich must release the input.
func TestApplyEnrichBuffersEmptyWhenAllRowsDropped(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	bld.Field(0).(*array.Int64Builder).Append(1)
	rec := bld.NewRecordBatch()
	bld.Release()

	p := &tablePipeline{target: "t", enricher: nilEnricher{}}
	b := &dataplane.Batch{Table: "t", Record: rec, Staged: true, Seq: 3}

	queued := 0
	_, dropped, err := p.applyEnrich(context.Background(), b, int(rec.NumRows()), func(*dataplane.Batch) error {
		queued++
		return nil
	})
	if err != nil {
		t.Fatalf("applyEnrich: %v", err)
	}
	if !dropped {
		t.Fatal("dropped = false, want true when every row is dropped")
	}
	if queued != 1 {
		t.Fatalf("bufferEmpty calls = %d, want 1", queued)
	}
	if b.Record != nil {
		t.Fatal("applyEnrich must release the input batch")
	}
}
