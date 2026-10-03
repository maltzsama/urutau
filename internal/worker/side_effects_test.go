package worker

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/snapshot"
)

// The steady-state path (no snapshot in progress, no window open) applies no
// side effect; the marking path still records a live key once a snapshot is
// in progress (#577).
func TestMarkBatchSideEffectsSteadyStateAndSnapshot(t *testing.T) {
	batch := wireBatch(t, "t", dataplane.UpsertMode, []rowchange.Change{
		chg("t", rowchange.OpInsert, 7, "a", "p1"),
	})
	p := newTablePipeline("t", &fakeCommitter{}, dataplane.UpsertMode)
	p.knownSchema = testSchema()

	if err := markBatchSideEffects(p, batch, Ingest{Table: "t", Batch: batch}); err != nil {
		t.Fatalf("steady state: %v", err)
	}
	if p.bootstrapGuard.TestString(rowchange.KeyString([]any{int64(7)})) {
		t.Fatal("steady state marked a key with no snapshot in progress")
	}

	p.snapshotState = string(snapshot.StateInProgress)
	if err := markBatchSideEffects(p, batch, Ingest{Table: "t", Batch: batch}); err != nil {
		t.Fatalf("snapshot in progress: %v", err)
	}
	if !p.bootstrapGuard.TestString(rowchange.KeyString([]any{int64(7)})) {
		t.Fatal("snapshot in progress must mark the live key")
	}
}

// A malformed record must still surface its wrapped error on the steady-state
// path: the early return only skips the per-row work, not the schema check
// (Sourcery finding on #629).
func TestMarkBatchSideEffectsMalformedRecordErrors(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	bld.Field(0).(*array.Int64Builder).Append(1)
	rec := bld.NewRecordBatch()
	bld.Release()
	defer rec.Release()

	p := newTablePipeline("t", &fakeCommitter{}, dataplane.UpsertMode)
	p.knownSchema = testSchema()

	err := markBatchSideEffects(p, &dataplane.Batch{Table: "t", Record: rec}, Ingest{Table: "t"})
	if err == nil {
		t.Fatal("a record without the wire metadata schema must error, even in steady state")
	}
}

// An open window still deduplicates a live InWindow key: the early return
// must not skip the window path (#577).
func TestMarkBatchSideEffectsWindowDedup(t *testing.T) {
	batch := wireBatch(t, "t", dataplane.UpsertMode, []rowchange.Change{
		chg("t", rowchange.OpInsert, 7, "a", "p1"),
	})
	p := newTablePipeline("t", &fakeCommitter{}, dataplane.UpsertMode)
	p.knownSchema = testSchema()
	key := rowchange.KeyString([]any{int64(7)})
	p.windows[7] = &snapshotWindow{
		keys:    map[string]struct{}{key: {}},
		touched: map[string]struct{}{},
	}

	ing := Ingest{Table: "t", Batch: batch, Win: &rowchange.Window{ChunkID: 7, InWindow: true}}
	if err := markBatchSideEffects(p, batch, ing); err != nil {
		t.Fatalf("window path: %v", err)
	}
	if p.dropped != 1 {
		t.Fatalf("dropped = %d, want 1 (the live key must touch the window)", p.dropped)
	}
	if _, ok := p.windows[7].touched[key]; !ok {
		t.Fatal("the live key must be marked touched in the open window")
	}
}
