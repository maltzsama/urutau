package worker

import (
	"testing"

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
