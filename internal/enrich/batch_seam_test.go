package enrich

// RV-01: the enriched batch's re-encode bridge carries the table's primary
// key. Without it, the bridge C-8 fail-fast rejects ANY enriched batch
// containing a delete — SchemaFromArrow reconstructs columns only, the PK
// must ride from the Assignment (the worker passes knownSchema.PrimaryKey).

import (
	"testing"

	"github.com/maltzsama/urutau/core"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

func pkBatch(t *testing.T) *dpint.Batch {
	t.Helper()
	cb := rowchange.Batch{
		Table: "events",
		Changes: []rowchange.Change{
			{Op: rowchange.OpInsert, Key: []any{int64(1)},
				After: map[string]any{"id": int64(1), "user_ref": int64(1), "q": "x"}, Position: "p1"},
			{Op: rowchange.OpDelete, Key: []any{int64(2)}, Position: "p2"},
		},
		Mode: rowchange.UpsertMode,
	}
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64, Nullable: true}},
			{Name: "q", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
	dpb, err := dpint.BatchFromChangeBatch(cb, cs)
	if err != nil {
		t.Fatalf("bridge input: %v", err)
	}
	return dpb
}

func TestEnrichBatchPrimaryKeyUnlocksDeletes(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	in := pkBatch(t)
	defer in.Release()

	out, err := s.EnrichBatch(in, []string{"id"})
	if err != nil {
		t.Fatalf("EnrichBatch with PK must pass the bridge delete guard: %v", err)
	}
	if out == nil {
		t.Fatal("expected output (left join keeps the delete row)")
	}
	defer out.Release()

	rows, err := transport.DecodeBatch(out.Record, "events", []string{"id"})
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	var del *rowchange.Change
	for i := range rows {
		if rows[i].Op == rowchange.OpDelete {
			del = &rows[i]
		}
	}
	if del == nil {
		t.Fatal("delete row missing from enriched output")
	}
	// The key-only delete must carry its PK value — the bridge backfill
	// only fires when the schema knows the PK.
	if len(del.Key) != 1 || del.Key[0] != int64(2) {
		t.Fatalf("delete key = %v, want [2]", del.Key)
	}
	if id, ok := del.After["id"].(int64); !ok || id != 2 {
		t.Fatalf("delete After[pk] = %v, want 2 — key projection lost", del.After["id"])
	}
}

func TestEnrichBatchWithoutPKRejectsDeletes(t *testing.T) {
	s, _ := newTestStage(t, refCfg(nil), usersRows())
	in := pkBatch(t)
	defer in.Release()

	// Control: without the PK the bridge C-8 fail-fast must still fire —
	// the seam must not silently lose the guard.
	if _, err := s.EnrichBatch(in, nil); err == nil {
		t.Fatal("EnrichBatch without PK must error on batches carrying deletes")
	}
}
