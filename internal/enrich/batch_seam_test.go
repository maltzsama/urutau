package enrich

// RV-01: the enriched batch's re-encode bridge carries the table's primary
// key. Without it, the bridge C-8 fail-fast rejects ANY enriched batch
// containing a delete — SchemaFromArrow reconstructs columns only, the PK
// must ride from the Assignment (the worker passes knownSchema.PrimaryKey).

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/maltzsama/urutau/core"
	dpint "github.com/maltzsama/urutau/internal/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
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

func fieldIdxByName(s *arrow.Schema, name string) int {
	for i, f := range s.Fields() {
		if f.Name == name {
			return i
		}
	}
	return -1
}

// E-6: with the reference columns declared on the wire (RefColumns), batch 1
// (cold/empty reference — everything misses) and batch 2 (reference hot —
// everything hits) carry the SAME wire schema, and a miss writes the columns
// as NULL instead of leaving them out — no schema drift between batches of
// the same stream.
func TestEnrichBatchSameSchemaAcrossEmptyAndHotReference(t *testing.T) {
	cfg := refCfg(func(c *spec.Enrich) {
		c.OnColdStart = "pass"
	})
	s, err := New([]spec.Enrich{cfg}, []string{"id", "user_ref", "q"}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	refDests := s.RefColumns()
	if len(refDests) != 2 || refDests[0] != "users.name" || refDests[1] != "users.tier" {
		t.Fatalf("RefColumns = %v, want [users.name users.tier]", refDests)
	}

	// The caller (runner/worker) extends the table's wire schema with
	// RefColumns before the pipeline starts — simulated here with the same
	// registered shape (nullable strings).
	wire := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "q", Type: core.ColumnType{Kind: core.KindString}},
	}}
	for _, dest := range refDests {
		wire.Columns = append(wire.Columns, core.Column{
			Name: dest,
			Type: core.ColumnType{Kind: core.KindString, Nullable: true},
		})
	}

	mkBatch := func(t *testing.T, id int64) *dpint.Batch {
		t.Helper()
		cb := rowchange.Batch{
			Table: "events",
			Changes: []rowchange.Change{
				{Op: rowchange.OpInsert, Key: []any{id},
					After: map[string]any{"id": id, "user_ref": int64(1), "q": "x"}, Position: "p"},
			},
			Mode: rowchange.UpsertMode,
		}
		b, err := dpint.BatchFromChangeBatch(cb, wire)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return b
	}

	loader := &fakeLoader{} // first image: empty
	if err := s.UseLoader("users", loader); err != nil {
		t.Fatal(err)
	}

	// Batch 1: cold reference (never loaded) — everything misses, but the
	// reference columns are written as NULL instead of left out.
	b1 := mkBatch(t, 1)
	defer b1.Release()
	out1, err := s.EnrichBatch(b1, []string{"id"})
	if err != nil {
		t.Fatalf("enrich batch 1: %v", err)
	}
	if out1 == nil {
		t.Fatal("batch 1 vanished")
	}
	defer out1.Release()
	sch1 := out1.Record.Schema()

	// The reference goes hot with rows.
	loader.SetRows(usersRows())
	s.Start(context.Background())
	defer s.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !s.refs[0].isHot() {
		time.Sleep(time.Millisecond)
	}
	if !s.refs[0].isHot() {
		t.Fatal("reference did not go hot")
	}

	// Batch 2: hits.
	b2 := mkBatch(t, 2)
	defer b2.Release()
	out2, err := s.EnrichBatch(b2, []string{"id"})
	if err != nil {
		t.Fatalf("enrich batch 2: %v", err)
	}
	defer out2.Release()
	sch2 := out2.Record.Schema()

	if !sch1.Equal(sch2) {
		t.Fatalf("schemas diverge between batch 1 and batch 2:\n%v\n%v", sch1, sch2)
	}
	for _, dest := range refDests {
		i1 := fieldIdxByName(sch1, dest)
		if i1 < 0 {
			t.Fatalf("batch 1 lacks %q", dest)
		}
		if !out1.Record.Column(i1).IsNull(0) {
			t.Fatalf("batch 1 %s should be NULL on a miss", dest)
		}
		i2 := fieldIdxByName(sch2, dest)
		if out2.Record.Column(i2).IsNull(0) {
			t.Fatalf("batch 2 %s should carry the hit value", dest)
		}
	}
	name := out2.Record.Column(fieldIdxByName(sch2, "users.name")).(*array.String)
	if name.Value(0) != "ana" {
		t.Fatalf("users.name = %q, want ana", name.Value(0))
	}
}
