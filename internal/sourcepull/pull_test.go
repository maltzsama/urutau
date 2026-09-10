package sourcepull

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
)

// TestMakeBatchColumnar — the puller builds a wire-schema RecordBatch
// directly (no bridge). The batch decodes back to the buffered changes.
func TestMakeBatchColumnar(t *testing.T) {
	p := New(make(chan rowchange.Change))
	p.SetSchemas(map[string]core.Schema{
		"t": {
			Columns: []core.Column{
				{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
				{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			},
			PrimaryKey: []string{"id"},
		},
	})
	ch := make(chan rowchange.Change, 3)
	ch <- rowchange.Change{Op: rowchange.OpInsert, Table: "t", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "a"}, Position: "p1"}
	ch <- rowchange.Change{Op: rowchange.OpUpdate, Table: "t", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "v": "b"}, Position: "p2"}
	ch <- rowchange.Change{Op: rowchange.OpDelete, Table: "t", Key: []any{int64(2)}, Position: "p3"}
	close(ch)
	p.ch = ch

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	defer b.Release()

	rows, err := transport.DecodeBatch(b.Record, "t", []string{"id"})
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	if rows[1].After["v"] != "b" || rows[1].Op != rowchange.OpUpdate {
		t.Fatalf("row 1 = %+v", rows[1])
	}
	if rows[2].Op != rowchange.OpDelete || rows[2].Key[0] != int64(2) {
		t.Fatalf("row 2 delete/key = %v %v", rows[2].Op, rows[2].Key)
	}
	// The key-only delete backfills its PK column.
	if rows[2].After["id"] != int64(2) {
		t.Fatalf("row 2 After[id] = %v, want 2", rows[2].After["id"])
	}
}

// TestMakeBatchDeleteWithoutPK — a delete on a schema with no primary key
// is rejected (was the bridge C-8 guard).
func TestMakeBatchDeleteWithoutPK(t *testing.T) {
	p := New(make(chan rowchange.Change))
	// No SetSchemas → inferred schema has no PrimaryKey.
	ch := make(chan rowchange.Change, 1)
	ch <- rowchange.Change{Op: rowchange.OpDelete, Table: "t", Key: []any{int64(1)}, Position: "p1"}
	close(ch)
	p.ch = ch

	if _, err := p.Next(context.Background()); err == nil {
		t.Fatal("delete without a primary key must be rejected")
	}
}
