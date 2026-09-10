package transport

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

func encodeFixture() ([]rowchange.Change, core.Schema) {
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindFloat64, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
	ts := time.Unix(1_700_000_000, 0).UTC()
	rows := []rowchange.Change{
		{Op: rowchange.OpInsert, After: map[string]any{"id": int64(1), "v": "a", "amount": 1.5}, Position: "p1", CommitTS: ts},
		{Op: rowchange.OpUpdate, After: map[string]any{"id": int64(2), "v": "b"}, Position: "p2", IngestTS: ts},
		{Op: rowchange.OpDelete, Key: []any{int64(3)}, Position: "p3"},
	}
	return rows, cs
}

// TestRecordFromChangesMatchesEncodeBatch — the extracted primitive plus an
// IPC wrap must be byte-identical to the pre-refactor EncodeBatch output.
func TestRecordFromChangesMatchesEncodeBatch(t *testing.T) {
	rows, cs := encodeFixture()
	alloc := memory.NewGoAllocator()

	body, _, err := EncodeBatch(rows, cs, &pb.BatchMeta{Table: "t"}, alloc)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	rec, err := RecordFromChanges(rows, cs, alloc)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	defer rec.Release()
	direct, err := recordToIPC(rec)
	if err != nil {
		t.Fatalf("recordToIPC: %v", err)
	}

	if !bytes.Equal(body, direct) {
		t.Fatalf("EncodeBatch body (%d bytes) != RecordFromChanges+wrap (%d bytes)", len(body), len(direct))
	}
}

// TestRecordFromChangesDecodes — the record round-trips through DecodeBatch
// without the IPC hop.
func TestRecordFromChangesDecodes(t *testing.T) {
	rows, cs := encodeFixture()
	rec, err := RecordFromChanges(rows, cs, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges: %v", err)
	}
	defer rec.Release()

	got, err := DecodeBatch(rec, "t", cs.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3", len(got))
	}
	if got[0].After["id"] != int64(1) || got[0].After["v"] != "a" || got[0].After["amount"] != 1.5 {
		t.Fatalf("row 0 wrong: %v", got[0].After)
	}
	if got[2].Op != rowchange.OpDelete || got[2].Key[0] != int64(3) {
		t.Fatalf("row 2 delete/key wrong: op=%v key=%v", got[2].Op, got[2].Key)
	}
	// The key-only delete backfills its PK column from the tuple.
	if got[2].After["id"] != int64(3) {
		t.Fatalf("row 2 After[id] = %v, want 3 (key backfill)", got[2].After["id"])
	}
}

// TestMergeSchemaGrowsNeverShrinks — MergeSchema keeps every known column
// and appends only the inferred ones the known schema lacks, preserving PK.
func TestMergeSchemaGrowsNeverShrinks(t *testing.T) {
	known := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
	changes := []rowchange.Change{
		{After: map[string]any{"id": int64(1), "v": "x", "users.name": "ana"}},
	}
	merged := MergeSchema(changes, known)
	if len(merged.Columns) != 3 {
		t.Fatalf("columns = %d, want 3 (id, v, users.name)", len(merged.Columns))
	}
	if merged.Columns[0].Name != "id" || merged.Columns[1].Name != "v" || merged.Columns[2].Name != "users.name" {
		t.Fatalf("column order/identity wrong: %v", merged.Columns)
	}
	if !merged.Columns[2].Type.Nullable {
		t.Fatal("inferred join-output column must be nullable")
	}
	if len(merged.PrimaryKey) != 1 || merged.PrimaryKey[0] != "id" {
		t.Fatalf("PK lost: %v", merged.PrimaryKey)
	}

	// Empty known schema → full inference.
	inferred := MergeSchema(changes, core.Schema{})
	if len(inferred.Columns) != 3 {
		t.Fatalf("inferred columns = %d, want 3", len(inferred.Columns))
	}
}

func TestRecordFromChangesEmpty(t *testing.T) {
	_, cs := encodeFixture()
	rec, err := RecordFromChanges(nil, cs, nil)
	if err != nil {
		t.Fatalf("RecordFromChanges(nil): %v", err)
	}
	defer rec.Release()
	if rec.NumRows() != 0 {
		t.Fatalf("rows = %d, want 0", rec.NumRows())
	}
	// Still a valid wire schema.
	if _, err := NewBatchReader(rec, cs.PrimaryKey); err != nil {
		t.Fatalf("empty record is not wire-schema: %v", err)
	}
}
