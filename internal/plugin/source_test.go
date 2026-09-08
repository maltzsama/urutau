package plugin

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/maltzsama/urutau/internal/rowchange"
)

func TestStringPositionCompare(t *testing.T) {
	tests := []struct {
		name     string
		a, b     StringPosition
		expected int
	}{
		{"equal", StringPosition{Offset: "abc"}, StringPosition{Offset: "abc"}, 0},
		{"less", StringPosition{Offset: "aaa"}, StringPosition{Offset: "bbb"}, -1},
		{"greater", StringPosition{Offset: "ccc"}, StringPosition{Offset: "bbb"}, 1},
		{"empty", StringPosition{}, StringPosition{}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.expected {
				t.Errorf("Compare() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestStringPositionContains(t *testing.T) {
	a := StringPosition{Offset: "abc"}
	b := StringPosition{Offset: "abc"}
	if !a.Contains(b) {
		t.Error("should contain equal position")
	}

	c := StringPosition{Offset: "aaa"}
	if a.Contains(c) {
		t.Error("should not contain lesser position")
	}
}

func TestStringPositionString(t *testing.T) {
	p := StringPosition{Offset: "test123"}
	if p.String() != "test123" {
		t.Errorf("String() = %q, want %q", p.String(), "test123")
	}
}

func TestArrowToCoreSchema(t *testing.T) {
	arrowSchema := mustArrowSchema(t)
	coreSchema := arrowToCoreSchema(arrowSchema)

	if len(coreSchema.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(coreSchema.Columns))
	}

	names := []string{"id", "name", "active"}
	for i, name := range names {
		if coreSchema.Columns[i].Name != name {
			t.Errorf("column %d: got name %q, want %q", i, coreSchema.Columns[i].Name, name)
		}
	}

	if coreSchema.Columns[0].Type.Kind.String() != "int64" {
		t.Errorf("id kind: got %q, want int64", coreSchema.Columns[0].Type.Kind)
	}
	if coreSchema.Columns[1].Type.Kind.String() != "string" {
		t.Errorf("name kind: got %q, want string", coreSchema.Columns[1].Type.Kind)
	}
	if coreSchema.Columns[2].Type.Kind.String() != "bool" {
		t.Errorf("active kind: got %q, want bool", coreSchema.Columns[2].Type.Kind)
	}
}

func TestColumnIndex(t *testing.T) {
	schema := mustArrowSchema(t)

	if got := columnIndex(schema, "name"); got != 1 {
		t.Errorf("columnIndex(name) = %d, want 1", got)
	}
	if got := columnIndex(schema, "nonexistent"); got != -1 {
		t.Errorf("columnIndex(nonexistent) = %d, want -1", got)
	}
}

func TestExtractPK(t *testing.T) {
	row := map[string]any{"id": int64(42), "name": "test"}
	pk := extractPK([]string{"id"}, row)
	if len(pk) != 1 || pk[0] != int64(42) {
		t.Errorf("extractPK = %v, want [42]", pk)
	}

	row2 := map[string]any{"name": "test"}
	pk2 := extractPK([]string{"id"}, row2)
	if pk2 != nil {
		t.Errorf("extractPK missing col = %v, want nil", pk2)
	}

	pk3 := extractPK([]string{"id"}, nil)
	if pk3 != nil {
		t.Errorf("extractPK nil row = %v, want nil", pk3)
	}
}

func TestCollectColumns(t *testing.T) {
	b := batchWithColumns(t, "a", "b", "c")
	cols := collectColumns(b)
	if len(cols) != 3 {
		t.Errorf("collectColumns = %v, want 3 columns", cols)
	}
}

func TestBatchToRecords(t *testing.T) {
	alloc := memory.NewGoAllocator()
	b := rowchange.Batch{
		Table:    "test",
		Position: "pos123",
		Upserts: []rowchange.Change{
			{Op: rowchange.OpInsert, Table: "test", Key: []any{int64(1)}, After: map[string]any{"id": int64(1), "name": "alice"}},
			{Op: rowchange.OpUpdate, Table: "test", Key: []any{int64(2)}, After: map[string]any{"id": int64(2), "name": "bob"}},
		},
		Deletes: []rowchange.Change{
			{Op: rowchange.OpDelete, Table: "test", Key: []any{int64(3)}, Before: map[string]any{"id": int64(3), "name": "charlie"}},
		},
	}

	records := batchToRecords(b, alloc)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	defer records[0].Release()

	if records[0].NumRows() != 3 {
		t.Errorf("expected 3 rows, got %d", records[0].NumRows())
	}
	if records[0].NumCols() != 5 { // op, id, name, offset, ts_source
		t.Errorf("expected 5 cols, got %d", records[0].NumCols())
	}
}

func mustArrowSchema(t *testing.T) *arrow.Schema {
	t.Helper()
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
}

func batchWithColumns(t *testing.T, cols ...string) rowchange.Batch {
	t.Helper()
	b := rowchange.Batch{Table: "test"}
	for _, c := range cols {
		b.Upserts = append(b.Upserts, rowchange.Change{
			After: map[string]any{c: "val"},
		})
	}
	return b
}
