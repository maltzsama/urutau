package core

import "testing"

// TestKeyIndexesResolves resolves the PK to column positions in key order.
func TestKeyIndexesResolves(t *testing.T) {
	s := Schema{
		Columns:    []Column{{Name: "id", Type: ColumnType{Kind: KindInt64}}, {Name: "v", Type: ColumnType{Kind: KindString}}},
		PrimaryKey: []string{"id"},
	}
	idx, err := s.KeyIndexes()
	if err != nil {
		t.Fatalf("key indexes: %v", err)
	}
	if len(idx) != 1 || idx[0] != 0 {
		t.Fatalf("indexes = %v, want [0]", idx)
	}
}

// TestKeyIndexesMissingColumn errors when the PK names a column that is not
// in the schema — no silent nil-key writes.
func TestKeyIndexesMissingColumn(t *testing.T) {
	s := Schema{Columns: []Column{{Name: "id", Type: ColumnType{Kind: KindInt64}}}, PrimaryKey: []string{"nope"}}
	if _, err := s.KeyIndexes(); err == nil {
		t.Fatal("missing PK column did not error")
	}
}

// TestColumnTypeString renders kinds for diagnostics.
func TestColumnTypeString(t *testing.T) {
	if got := (ColumnType{Kind: KindInt64}).String(); got != "int64" {
		t.Fatalf("int64 = %q", got)
	}
	if got := (ColumnType{Kind: KindDecimal, Precision: 20, Scale: 2}).String(); got != "decimal(20,2)" {
		t.Fatalf("decimal = %q", got)
	}
}
