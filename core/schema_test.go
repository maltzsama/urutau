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

func TestSchemaValidate(t *testing.T) {
	ok := Schema{
		Columns:    []Column{{Name: "id", Type: ColumnType{Kind: KindInt64}}, {Name: "v", Type: ColumnType{Kind: KindString}}},
		PrimaryKey: []string{"id"},
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid schema rejected: %v", err)
	}
	if err := (Schema{}).Validate(); err != nil {
		t.Fatalf("empty schema must be valid: %v", err)
	}
	dup := Schema{Columns: []Column{{Name: "a", Type: ColumnType{Kind: KindString}}, {Name: "a", Type: ColumnType{Kind: KindString}}}}
	if err := dup.Validate(); err == nil {
		t.Error("duplicate column must error")
	}
	noName := Schema{Columns: []Column{{Name: "", Type: ColumnType{Kind: KindString}}}}
	if err := noName.Validate(); err == nil {
		t.Error("empty column name must error")
	}
	badPK := Schema{Columns: []Column{{Name: "a", Type: ColumnType{Kind: KindString}}}, PrimaryKey: []string{"a", "a"}}
	if err := badPK.Validate(); err == nil {
		t.Error("duplicate PK entry must error")
	}
}

func TestColumnTypeStringOpaque(t *testing.T) {
	ct := ColumnType{Kind: KindUnknown, Opaque: &OpaqueOrigin{TypeName: "point", VendorName: "mysql"}}
	if got := ct.String(); got != "unknown (mysql point)" {
		t.Fatalf("opaque = %q", got)
	}
	if got := (ColumnType{Kind: KindUnknown}).String(); got != "unknown" {
		t.Fatalf("bare unknown = %q", got)
	}
}
