package core

import "testing"

// An unknown metadata key (typo in the spec) must fail loudly — the catalog
// is closed by design and a silent string column would carry NULLs forever.
func TestResolveSchemaRejectsUnknownMetadataKey(t *testing.T) {
	src := Schema{Columns: []Column{{Name: "id", Type: ColumnType{Kind: KindInt64}}}}
	meta := []MetadataColumn{{From: "comit_ts", As: "commit_ts"}} // typo
	if _, _, err := ResolveSchema(src, CastPolicy{}, meta); err == nil {
		t.Error("ResolveSchema with an unknown metadata key should error")
	}
}

// An empty metadata destination name would create a column with no name.
func TestResolveSchemaRejectsEmptyAs(t *testing.T) {
	src := Schema{Columns: []Column{{Name: "id", Type: ColumnType{Kind: KindInt64}}}}
	meta := []MetadataColumn{{From: MetaOp, As: ""}}
	if _, _, err := ResolveSchema(src, CastPolicy{}, meta); err == nil {
		t.Error("ResolveSchema with an empty metadata destination should error")
	}
}
