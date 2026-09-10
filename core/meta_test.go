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

// The spec loader round-trips YAML through JSON, so an unknown key must fail
// at parse time via UnmarshalJSON, not only at ResolveSchema.
func TestMetadataKeyUnmarshalJSON(t *testing.T) {
	var k MetadataKey
	if err := k.UnmarshalJSON([]byte(`"commit_ts"`)); err != nil || k != MetaCommitTS {
		t.Fatalf("valid key: k=%q err=%v", k, err)
	}
	if err := k.UnmarshalJSON([]byte(`"comit_ts"`)); err == nil {
		t.Error("unknown key must fail UnmarshalJSON")
	}
}

// Every metadata column is nullable; the type is the shape only.
func TestMetadataKeyColumnTypeNullable(t *testing.T) {
	for _, k := range []MetadataKey{MetaOp, MetaCommitTS, MetaEnrichMiss, MetaStream} {
		if ct := k.ColumnType(); !ct.Nullable {
			t.Errorf("%s.ColumnType() must be nullable, got %+v", k, ct)
		}
	}
}
