package dataplane

import "testing"

func TestRequireUpsertKey(t *testing.T) {
	// Upsert with a key: fine.
	if err := RequireUpsertKey("raw.orders", []string{"id"}, UpsertMode); err != nil {
		t.Fatalf("upsert with a key must pass: %v", err)
	}
	// Upsert without a key: the equality delete would carry no columns.
	if err := RequireUpsertKey("raw.orders", nil, UpsertMode); err == nil {
		t.Fatal("upsert without a key must fail")
	}
	// Append without a key: allowed (the log has no equality key).
	if err := RequireUpsertKey("raw.events", nil, AppendMode); err != nil {
		t.Fatalf("append without a key must pass: %v", err)
	}
}
