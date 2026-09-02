package transport

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/maltzsama/urutau/internal/change"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

func TestCodecRoundTrip(t *testing.T) {
	rows := []change.Change{
		{
			Op: change.OpInsert, Table: "raw.orders",
			Key: []any{int64(42)}, After: map[string]any{"id": int64(42), "v": "hello", "amount": 1.5, "active": true},
			Position: "0/1A",
		},
		{
			Op: change.OpUpdate, Table: "raw.orders",
			Key:      []any{"s", 1.25},
			Before:   map[string]any{"id": int64(7), "v": "old"},
			After:    map[string]any{"id": int64(7), "v": "new"},
			Position: "0/1B",
		},
		{
			Op: change.OpDelete, Table: "raw.orders",
			Key: []any{int64(2)}, Before: map[string]any{"id": int64(2), "v": nil},
			Position: "0/1C",
		},
	}

	meta := &pb.BatchMeta{
		Table: "raw.orders", LowPos: "0/1A", HighPos: "0/1C",
		BatchId: 9, Epoch: 1,
		Window: &pb.WindowTag{ChunkId: 3, Snapshot: true},
	}

	body, metaBytes, err := EncodeBatch(rows, meta)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	r, err := ipc.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("ipc reader: %v", err)
	}
	defer r.Release()
	rec, err := r.Read()
	if err != nil {
		t.Fatalf("ipc read: %v", err)
	}
	defer rec.Release()

	got, gotMeta, err := DecodeBatch(rec, metaBytes)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotMeta.Table != "raw.orders" || gotMeta.BatchId != 9 || !gotMeta.Window.Snapshot || gotMeta.Window.ChunkId != 3 {
		t.Fatalf("meta mismatch: %+v", gotMeta)
	}
	if len(got) != len(rows) {
		t.Fatalf("rows = %d, want %d", len(got), len(rows))
	}
	for i := range rows {
		want, gotRow := rows[i], got[i]
		if gotRow.Op != want.Op || gotRow.Position != want.Position || gotRow.Table != want.Table {
			t.Errorf("row %d: op/pos/table mismatch: %+v", i, gotRow)
		}
		if keyStringJSON(want.Key) != keyStringJSON(gotRow.Key) {
			t.Errorf("row %d: key %v ≠ %v", i, want.Key, gotRow.Key)
		}
		for k, v := range want.After {
			if gotRow.After[k] != v {
				t.Errorf("row %d: after[%s] = %v (%T), want %v (%T)", i, k, gotRow.After[k], gotRow.After[k], v, v)
			}
		}
		if want.Before != nil {
			for k, v := range want.Before {
				if v != nil && gotRow.Before[k] != v {
					t.Errorf("row %d: before[%s] = %v, want %v", i, k, gotRow.Before[k], v)
				}
			}
		}
	}
	// Integral JSON numbers must decode as int64, not float64.
	if got[0].After["id"] != int64(42) {
		t.Errorf("after.id = %T, want int64", got[0].After["id"])
	}
	if got[0].After["amount"] != 1.5 {
		t.Errorf("after.amount = %v, want float64 1.5 (decimals stay float)", got[0].After["amount"])
	}
	if got[0].After["active"] != true {
		t.Errorf("after.active = %v, want true", got[0].After["active"])
	}
}

// keyStringJSON is a comparable rendering of a key tuple.
func keyStringJSON(k []any) string {
	b, _ := json.Marshal(k)
	return string(b)
}
