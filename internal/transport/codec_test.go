package transport

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/maltzsama/urutau/internal/change"
	"github.com/maltzsama/urutau/internal/core"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
)

func TestCodecRoundTrip(t *testing.T) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindFloat64}},
			{Name: "active", Type: core.ColumnType{Kind: core.KindBool}},
		},
		PrimaryKey: []string{"id"},
	}

	rows := []change.Change{
		{
			Op: change.OpInsert, Table: "raw.orders",
			After:    map[string]any{"id": int64(42), "v": "hello", "amount": 1.5, "active": true},
			Position: "0/1A",
		},
		{
			Op: change.OpUpdate, Table: "raw.orders",
			After:    map[string]any{"id": int64(7), "v": "new"},
			Position: "0/1B",
		},
		{
			Op: change.OpDelete, Table: "raw.orders",
			After:    map[string]any{"id": int64(2), "v": nil},
			Position: "0/1C",
		},
	}

	meta := &pb.BatchMeta{
		Table: "raw.orders", LowPos: "0/1A", HighPos: "0/1C",
		BatchId: 9, Epoch: 1,
		Window: &pb.WindowTag{ChunkId: 3, Snapshot: true},
	}

	body, metaBytes, err := EncodeBatch(rows, schema, meta)
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
		want := rows[i]
		gotRow := got[i]
		if gotRow.Op != want.Op || gotRow.Position != want.Position || gotRow.Table != want.Table {
			t.Errorf("row %d: op/pos/table mismatch: %+v", i, gotRow)
		}
		for k, v := range want.After {
			if v == nil {
				continue // nil values are omitted from After
			}
			if gotRow.After[k] != v {
				t.Errorf("row %d: after[%s] = %v (%T), want %v (%T)", i, k, gotRow.After[k], gotRow.After[k], v, v)
			}
		}
	}
	// Typed wire format: int64 stays int64, float64 stays float64.
	if got[0].After["id"] != int64(42) {
		t.Errorf("after.id = %T %v, want int64 42", got[0].After["id"], got[0].After["id"])
	}
	if got[0].After["amount"] != 1.5 {
		t.Errorf("after.amount = %v (%T), want float64 1.5", got[0].After["amount"], got[0].After["amount"])
	}
	if got[0].After["active"] != true {
		t.Errorf("after.active = %v, want true", got[0].After["active"])
	}
}

func TestCodecLargeInt64(t *testing.T) {
	// Regression test: int64 above 2^53 must survive the wire format exactly.
	// This fails with JSON encoding where float64 precision is lost.
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		},
		PrimaryKey: []string{"id"},
	}

	bigID := int64(9007199254740993) // 2^53 + 1
	rows := []change.Change{
		{
			Op: change.OpInsert, Table: "t",
			After:    map[string]any{"id": bigID},
			Position: "p1",
		},
	}
	meta := &pb.BatchMeta{Table: "t", LowPos: "p1", HighPos: "p1"}

	body, metaBytes, err := EncodeBatch(rows, schema, meta)
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

	got, _, err := DecodeBatch(rec, metaBytes)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got[0].After["id"] != bigID {
		t.Errorf("after.id = %v, want %d (precision loss!)", got[0].After["id"], bigID)
	}
}

func TestCodecDecimal(t *testing.T) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "price", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 10, Scale: 2}},
		},
		PrimaryKey: []string{},
	}

	rows := []change.Change{
		{
			Op: change.OpInsert, Table: "t",
			After:    map[string]any{"price": "12345678.90"},
			Position: "p1",
		},
	}
	meta := &pb.BatchMeta{Table: "t", LowPos: "p1", HighPos: "p1"}

	body, metaBytes, err := EncodeBatch(rows, schema, meta)
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

	got, _, err := DecodeBatch(rec, metaBytes)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Decimal values come back as their canonical text form.
	if got[0].After["price"] == nil {
		t.Fatal("after.price is nil")
	}
	t.Logf("decimal value: %v (%T)", got[0].After["price"], got[0].After["price"])
}
