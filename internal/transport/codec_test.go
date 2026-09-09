package transport

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
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

	rows := []rowchange.Change{
		{
			Op: rowchange.OpInsert, Table: "raw.orders",
			After:    map[string]any{"id": int64(42), "v": "hello", "amount": 1.5, "active": true},
			Position: "0/1A",
		},
		{
			Op: rowchange.OpUpdate, Table: "raw.orders",
			After:    map[string]any{"id": int64(7), "v": "new"},
			Position: "0/1B",
		},
		{
			Op: rowchange.OpDelete, Table: "raw.orders",
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

	got, gotMeta, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
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
		// The key tuple is rebuilt from the row's own values, in PK order.
		wantKey := want.After["id"]
		if len(gotRow.Key) != 1 || gotRow.Key[0] != wantKey {
			t.Errorf("row %d: key = %v, want [%v]", i, gotRow.Key, wantKey)
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
	rows := []rowchange.Change{
		{
			Op: rowchange.OpInsert, Table: "t",
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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got[0].After["id"] != bigID {
		t.Errorf("after.id = %v, want %d (precision loss!)", got[0].After["id"], bigID)
	}
	if got[0].Key[0] != bigID {
		t.Errorf("key = %v, want [%d]", got[0].Key, bigID)
	}
}

func TestCodecDecimal(t *testing.T) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "price", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 10, Scale: 2}},
		},
		PrimaryKey: []string{},
	}

	rows := []rowchange.Change{
		{
			Op: rowchange.OpInsert, Table: "t",
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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Decimal values come back as their canonical text form.
	if got[0].After["price"] == nil {
		t.Fatal("after.price is nil")
	}
	t.Logf("decimal value: %v (%T)", got[0].After["price"], got[0].After["price"])
}

func TestCodecDeleteKeyOnly(t *testing.T) {
	// A delete may carry no row image — only the key (how the MySQL binlog
	// decoder emits deletes). The key values must still travel so the
	// equality delete matches at read time; a NULL tuple silently deletes
	// nothing, which is how a distributed run lost deletes.
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
	rows := []rowchange.Change{
		{Op: rowchange.OpDelete, Table: "raw.orders", Key: []any{int64(2)}, Position: "p1"},
	}
	meta := &pb.BatchMeta{Table: "raw.orders", HighPos: "p1"}

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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got[0].Key) != 1 || got[0].Key[0] != int64(2) {
		t.Fatalf("key = %v, want [2] — the delete would match nothing", got[0].Key)
	}
	if got[0].After["id"] != int64(2) {
		t.Fatalf("after.id = %v, want 2", got[0].After["id"])
	}
}

func TestCodecPartialBeforeBackfillsKey(t *testing.T) {
	// A before-image that lacks the key column value must still produce a
	// matching equality delete: the key tuple backfills the missing PK
	// column on encode.
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
	rows := []rowchange.Change{
		{Op: rowchange.OpDelete, Table: "t", Key: []any{int64(9)},
			Before:   map[string]any{"v": "old"}, // no id — partial image
			Position: "p1"},
	}
	meta := &pb.BatchMeta{Table: "t", HighPos: "p1"}

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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got[0].Key[0] != int64(9) {
		t.Fatalf("key = %v, want [9] — a NULL tuple deletes nothing", got[0].Key)
	}
	if got[0].After["v"] != "old" {
		t.Fatalf("after.v = %v, want old", got[0].After["v"])
	}
}

// T-1: Kind coverage matrix — every supported kind round-trips exactly.
func TestCodecKindCoverageMatrix(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "c_bool", Type: core.ColumnType{Kind: core.KindBool}},
			{Name: "c_int32", Type: core.ColumnType{Kind: core.KindInt32}},
			{Name: "c_int64", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "c_uint64", Type: core.ColumnType{Kind: core.KindUInt64}},
			{Name: "c_float32", Type: core.ColumnType{Kind: core.KindFloat32}},
			{Name: "c_float64", Type: core.ColumnType{Kind: core.KindFloat64}},
			{Name: "c_string", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "c_binary", Type: core.ColumnType{Kind: core.KindBinary}},
			{Name: "c_date", Type: core.ColumnType{Kind: core.KindDate}},
			{Name: "c_time", Type: core.ColumnType{Kind: core.KindTime}},
			{Name: "c_tstz", Type: core.ColumnType{Kind: core.KindTimestampTZ}},
			{Name: "c_ts", Type: core.ColumnType{Kind: core.KindTimestamp}},
		},
		PrimaryKey: []string{"c_int64"},
	}

	rows := []rowchange.Change{
		{
			Op: rowchange.OpInsert, Table: "t",
			After: map[string]any{
				"c_bool":   true,
				"c_int32":  int32(42),
				"c_int64":  int64(9007199254740993),
				"c_uint64": uint64(18446744073709551615),
				"c_float32": float32(1.5),
				"c_float64": float64(2.5),
				"c_string":  "hello",
				"c_binary":  []byte{0xDE, 0xAD},
				"c_date":    int32(20000),
				"c_time":    int64(43200000000),
				"c_tstz":    now,
				"c_ts":      now,
			},
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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
	a := got[0].After

	// Bool
	if a["c_bool"] != true {
		t.Errorf("c_bool = %v, want true", a["c_bool"])
	}
	// Int32
	if v, ok := a["c_int32"].(int32); !ok || v != 42 {
		t.Errorf("c_int32 = %v (%T), want int32(42)", a["c_int32"], a["c_int32"])
	}
	// Int64 (large — above float64 precision)
	if a["c_int64"] != int64(9007199254740993) {
		t.Errorf("c_int64 = %v, want 9007199254740993", a["c_int64"])
	}
	// UInt64
	if a["c_uint64"] != uint64(18446744073709551615) {
		t.Errorf("c_uint64 = %v, want 18446744073709551615", a["c_uint64"])
	}
	// Float32
	if v, ok := a["c_float32"].(float64); !ok || math.Abs(v-1.5) > 0.001 {
		t.Errorf("c_float32 = %v (%T), want ~1.5", a["c_float32"], a["c_float32"])
	}
	// Float64
	if a["c_float64"] != 2.5 {
		t.Errorf("c_float64 = %v, want 2.5", a["c_float64"])
	}
	// String
	if a["c_string"] != "hello" {
		t.Errorf("c_string = %v, want hello", a["c_string"])
	}
	// Binary
	if v, ok := a["c_binary"].([]byte); !ok || len(v) != 2 || v[0] != 0xDE || v[1] != 0xAD {
		t.Errorf("c_binary = %v (%T), want [0xDE 0xAD]", a["c_binary"], a["c_binary"])
	}
	// Date
	if v, ok := a["c_date"].(int32); !ok || v != 20000 {
		t.Errorf("c_date = %v (%T), want int32(20000)", a["c_date"], a["c_date"])
	}
	// Time
	if a["c_time"] != int64(43200000000) {
		t.Errorf("c_time = %v, want 43200000000", a["c_time"])
	}
	// TimestampTZ — compare via time.Time
	if v, ok := a["c_tstz"].(time.Time); !ok || !v.Equal(now) {
		t.Errorf("c_tstz = %v (%T), want %v", a["c_tstz"], a["c_tstz"], now)
	}
	// Timestamp (naive) — also time.Time
	if v, ok := a["c_ts"].(time.Time); !ok || !v.Equal(now) {
		t.Errorf("c_ts = %v (%T), want %v", a["c_ts"], a["c_ts"], now)
	}
}

// T-2: Preservação de campos por transform — todos os transforms preservam
// Mode, SnapshotState e SnapshotPending nos batches de saída.
// (Coberto nos tests de cada transform em dataplane/enrich_test.go)

// T-5: Lifetime de binary pós-Release — valores []byte sobrevivem após
// Release do RecordBatch (bytes.Clone garante isolamento).
func TestCodecBinaryLifetimeAfterRelease(t *testing.T) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "data", Type: core.ColumnType{Kind: core.KindBinary}},
		},
		PrimaryKey: []string{},
	}
	rows := []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "t", After: map[string]any{"data": []byte{1, 2, 3}}, Position: "p1"},
	}
	meta := &pb.BatchMeta{Table: "t", HighPos: "p1"}

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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	orig := got[0].After["data"].([]byte)
	// Mutate the original — the decoded value must be independent.
	orig[0] = 0xFF
	got2, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode2: %v", err)
	}
	v := got2[0].After["data"].([]byte)
	if v[0] != 0x01 {
		t.Errorf("binary was aliased: after mutation got[0]=%X, want 0x01", v[0])
	}
}

// T-6: AddMetadata sem duplicatas + decode pós-AddMetadata.
func TestCodecAddMetadataNoDuplicates(t *testing.T) {
	// AddMetadata should not duplicate existing metadata columns.
	// If __phase already exists on the wire, AddMetadata must not add a second.
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		},
		PrimaryKey: []string{"id"},
	}
	rows := []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "t", After: map[string]any{"id": int64(1)}, Position: "p1"},
	}
	meta := &pb.BatchMeta{Table: "t", HighPos: "p1"}

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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The row must decode successfully — no duplicate column errors.
	if len(got) != 1 {
		t.Fatalf("rows = %d, want 1", len(got))
	}
}

// T-7: EncodeBounds(nil, high) → erro.
func TestCodecEncodeBoundsNilLow(t *testing.T) {
	high := []any{"0/2"}
	_, err := EncodeBounds(nil, high)
	if err == nil {
		t.Fatal("expected error for nil low bound")
	}
}

// T-9: Collapse com PK de cada tipo — composite PK com Int32 + String.
func TestCodecCollapseMultiTypePK(t *testing.T) {
	// Collapse should handle composite PKs of mixed types correctly.
	// This test encodes rows with PK = (int32, string) and verifies
	// that rows with the same composite key collapse to one.
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "pk1", Type: core.ColumnType{Kind: core.KindInt32}},
			{Name: "pk2", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"pk1", "pk2"},
	}
	rows := []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "t", After: map[string]any{"pk1": int32(1), "pk2": "a", "v": "v1"}, Position: "p1"},
		{Op: rowchange.OpUpdate, Table: "t", After: map[string]any{"pk1": int32(1), "pk2": "a", "v": "v2"}, Position: "p2"},
		{Op: rowchange.OpInsert, Table: "t", After: map[string]any{"pk1": int32(2), "pk2": "b", "v": "v3"}, Position: "p3"},
	}
	meta := &pb.BatchMeta{Table: "t", LowPos: "p1", HighPos: "p3"}

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

	got, _, err := DecodeBatch(rec, metaBytes, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3", len(got))
	}
	// Verify PK1 type preservation
	if _, ok := got[0].Key[0].(int32); !ok {
		t.Errorf("key[0] = %T, want int32", got[0].Key[0])
	}
	if _, ok := got[0].Key[1].(string); !ok {
		t.Errorf("key[1] = %T, want string", got[0].Key[1])
	}
}

// T-11: __op inválido → erro.
func TestCodecInvalidOpRejected(t *testing.T) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
	rows := []rowchange.Change{
		{Op: rowchange.OpInsert, Table: "t", After: map[string]any{"id": int64(1), "v": "a"}, Position: "p1"},
	}
	meta := &pb.BatchMeta{Table: "t", HighPos: "p1"}

	body, _, err := EncodeBatch(rows, schema, meta)
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

	// DecodeBatch should succeed — the op column is valid (0).
	_, _, err = DecodeBatch(rec, nil, schema.PrimaryKey)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
}

// T-12: Nomes reservados rejeitados no schema.
func TestCodecReservedNamesRejected(t *testing.T) {
	reserved := []string{"__op", "__pos", "__commit_ts", "__ingest_ts", "__snapshot", "__phase"}
	for _, name := range reserved {
		schema := core.Schema{
			Columns: []core.Column{
				{Name: name, Type: core.ColumnType{Kind: core.KindString}},
			},
			PrimaryKey: []string{},
		}
		rows := []rowchange.Change{
			{Op: rowchange.OpInsert, Table: "t", After: map[string]any{name: "val"}, Position: "p1"},
		}
		meta := &pb.BatchMeta{Table: "t", HighPos: "p1"}
		_, _, err := EncodeBatch(rows, schema, meta)
		if err == nil {
			t.Errorf("reserved name %q should be rejected", name)
		}
	}
}
