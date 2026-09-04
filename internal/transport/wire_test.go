package transport

import (
	"testing"
	"time"

	"github.com/maltzsama/urutau/internal/core"
)

// The assignment schema round trip must preserve every canonical kind —
// the worker rebuilds the Iceberg table from it, so a lossy round trip
// would create a target with wrong column types.
func TestTableSchemaRoundTrip(t *testing.T) {
	cs := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "name", Type: core.ColumnType{Kind: core.KindString, Nullable: false}},
			{Name: "amount", Type: core.ColumnType{Kind: core.KindDecimal, Precision: 20, Scale: 4}},
			{Name: "active", Type: core.ColumnType{Kind: core.KindBool}},
			{Name: "born", Type: core.ColumnType{Kind: core.KindTimestampTZ}},
			{Name: "uid", Type: core.ColumnType{Kind: core.KindUUID}},
			{Name: "payload", Type: core.ColumnType{Kind: core.KindBinary}},
			{Name: "ratio", Type: core.ColumnType{Kind: core.KindFloat64}},
		},
		PrimaryKey: []string{"id"},
	}
	b, err := EncodeTableSchema(cs)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeTableSchema(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.Columns) != len(cs.Columns) {
		t.Fatalf("columns = %d, want %d", len(got.Columns), len(cs.Columns))
	}
	for i, want := range cs.Columns {
		have := got.Columns[i]
		if have.Name != want.Name {
			t.Fatalf("col %d: name %q, want %q", i, have.Name, want.Name)
		}
		if have.Type.Kind != want.Type.Kind {
			t.Fatalf("col %q: kind %v, want %v", want.Name, have.Type.Kind, want.Type.Kind)
		}
		if have.Type.Nullable != want.Type.Nullable {
			t.Fatalf("col %q: nullable %v, want %v", want.Name, have.Type.Nullable, want.Type.Nullable)
		}
		if want.Type.Kind == core.KindDecimal && (have.Type.Precision != want.Type.Precision || have.Type.Scale != want.Type.Scale) {
			t.Fatalf("col %q: decimal (%d,%d), want (%d,%d)",
				want.Name, have.Type.Precision, have.Type.Scale, want.Type.Precision, want.Type.Scale)
		}
	}
}

// Chunk bounds must survive as their native types: the worker binds them
// into SQL comparisons against the primary key columns.
func TestBoundsRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	low := []any{int64(100), "abc", at}
	high := []any{int64(200), "zzz", at.Add(time.Hour)}

	b, err := EncodeBounds(low, high)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rows, err := DecodeBounds(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (low + high)", len(rows))
	}
	if rows[0][0] != int64(100) {
		t.Fatalf("low[0] = %v (%T), want int64 100", rows[0][0], rows[0][0])
	}
	if rows[0][1] != "abc" {
		t.Fatalf("low[1] = %v, want abc", rows[0][1])
	}
	gotAt, ok := rows[0][2].(time.Time)
	if !ok || !gotAt.Equal(at) {
		t.Fatalf("low[2] = %v (%T), want %v", rows[0][2], rows[0][2], at)
	}
	if rows[1][0] != int64(200) {
		t.Fatalf("high[0] = %v, want int64 200", rows[1][0])
	}
}

// The open-high last chunk encodes one row; decoding yields only the low.
func TestBoundsOpenHigh(t *testing.T) {
	b, err := EncodeBounds([]any{int64(42)}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rows, err := DecodeBounds(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rows) != 1 || rows[0][0] != int64(42) {
		t.Fatalf("rows = %v, want one row [42]", rows)
	}
}

// A nil low (empty source edge) encodes an empty record.
func TestBoundsEmpty(t *testing.T) {
	b, err := EncodeBounds(nil, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rows, err := DecodeBounds(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %v, want none", rows)
	}
}
