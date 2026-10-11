package postgres

import (
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"

	"github.com/maltzsama/urutau/internal/source/postgres/catalog"
)

func TestFindColumnFound(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "id", DataType: "bigint"},
		{Name: "name", DataType: "text"},
		{Name: "amount", DataType: "numeric"},
	}}
	if got := st.FindColumn("name"); got != 1 {
		t.Errorf("FindColumn(name) = %d, want 1", got)
	}
}

func TestFindColumnNotFound(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "id", DataType: "bigint"},
	}}
	if got := st.FindColumn("missing"); got != -1 {
		t.Errorf("FindColumn(missing) = %d, want -1", got)
	}
}

func TestCanonicalSchemaIntTypes(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "a", DataType: "smallint"},
		{Name: "b", DataType: "integer"},
		{Name: "c", DataType: "bigint"},
	}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	for _, col := range got.Columns {
		if col.Type.Kind != core.KindInt64 {
			t.Errorf("%s kind = %v, want KindInt64", col.Name, col.Type.Kind)
		}
	}
}

func TestCanonicalSchemaFloatTypes(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "a", DataType: "real"},
		{Name: "b", DataType: "double precision"},
		{Name: "c", DataType: "money"},
	}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	for _, col := range got.Columns {
		if col.Type.Kind != core.KindFloat64 {
			t.Errorf("%s kind = %v, want KindFloat64", col.Name, col.Type.Kind)
		}
	}
}

func TestCanonicalSchemaStringTypes(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "a", DataType: "character varying"},
		{Name: "b", DataType: "character"},
		{Name: "c", DataType: "text"},
		{Name: "d", DataType: "citext"},
	}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	for _, col := range got.Columns {
		if col.Type.Kind != core.KindString {
			t.Errorf("%s kind = %v, want KindString", col.Name, col.Type.Kind)
		}
	}
}

func TestCanonicalSchemaBoolAndJSON(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "a", DataType: "boolean"},
		{Name: "b", DataType: "json"},
		{Name: "c", DataType: "jsonb"},
	}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	if got.Columns[0].Type.Kind != core.KindBool {
		t.Errorf("boolean kind = %v", got.Columns[0].Type.Kind)
	}
	if got.Columns[1].Type.Kind != core.KindJSON {
		t.Errorf("json kind = %v", got.Columns[1].Type.Kind)
	}
	if got.Columns[2].Type.Kind != core.KindJSON {
		t.Errorf("jsonb kind = %v", got.Columns[2].Type.Kind)
	}
}

func TestCanonicalSchemaTemporalTypes(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "a", DataType: "date"},
		{Name: "b", DataType: "time without time zone"},
		{Name: "c", DataType: "time with time zone"},
		{Name: "d", DataType: "timestamp without time zone"},
		{Name: "e", DataType: "timestamp with time zone"},
	}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	wantKinds := []core.Kind{core.KindDate, core.KindTime, core.KindTime, core.KindTimestamp, core.KindTimestampTZ}
	for i, w := range wantKinds {
		if got.Columns[i].Type.Kind != w {
			t.Errorf("%s kind = %v, want %v", got.Columns[i].Name, got.Columns[i].Type.Kind, w)
		}
	}
}

func TestCanonicalSchemaUUIDAndBinary(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "a", DataType: "uuid"},
		{Name: "b", DataType: "bytea"},
	}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	if got.Columns[0].Type.Kind != core.KindUUID {
		t.Errorf("uuid kind = %v", got.Columns[0].Type.Kind)
	}
	if got.Columns[1].Type.Kind != core.KindBinary {
		t.Errorf("bytea kind = %v", got.Columns[1].Type.Kind)
	}
}

func TestCanonicalSchemaPKColumns(t *testing.T) {
	st := &catalog.TableState{
		Columns:   []catalog.Column{{Name: "a"}, {Name: "b"}, {Name: "id"}},
		PKColumns: []int{2},
	}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	if len(got.PrimaryKey) != 1 || got.PrimaryKey[0] != "id" {
		t.Errorf("PrimaryKey = %v, want [id]", got.PrimaryKey)
	}
}

func TestCanonicalSchemaEmptyColumns(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	if len(got.Columns) != 0 {
		t.Errorf("empty columns = %v", got.Columns)
	}
}

func TestCanonicalSchemaNumericPrecision(t *testing.T) {
	st := &catalog.TableState{Columns: []catalog.Column{
		{Name: "a", DataType: "numeric(10,2)"},
	}}
	got, err := catalog.CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	if got.Columns[0].Type.Kind != core.KindDecimal {
		t.Errorf("numeric kind = %v", got.Columns[0].Type.Kind)
	}
	if got.Columns[0].Type.Precision != 10 || got.Columns[0].Type.Scale != 2 {
		t.Errorf("numeric precision = %d, scale = %d", got.Columns[0].Type.Precision, got.Columns[0].Type.Scale)
	}
}

// A timestamptz cursor must render as a string the next query casts back with
// ?::timestamptz — Go's default layout does not round-trip (issue #572).
func TestIncrementalCursorFormatRoundTrips(t *testing.T) {
	ts := time.Date(2026, 10, 7, 12, 0, 0, 123456789, time.UTC)
	got := formatIncrementalCursor(ts)
	back, err := time.Parse(time.RFC3339Nano, got)
	if err != nil || !back.Equal(ts) {
		t.Fatalf("cursor %q did not round-trip: %v", got, err)
	}
	if formatIncrementalCursor(int64(42)) != "42" {
		t.Fatalf("int cursor = %q", formatIncrementalCursor(int64(42)))
	}
}

func TestIncrementalPosEncodeDecode(t *testing.T) {
	pos := encodeIncrementalPos("2026-10-07T12:00:00Z", []string{"42", "abc"})
	cur, pk := decodeIncrementalPos(pos)
	if cur != "2026-10-07T12:00:00Z" || len(pk) != 2 || pk[0] != "42" || pk[1] != "abc" {
		t.Fatalf("decode = %q %v", cur, pk)
	}
	if cur, pk := decodeIncrementalPos("plain-cursor"); cur != "plain-cursor" || pk != nil {
		t.Fatalf("plain decode = %q %v", cur, pk)
	}
}
