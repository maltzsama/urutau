package postgres

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/source"
)

func TestFindColumnFound(t *testing.T) {
	st := &TableState{Columns: []Column{
		{Name: "id", DataType: "bigint"},
		{Name: "name", DataType: "text"},
		{Name: "amount", DataType: "numeric"},
	}}
	if got := st.FindColumn("name"); got != 1 {
		t.Errorf("FindColumn(name) = %d, want 1", got)
	}
}

func TestFindColumnNotFound(t *testing.T) {
	st := &TableState{Columns: []Column{
		{Name: "id", DataType: "bigint"},
	}}
	if got := st.FindColumn("missing"); got != -1 {
		t.Errorf("FindColumn(missing) = %d, want -1", got)
	}
}

func TestCanonicalSchemaIntTypes(t *testing.T) {
	st := &TableState{Columns: []Column{
		{Name: "a", DataType: "smallint"},
		{Name: "b", DataType: "integer"},
		{Name: "c", DataType: "bigint"},
	}}
	got, err := CanonicalSchema(st)
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
	st := &TableState{Columns: []Column{
		{Name: "a", DataType: "real"},
		{Name: "b", DataType: "double precision"},
		{Name: "c", DataType: "money"},
	}}
	got, err := CanonicalSchema(st)
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
	st := &TableState{Columns: []Column{
		{Name: "a", DataType: "character varying"},
		{Name: "b", DataType: "character"},
		{Name: "c", DataType: "text"},
		{Name: "d", DataType: "citext"},
	}}
	got, err := CanonicalSchema(st)
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
	st := &TableState{Columns: []Column{
		{Name: "a", DataType: "boolean"},
		{Name: "b", DataType: "json"},
		{Name: "c", DataType: "jsonb"},
	}}
	got, err := CanonicalSchema(st)
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
	st := &TableState{Columns: []Column{
		{Name: "a", DataType: "date"},
		{Name: "b", DataType: "time without time zone"},
		{Name: "c", DataType: "time with time zone"},
		{Name: "d", DataType: "timestamp without time zone"},
		{Name: "e", DataType: "timestamp with time zone"},
	}}
	got, err := CanonicalSchema(st)
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
	st := &TableState{Columns: []Column{
		{Name: "a", DataType: "uuid"},
		{Name: "b", DataType: "bytea"},
	}}
	got, err := CanonicalSchema(st)
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
	st := &TableState{
		Columns:   []Column{{Name: "a"}, {Name: "b"}, {Name: "id"}},
		PKColumns: []int{2},
	}
	got, err := CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	if len(got.PrimaryKey) != 1 || got.PrimaryKey[0] != "id" {
		t.Errorf("PrimaryKey = %v, want [id]", got.PrimaryKey)
	}
}

func TestCanonicalSchemaEmptyColumns(t *testing.T) {
	st := &TableState{Columns: []Column{}}
	got, err := CanonicalSchema(st)
	if err != nil {
		t.Fatalf("CanonicalSchema: %v", err)
	}
	if len(got.Columns) != 0 {
		t.Errorf("empty columns = %v", got.Columns)
	}
}

func TestCanonicalSchemaNumericPrecision(t *testing.T) {
	st := &TableState{Columns: []Column{
		{Name: "a", DataType: "numeric(10,2)"},
	}}
	got, err := CanonicalSchema(st)
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

func TestNewChunkerBadSource(t *testing.T) {
	_, err := NewChunker(context.Background(), nil, "no_schema_dot", "id", 1000)
	if err == nil {
		t.Error("bare source name: want error")
	}
}

func TestNewChunkerBadChunkSize(t *testing.T) {
	_, err := NewChunker(context.Background(), nil, "public.orders", "id", 0)
	if err == nil {
		t.Error("zero chunk size: want error")
	}
	_, err = NewChunker(context.Background(), nil, "public.orders", "id", -1)
	if err == nil {
		t.Error("negative chunk size: want error")
	}
}

func TestNewChunkerValid(t *testing.T) {
	c, err := NewChunker(context.Background(), nil, "public.orders", "id", 1000)
	if err != nil {
		t.Fatalf("NewChunker: %v", err)
	}
	if c.pk[0] != "id" {
		t.Errorf("pk = %v", c.pk)
	}
}

func TestNewChunkerCompositeKey(t *testing.T) {
	c, err := NewChunker(context.Background(), nil, "public.orders", "a, b", 1000)
	if err != nil {
		t.Fatalf("NewChunker: %v", err)
	}
	if len(c.pk) != 2 || c.pk[0] != "a" || c.pk[1] != "b" {
		t.Errorf("pk = %v", c.pk)
	}
}

func TestNewChunkerKeyWithSpaces(t *testing.T) {
	c, err := NewChunker(context.Background(), nil, "public.orders", " a , b , c ", 500)
	if err != nil {
		t.Fatalf("NewChunker: %v", err)
	}
	if len(c.pk) != 3 {
		t.Errorf("pk = %v", c.pk)
	}
}

func TestEnsureSetupBadSlotName(t *testing.T) {
	err := EnsureSetup(t.Context(), nil, "bad-name!", []source.TableRef{{Source: "public.t"}})
	if err == nil {
		t.Error("bad slot name: want error")
	}
}

func TestEnsureSetupEmptyTables(t *testing.T) {
	err := EnsureSetup(t.Context(), nil, "slot", nil)
	if err == nil {
		t.Error("empty tables: want error")
	}
}

func TestEnsureSetupBareSourceName(t *testing.T) {
	err := EnsureSetup(t.Context(), nil, "slot", []source.TableRef{{Source: "no_dot"}})
	if err == nil {
		t.Error("bare source name: want error")
	}
}

func TestEnsureSetupValidSlotName(t *testing.T) {
	// Valid slot name passes validation; the nil DB causes a panic later,
	// so just verify the slot-name regex accepts it.
	if !slotNameRe.MatchString("_valid_slot_123") {
		t.Error("_valid_slot_123 should match slot name regex")
	}
	if !slotNameRe.MatchString("mySlot") {
		t.Error("mySlot should match slot name regex")
	}
	if slotNameRe.MatchString("bad-name!") {
		t.Error("bad-name! should not match slot name regex")
	}
	if slotNameRe.MatchString("1starts_digit") {
		t.Error("starts with digit should not match")
	}
}
