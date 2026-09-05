package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/core"
)

func upsertSchema() (core.Schema, core.TableRef) {
	schema := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
	}
	ref := core.TableRef{Source: "src.orders", Target: "orders", PrimaryKey: []string{"id"}}
	return schema, ref
}

func TestBuildDDLUpsert(t *testing.T) {
	schema, ref := upsertSchema()
	ddl, err := buildDDL(tableIdent{db: "lakehouse", table: "orders"}, ref, schema, nil, change.UpsertMode)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `lakehouse`.`orders`",
		"`id` Int64",                   // pk: forced non-null
		"`v` Nullable(String)",         // nullable follows the canonical flag
		"`position` String",            // technical
		"`seq` UInt64",                 // technical
		"`is_deleted` UInt8 DEFAULT 0", // technical, upsert only
		"ENGINE = ReplacingMergeTree(seq, is_deleted)",
		"ORDER BY (`id`)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("ddl %q: want substring %q", ddl, want)
		}
	}
	if strings.Contains(ddl, "PARTITION BY") {
		t.Errorf("default table must not declare a partition: %q", ddl)
	}
}

func TestBuildDDLAppend(t *testing.T) {
	schema, ref := upsertSchema()
	ref.PrimaryKey = nil // append tables need no key
	ddl, err := buildDDL(tableIdent{db: "lakehouse", table: "events"}, ref, schema, nil, change.AppendMode)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, want := range []string{
		"ENGINE = MergeTree",
		"ORDER BY tuple()",
		"`seq` UInt64", // ordering only, keeps the resume argMax correct
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("ddl %q: want substring %q", ddl, want)
		}
	}
	if strings.Contains(ddl, "is_deleted") {
		t.Errorf("append table must not carry tombstone machinery: %q", ddl)
	}
	if strings.Contains(ddl, "ReplacingMergeTree") {
		t.Errorf("append table must not version: %q", ddl)
	}
}

func TestBuildDDLPartitionByOptIn(t *testing.T) {
	schema, ref := upsertSchema()
	ddl, err := buildDDL(tableIdent{db: "lakehouse", table: "orders"}, ref, schema,
		[]string{"toYYYYMMDD(ingest_ts)"}, change.UpsertMode)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !strings.Contains(ddl, "PARTITION BY (toYYYYMMDD(ingest_ts))") {
		t.Errorf("ddl %q: want the opt-in partition expression", ddl)
	}
}

func TestBuildDDLReservedNames(t *testing.T) {
	schema, ref := upsertSchema()
	for _, name := range []string{"position", "seq", "is_deleted"} {
		schema.Columns = append(schema.Columns, core.Column{Name: name, Type: core.ColumnType{Kind: core.KindString, Nullable: true}})
		if _, err := buildDDL(tableIdent{db: "d", table: "t"}, ref, schema, nil, change.UpsertMode); err == nil {
			t.Errorf("column %q: want reserved-name error", name)
		}
		schema.Columns = schema.Columns[:len(schema.Columns)-1]
	}
}

func TestBuildDDLUpsertRequiresPK(t *testing.T) {
	schema, ref := upsertSchema()
	ref.PrimaryKey = nil
	if _, err := buildDDL(tableIdent{db: "d", table: "t"}, ref, schema, nil, change.UpsertMode); err == nil {
		t.Fatal("upsert without a primary key: want error")
	}
}

func TestBuildDDLUnknownPKColumn(t *testing.T) {
	schema, ref := upsertSchema()
	ref.PrimaryKey = []string{"nope"}
	if _, err := buildDDL(tableIdent{db: "d", table: "t"}, ref, schema, nil, change.UpsertMode); err == nil {
		t.Fatal("pk column outside the schema: want error")
	}
}

func TestBuildDDLNestedUnsupported(t *testing.T) {
	schema := core.Schema{Columns: []core.Column{
		{Name: "tags", Type: core.ColumnType{Kind: core.KindList, Elem: &core.ColumnType{Kind: core.KindString}}},
	}}
	ref := core.TableRef{Target: "orders", PrimaryKey: []string{"tags"}}
	_, err := buildDDL(tableIdent{db: "d", table: "t"}, ref, schema, nil, change.AppendMode)
	if err == nil || !strings.Contains(err.Error(), "not supported yet") {
		t.Fatalf("nested list: want escape-valve error, got %v", err)
	}
}

func TestCHTypeMapping(t *testing.T) {
	cases := []struct {
		ct      core.ColumnType
		want    string
		wantErr bool
	}{
		{core.ColumnType{Kind: core.KindBool}, "Bool", false},
		{core.ColumnType{Kind: core.KindInt64}, "Int64", false},
		{core.ColumnType{Kind: core.KindFloat64}, "Float64", false},
		{core.ColumnType{Kind: core.KindString}, "String", false},
		{core.ColumnType{Kind: core.KindJSON}, "String", false},
		{core.ColumnType{Kind: core.KindTimestampTZ}, "DateTime64(6, 'UTC')", false},
		{core.ColumnType{Kind: core.KindUUID}, "UUID", false},
		{core.ColumnType{Kind: core.KindDecimal, Precision: 20, Scale: 4}, "Decimal(20, 4)", false},
		{core.ColumnType{Kind: core.KindUnknown}, "", true},
		{core.ColumnType{Kind: core.KindStruct}, "", true},
	}
	for _, tc := range cases {
		got, err := chType(tc.ct, false)
		if tc.wantErr {
			if err == nil {
				t.Errorf("kind %s: want error", tc.ct.Kind)
			}
			continue
		}
		if err != nil {
			t.Errorf("kind %s: %v", tc.ct.Kind, err)
			continue
		}
		if got != tc.want {
			t.Errorf("kind %s = %q, want %q", tc.ct.Kind, got, tc.want)
		}
	}
}

func TestParseCHType(t *testing.T) {
	if base, nullable := parseCHType("Nullable(String)"); base != "String" || !nullable {
		t.Errorf("Nullable(String) = %q %v", base, nullable)
	}
	if base, nullable := parseCHType("DateTime64(6, 'UTC')"); base != "DateTime64(6, 'UTC')" || nullable {
		t.Errorf("DateTime64 = %q %v", base, nullable)
	}
	if base, nullable := parseCHType("LowCardinality(Nullable(String))"); base != "String" || !nullable {
		t.Errorf("LowCardinality = %q %v", base, nullable)
	}
}

func TestCoerce(t *testing.T) {
	ts := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	midnight := time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		base string
		in   any
		want any
	}{
		{"Int64", int64(7), int64(7)},
		{"Int64", int(7), int64(7)},
		{"Int64", "7", int64(7)},
		{"Int64", 7.0, int64(7)},
		{"Float64", 1.5, 1.5},
		{"Float64", "1.5", 1.5},
		{"Bool", true, true},
		{"Bool", "true", true},
		{"String", "x", "x"},
		{"String", []byte("x"), "x"},
		{"Date", "2026-09-05", midnight},
		{"DateTime64(6, 'UTC')", "2026-09-05T12:00:00Z", ts},
		{"UUID", "0b1c2d3e-1111-2222-3333-444455556666", "0b1c2d3e-1111-2222-3333-444455556666"},
	}
	for _, tc := range cases {
		got, err := coerce(tc.base, tc.in)
		if err != nil {
			t.Errorf("coerce(%s, %v): %v", tc.base, tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("coerce(%s, %v) = %v (%T), want %v", tc.base, tc.in, got, got, tc.want)
		}
	}

	// Decimal parses to shopspring's type; compare by text.
	d, err := coerce("Decimal(20, 4)", "123.45")
	if err != nil {
		t.Fatalf("coerce decimal: %v", err)
	}
	if got, ok := d.(interface{ String() string }); !ok || got.String() != "123.45" {
		t.Errorf("coerce decimal = %v, want 123.45", d)
	}

	if _, err := coerce("Int64", 7.5); err == nil {
		t.Error("non-integral float for Int64: want error")
	}
	if _, err := coerce("String", 42); err == nil {
		t.Error("number for String: want error")
	}
}

func TestCoerceNilStaysNil(t *testing.T) {
	// nil must reach valueFor as nil, so the nullable-vs-zero decision stays
	// with the column metadata, not the coercion.
	got, err := coerce("String", nil)
	if err != nil || got != nil {
		t.Errorf("coerce nil = %v, %v; want nil, nil", got, err)
	}
}
