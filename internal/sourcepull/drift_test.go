package sourcepull

// Drift detection lives at the SOURCE boundary where the native row shape
// exists: encoding against the canonical schema would silently drop a field
// the schema does not know (top-level OR nested inside a struct), hiding
// source evolution from the downstream columnar worker. When the source
// installs canonical schemas, makeBatch rejects drift before encode.

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

func structSchema() map[string]core.Schema {
	return map[string]core.Schema{
		"t": {
			Columns: []core.Column{
				{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
				{Name: "address", Type: core.ColumnType{Kind: core.KindStruct, Fields: []core.Column{
					{Name: "city", Type: core.ColumnType{Kind: core.KindString}},
				}}},
			},
			PrimaryKey: []string{"id"},
		},
	}
}

func feed(ch chan<- rowchange.Change, c rowchange.Change) {
	ch <- c
	close(ch)
}

func TestDriftAtBoundaryNestedStruct(t *testing.T) {
	p := New(make(chan rowchange.Change))
	p.SetSchemas(structSchema())

	ch := make(chan rowchange.Change, 1)
	feed(ch, rowchange.Change{Op: rowchange.OpInsert, Table: "t",
		After: map[string]any{
			"id":      int64(1),
			"address": map[string]any{"city": "sp", "complement": "apto 4"},
		}})
	p.ch = ch

	_, err := p.Next(context.Background())
	if err == nil || err.Error() != "sourcepull: schema drift: column \"address.complement\" is not in the spec — declare it and resume" {
		t.Fatalf("err = %v, want nested drift on address.complement", err)
	}
}

func TestDriftAtBoundaryTopLevel(t *testing.T) {
	p := New(make(chan rowchange.Change))
	p.SetSchemas(structSchema())

	ch := make(chan rowchange.Change, 1)
	feed(ch, rowchange.Change{Op: rowchange.OpInsert, Table: "t",
		After: map[string]any{"id": int64(1), "surprise": "x"}})
	p.ch = ch

	_, err := p.Next(context.Background())
	if err == nil || err.Error() != "sourcepull: schema drift: column \"surprise\" is not in the spec — declare it and resume" {
		t.Fatalf("err = %v, want top-level drift on surprise", err)
	}
}

func TestDriftAtBoundaryConformingPasses(t *testing.T) {
	p := New(make(chan rowchange.Change))
	p.SetSchemas(structSchema())

	ch := make(chan rowchange.Change, 1)
	feed(ch, rowchange.Change{Op: rowchange.OpInsert, Table: "t",
		After: map[string]any{"id": int64(1), "address": map[string]any{"city": "sp"}}})
	p.ch = ch

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("conforming row must pass: %v", err)
	}
	if b == nil || b.Record.NumRows() != 1 {
		t.Fatalf("batch = %v, want one row", b)
	}
	b.Release()
}

// SetSchemas must invalidate the cached column index: a schema reordered or
// shrunk after the cache was built would otherwise read the wrong column
// (#581).
func TestSetSchemasInvalidatesColumnIndex(t *testing.T) {
	p := New(make(chan rowchange.Change))
	p.SetSchemas(schemaFor("t"))
	_ = p.colIndex("t", p.schemas["t"]) // build the cache from id,v

	p.SetSchemas(map[string]core.Schema{"t": {
		Columns: []core.Column{
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		},
		PrimaryKey: []string{"id"},
	}})
	idx := p.colIndex("t", p.schemas["t"])
	if idx["v"] != 0 || idx["id"] != 1 {
		t.Fatalf("index = %v, want v=0 id=1 after SetSchemas rebuilt it", idx)
	}
}

// An always-nil column the schema lacks is still materialized by the merge
// (preserving MergeSchema's behavior), not silently dropped by the fused
// pass that skips the merge when nothing is unknown (#581).
func TestUnknownNilColumnStillMerges(t *testing.T) {
	p := New(make(chan rowchange.Change))
	p.SetSchemas(schemaFor("t"))

	ch := make(chan rowchange.Change, 1)
	feed(ch, rowchange.Change{Op: rowchange.OpInsert, Table: "t",
		After: map[string]any{"id": int64(1), "v": "x", "ghost": nil}})
	p.ch = ch

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("nil unknown column must not trip drift: %v", err)
	}
	defer b.Release()
	for i := 0; i < int(b.Record.Schema().NumFields()); i++ {
		if b.Record.Schema().Field(i).Name == "ghost" {
			return
		}
	}
	t.Fatalf("an always-nil unknown column must still be merged, schema = %v", b.Record.Schema())
}

func TestNoSchemaSkipsDriftCheck(t *testing.T) {
	// A schema-less producer (no SetSchemas) keeps the inference fallback
	// and no drift gate — its resolved schema is owned upstream.
	p := New(make(chan rowchange.Change))
	ch := make(chan rowchange.Change, 1)
	feed(ch, rowchange.Change{Op: rowchange.OpInsert, Table: "t",
		After: map[string]any{"id": int64(1), "whatever": "x"}})
	p.ch = ch

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("schema-less producer must not trip the drift gate: %v", err)
	}
	if b != nil {
		b.Release()
	}
}
