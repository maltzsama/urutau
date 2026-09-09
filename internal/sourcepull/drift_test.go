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
