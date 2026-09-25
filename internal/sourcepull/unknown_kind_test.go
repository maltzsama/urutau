package sourcepull

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/source"
)

// A source introspects an unsigned MySQL column as KindUnknown; only the
// resolved (cast-applied) schema knows its wire kind. The engine hands that
// schema over through source.SchemaSetter, and the puller must encode the
// column with it instead of failing on "unsupported canonical kind unknown".
func TestPullerTakesResolvedKindForUnknownColumns(t *testing.T) {
	p := New(make(chan rowchange.Change))
	p.SetSchemas(map[string]core.Schema{
		"t": {
			Columns: []core.Column{
				{Name: "id", Type: core.ColumnType{Kind: core.KindUnknown}},
				{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			},
			PrimaryKey: []string{"id"},
		},
	})
	var setter source.SchemaSetter = p
	setter.SetSourceSchemas(map[string]core.Schema{
		"t": {
			Columns: []core.Column{
				{Name: "id", Type: core.ColumnType{Kind: core.KindUInt64}},
				// The engine's schema may carry more (enrich columns) or a
				// different kind for a mapped column; only KindUnknown
				// columns take the resolved kind.
				{Name: "v", Type: core.ColumnType{Kind: core.KindJSON}},
				{Name: "enriched", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			},
			PrimaryKey: []string{"id"},
		},
	})
	ch := make(chan rowchange.Change, 1)
	ch <- rowchange.Change{Op: rowchange.OpInsert, Table: "t", Key: []any{uint64(1) << 63}, After: map[string]any{"id": uint64(1) << 63, "v": "a"}, Position: "p1"}
	close(ch)
	p.ch = ch

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	defer b.Release()
	sch := b.Record.Schema()
	kind := func(name string) string {
		t.Helper()
		idx := sch.FieldIndices(name)
		if len(idx) == 0 {
			return ""
		}
		return sch.Field(idx[0]).Type.String()
	}
	if got := kind("id"); got != "uint64" {
		t.Errorf("id encoded as %q, want uint64", got)
	}
	if got := kind("v"); got != "utf8" {
		t.Errorf("v encoded as %q, want utf8 (the source's own kind)", got)
	}
	if got := kind("enriched"); got != "" {
		t.Errorf("batch carries enriched (%s): the resolved schema must not add columns", got)
	}
}
