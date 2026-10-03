package sourcepull

import (
	"context"
	"strings"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
)

func payloadSchema() map[string]core.Schema {
	return map[string]core.Schema{
		"t": {
			Columns: []core.Column{
				{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
				{Name: "payload", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
			},
			PrimaryKey: []string{"id"},
		},
	}
}

// A run of large rows closes a batch at the byte ceiling instead of buffering
// the whole row target (#579).
func TestByteCeilingSplitsABatch(t *testing.T) {
	const total = 5
	payload := strings.Repeat("x", batchTargetBytes/4)
	ch := make(chan rowchange.Change, total)
	for i := 0; i < total; i++ {
		ch <- rowchange.Change{
			Op: rowchange.OpInsert, Table: "t", Position: "p",
			After: map[string]any{"id": int64(i), "payload": payload},
		}
	}
	close(ch)
	p := New(ch)
	p.SetSchemas(payloadSchema())

	var rows int
	for {
		b, err := p.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		if b.Record.NumRows() >= total {
			t.Fatalf("a batch of %d rows exceeded the byte ceiling; want a split", b.Record.NumRows())
		}
		rows += int(b.Record.NumRows())
		b.Release()
	}
	if rows != total {
		t.Fatalf("emitted %d rows, want %d", rows, total)
	}
}

// A single row larger than the byte ceiling still lands: the cap never drops
// a row.
func TestByteCeilingKeepsAnOversizedRow(t *testing.T) {
	ch := make(chan rowchange.Change, 1)
	ch <- rowchange.Change{
		Op: rowchange.OpInsert, Table: "t", Position: "p",
		After: map[string]any{"id": int64(1), "payload": strings.Repeat("x", 2*batchTargetBytes)},
	}
	close(ch)
	p := New(ch)
	p.SetSchemas(payloadSchema())

	b, err := p.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b == nil || b.Record.NumRows() != 1 {
		t.Fatalf("oversized single row = %v, want one row", b)
	}
	b.Release()
}
