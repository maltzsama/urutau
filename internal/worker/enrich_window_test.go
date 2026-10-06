package worker

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/spec"
)

// A snapshot window's rows must be enriched before they are buffered, or the
// whole backfill lands with NULL reference columns (issue #564).
func TestWorkerEnrichesWindowRows(t *testing.T) {
	cfg := spec.Enrich{
		Table:    "users",
		Source:   spec.EnrichSource{URI: "mysql://refdb/internal", Query: "SELECT id, name FROM users"},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"name"},
		JoinType: "left",
	}
	decl := core.Schema{Columns: []core.Column{
		{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64}},
		{Name: "users.name", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
	}, PrimaryKey: []string{"id"}}

	st, err := enrich.New([]spec.Enrich{cfg}, decl, nil)
	if err != nil {
		t.Fatalf("enrich.New: %v", err)
	}
	if err := st.UseLoader("users", &staticLoader{}); err != nil {
		t.Fatalf("UseLoader: %v", err)
	}
	st.Start(context.Background())
	defer st.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for !st.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("reference never went hot")
		}
		time.Sleep(time.Millisecond)
	}

	fc := &fakeCommitter{}
	w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
	regTable(t, w, "raw.orders", fc, dataplane.UpsertMode)
	w.SetKnownSchema("raw.orders", decl)
	w.SetEnricher("raw.orders", st)

	winRec, err := transport.RecordFromChanges(
		[]rowchange.Change{{Op: rowchange.OpInsert, Table: "raw.orders", Key: []any{int64(1)},
			After: map[string]any{"id": int64(1), "user_ref": int64(7)}, Position: "p0"}},
		decl, nil)
	if err != nil {
		t.Fatalf("window record: %v", err)
	}
	if err := w.AddWindowRows("raw.orders", 7, &dataplane.Batch{Table: "raw.orders", Record: winRec, Mode: dataplane.AppendMode}); err != nil {
		t.Fatalf("AddWindowRows: %v", err)
	}
	ingest := make(chan Ingest, 1)
	ingest <- toIngest(t, rowchange.Change{Table: "raw.orders", Position: "p9"}, &rowchange.Window{WindowID: 7, Closes: true})
	close(ingest)
	if err := w.Run(context.Background(), ingest); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(fc.batches) != 1 || len(fc.batches[0].Changes) != 1 {
		t.Fatalf("batches: %+v", fc.batches)
	}
	if got := fc.batches[0].Changes[0].After["users.name"]; got != "ana" {
		t.Fatalf("window row not enriched: users.name = %#v, want ana", got)
	}
}
