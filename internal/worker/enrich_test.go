package worker

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// staticLoader satisfies enrich.Loader without a database: one row,
// id (Int64) + name (String).
type staticLoader struct{}

func (l *staticLoader) Load(context.Context) (arrow.RecordBatch, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(7)
	b.Field(1).(*array.StringBuilder).Append("ana")
	return b.NewRecordBatch(), nil
}
func (l *staticLoader) Close() error { return nil }

// TestWorkerEnrichJoinsBeforeBuffering: enriched rows land in the
// committed batch with the reference columns; an inner-miss event is
// dropped before the batch and counted.
func TestWorkerEnrichJoinsBeforeBuffering(t *testing.T) {
	leftCfg := spec.Enrich{
		Table:    "users",
		Source:   spec.EnrichSource{URI: "mysql://refdb/internal", Query: "SELECT id, name FROM users"},
		On:       map[string]string{"user_ref": "id"},
		Select:   []string{"name"},
		JoinType: "left",
	}
	build := func(t *testing.T, joinType string) (*fakeCommitter, *enrich.Stage) {
		t.Helper()
		cfg := leftCfg
		cfg.JoinType = joinType
		s, err := enrich.New([]spec.Enrich{cfg}, []string{"id", "v", "user_ref"}, nil)
		if err != nil {
			t.Fatalf("enrich.New: %v", err)
		}
		if err := s.UseLoader("users", &staticLoader{}); err != nil {
			t.Fatalf("UseLoader: %v", err)
		}
		s.Start(context.Background())
		t.Cleanup(s.Stop)
		// Warm: wait until every reference has completed its first load.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if s.Ready() {
				return nil, s
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("reference never went hot")
		return nil, nil
	}

	t.Run("left hit enriches the batch", func(t *testing.T) {
		_, st := build(t, "left")
		fc := &fakeCommitter{}
		w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
		regTable(t, w, "t", fc, dataplane.UpsertMode)
		w.SetKnownSchema("t", core.Schema{Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64}},
		}, PrimaryKey: []string{"id"}})
		w.SetEnricher("t", st)
		ingest := make(chan rowchange.Change, 2)
		ingest <- rowchange.Change{Op: rowchange.OpInsert, Table: "t", Key: []any{int64(1)}, Position: "p1",
			After: map[string]any{"id": int64(1), "v": "a", "user_ref": int64(7)}}
		close(ingest)
		if err := w.Run(context.Background(), IngestFromChanges(context.Background(), ingest, testSchema())); err != nil {
			t.Fatalf("run: %v", err)
		}
		if len(fc.batches) != 1 || len(fc.batches[0].Changes) != 1 {
			t.Fatalf("batches: %+v", fc.batches)
		}
		row := fc.batches[0].Changes[0]
		if row.After["users.name"] != "ana" {
			t.Fatalf("row not enriched: %v", row.After)
		}
	})

	t.Run("inner miss drops and counts", func(t *testing.T) {
		_, st := build(t, "inner")
		fc := &fakeCommitter{}
		w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
		regTable(t, w, "t", fc, dataplane.UpsertMode)
		w.SetKnownSchema("t", core.Schema{Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
			{Name: "user_ref", Type: core.ColumnType{Kind: core.KindInt64}},
		}, PrimaryKey: []string{"id"}})
		w.SetEnricher("t", st)
		ingest := make(chan rowchange.Change, 2)
		ingest <- rowchange.Change{Op: rowchange.OpInsert, Table: "t", Key: []any{int64(1)}, Position: "p1",
			After: map[string]any{"id": int64(1), "v": "a", "user_ref": int64(99)}} // miss
		close(ingest)
		if err := w.Run(context.Background(), IngestFromChanges(context.Background(), ingest, testSchema())); err != nil {
			t.Fatalf("run: %v", err)
		}
		if len(fc.batches) != 0 {
			t.Fatalf("dropped event reached the sink: %+v", fc.batches)
		}
		if got := w.EnrichDropped("t"); got != 1 {
			t.Fatalf("enrichDropped = %d, want 1", got)
		}
	})
}

// TestWorkerWithoutEnricherUnchanged: the pass-through path with no
// enricher is byte-identical to the pre-enrich behavior (zero cost).
func TestWorkerWithoutEnricherUnchanged(t *testing.T) {
	fc := &fakeCommitter{}
	if err := runWorker(t, Config{MaxRows: 100, MaxInterval: time.Hour}, []string{"t"},
		map[string]sink.TableWriter{"t": fc}, []rowchange.Change{
			chg("t", rowchange.OpInsert, 1, "a", "p1"),
		}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(fc.batches) != 1 || len(fc.batches[0].Changes) != 1 {
		t.Fatalf("baseline changed: %+v", fc.batches)
	}
	if fc.batches[0].Changes[0].After["name"] != nil {
		t.Fatal("unexpected enrichment without an enricher")
	}
}
