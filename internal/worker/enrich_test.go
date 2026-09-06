package worker

import (
	"context"
	"testing"
	"time"

	"github.com/maltzsama/urutau/change"
	"github.com/maltzsama/urutau/internal/enrich"
	"github.com/maltzsama/urutau/sink"
	"github.com/maltzsama/urutau/spec"
)

// staticLoader satisfies enrich.Loader without a database.
type staticLoader struct{ rows []map[string]any }

func (l *staticLoader) Load(context.Context) ([]map[string]any, error) { return l.rows, nil }
func (l *staticLoader) Close() error                                   { return nil }

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
		if err := s.UseLoader("users", &staticLoader{rows: []map[string]any{{"id": int64(7), "name": "ana"}}}); err != nil {
			t.Fatalf("UseLoader: %v", err)
		}
		s.Start(context.Background())
		t.Cleanup(s.Stop)
		// Warm: wait until the join answers.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			out, _ := s.Enrich([]change.Change{{Op: change.OpInsert, After: map[string]any{"user_ref": int64(7)}}})
			if len(out) == 1 && out[0].After["name"] == "ana" {
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
		w.RegisterCommitter("t", fc, change.UpsertMode)
		w.SetEnricher("t", st)
		ingest := make(chan change.Change, 2)
		ingest <- change.Change{Op: change.OpInsert, Table: "t", Key: []any{int64(1)}, Position: "p1",
			After: map[string]any{"id": int64(1), "v": "a", "user_ref": int64(7)}}
		close(ingest)
		if err := w.Run(context.Background(), ingest); err != nil {
			t.Fatalf("run: %v", err)
		}
		if len(fc.batches) != 1 || len(fc.batches[0].Upserts) != 1 {
			t.Fatalf("batches: %+v", fc.batches)
		}
		row := fc.batches[0].Upserts[0]
		if row.After["name"] != "ana" {
			t.Fatalf("row not enriched: %v", row.After)
		}
	})

	t.Run("inner miss drops and counts", func(t *testing.T) {
		_, st := build(t, "inner")
		fc := &fakeCommitter{}
		w := New(Config{MaxRows: 100, MaxInterval: time.Hour})
		w.RegisterCommitter("t", fc, change.UpsertMode)
		w.SetEnricher("t", st)
		ingest := make(chan change.Change, 2)
		ingest <- change.Change{Op: change.OpInsert, Table: "t", Key: []any{int64(1)}, Position: "p1",
			After: map[string]any{"id": int64(1), "v": "a", "user_ref": int64(99)}} // miss
		close(ingest)
		if err := w.Run(context.Background(), ingest); err != nil {
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
		map[string]sink.TableWriter{"t": fc}, []change.Change{
			chg("t", change.OpInsert, 1, "a", "p1"),
		}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(fc.batches) != 1 || len(fc.batches[0].Upserts) != 1 {
		t.Fatalf("baseline changed: %+v", fc.batches)
	}
	if fc.batches[0].Upserts[0].After["name"] != nil {
		t.Fatal("unexpected enrichment without an enricher")
	}
}
