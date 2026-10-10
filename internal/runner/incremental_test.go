package runner

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/dataplane"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/position"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// sourceStub satisfies source.Source for the incremental tests; no method here
// is exercised by runIncremental.
type sourceStub struct{}

func (sourceStub) Open(context.Context, []source.TableRef) (source.Reader, error) {
	return nil, errors.New("unused")
}
func (sourceStub) InitialPosition(context.Context) (position.Position, error) { return nil, nil }
func (sourceStub) ParsePosition(string) (position.Position, error)            { return nil, nil }
func (sourceStub) Introspect(context.Context, spec.Table) (core.TableRef, core.Schema, []core.Warning, error) {
	return core.TableRef{}, core.Schema{}, nil, nil
}

// bothFake implements BOTH incremental capabilities, so the runner must prefer
// the columnar one and never call the map method.
type bothFake struct {
	sourceStub
	mapCalls, batchCalls int
}

func (f *bothFake) Incremental(context.Context, source.TableRef, string, string) (string, []map[string]any, bool, error) {
	f.mapCalls++
	return "10", []map[string]any{{"id": int64(1), "v": "a"}}, false, nil
}

func (f *bothFake) IncrementalBatch(_ context.Context, t source.TableRef, _ string, _ string, schema core.Schema) (string, *dataplane.Batch, bool, error) {
	f.batchCalls++
	rows := []rowchange.Change{{
		Op: rowchange.OpInsert, Table: t.Target, Key: []any{int64(1)},
		After: map[string]any{"id": int64(1), "v": "a"}, Position: "10", Phase: core.PhaseIncremental,
	}}
	rec, err := transport.RecordFromChanges(rows, transport.MergeSchema(rows, schema), nil)
	if err != nil {
		return "", nil, false, err
	}
	return "10", &dataplane.Batch{Table: t.Target, Record: rec, Mode: dataplane.UpsertMode, Watermark: []byte("10")}, false, nil
}

// mapOnlyFake implements only source.IncrementalSource.
type mapOnlyFake struct {
	sourceStub
	mapCalls int
}

func (f *mapOnlyFake) Incremental(context.Context, source.TableRef, string, string) (string, []map[string]any, bool, error) {
	f.mapCalls++
	return "10", []map[string]any{{"id": int64(1), "v": "a"}}, false, nil
}

func incrementalSchema() core.Schema {
	return core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString}},
		},
		PrimaryKey: []string{"id"},
	}
}

func incrementalRefs() ([]core.TableRef, map[string]spec.Table) {
	ref := core.TableRef{Source: "db.t", Target: "raw.t", PrimaryKey: []string{"id"}}
	specBySource := map[string]spec.Table{"db.t": {Source: "db.t", Target: "raw.t", Cursor: "id"}}
	return []core.TableRef{ref}, specBySource
}

func drainOne(t *testing.T, r *Runner, src source.Source, known core.Schema) {
	t.Helper()
	w := worker.New(worker.Config{MaxRows: 100, MaxInterval: time.Hour})
	if len(known.Columns) > 0 {
		// Register first: SetKnownSchema is a no-op on an unregistered table.
		w.Register("raw.t", nil, dataplane.UpsertMode)
		w.SetKnownSchema("raw.t", known)
	}
	ingest := make(chan worker.Ingest, 4)
	refs, specBySource := incrementalRefs()
	if err := r.runIncremental(context.Background(), src, &fakeSink{}, refs, specBySource, w, ingest); err != nil {
		t.Fatalf("runIncremental: %v", err)
	}
	select {
	case in := <-ingest:
		if in.Batch == nil || in.Batch.Record == nil {
			t.Fatal("the incremental page was not ingested as a batch")
		}
	default:
		t.Fatal("no page was ingested")
	}
}

// The runner must prefer the columnar capability when the source implements it
// and the target schema is known (#733), never touching the map method.
func TestRunIncrementalPrefersBatchCapability(t *testing.T) {
	src := &bothFake{}
	known := incrementalSchema()
	r := &Runner{log: slog.New(slog.DiscardHandler)}

	drainOne(t, r, src, known)

	if src.batchCalls != 1 || src.mapCalls != 0 {
		t.Fatalf("batch calls = %d, map calls = %d; want 1 and 0 (prefer the columnar path)", src.batchCalls, src.mapCalls)
	}
}

// Without a known schema the columnar encoder cannot match the map path, so the
// runner falls back to source.IncrementalSource even when the source implements
// the batch capability.
func TestRunIncrementalFallsBackWithoutSchema(t *testing.T) {
	src := &bothFake{}
	r := &Runner{log: slog.New(slog.DiscardHandler)}

	drainOne(t, r, src, core.Schema{})

	if src.batchCalls != 0 || src.mapCalls != 1 {
		t.Fatalf("batch calls = %d, map calls = %d; want 0 and 1 (fall back on an unknown schema)", src.batchCalls, src.mapCalls)
	}
}

// A source that implements only IncrementalSource keeps working unchanged.
func TestRunIncrementalMapOnlySource(t *testing.T) {
	src := &mapOnlyFake{}
	r := &Runner{log: slog.New(slog.DiscardHandler)}

	drainOne(t, r, src, incrementalSchema())

	if src.mapCalls != 1 {
		t.Fatalf("map calls = %d, want 1", src.mapCalls)
	}
}
