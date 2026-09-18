package worker

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
	"github.com/maltzsama/urutau/spec"
)

// fakeChunkSource returns a fixed set of rows for any chunk.
type fakeChunkSource struct {
	pk   []string
	rows []map[string]any
}

func (f *fakeChunkSource) PK() []string { return f.pk }
func (f *fakeChunkSource) Bounds(context.Context) ([][]any, error) {
	return nil, nil
}
func (f *fakeChunkSource) Scan(_ context.Context, _ source.Chunk, fn func(map[string]any) error) error {
	for _, r := range f.rows {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

type fakeQuerySource struct{ cs *fakeChunkSource }

func (q *fakeQuerySource) NewChunker(_, _ string, _ int) (source.ChunkSource, error) {
	return q.cs, nil
}
func (q *fakeQuerySource) CloseQuery() error { return nil }

// TestChunkExecutorEmitsWireBatch — the chunk executor scans rows and hands
// AddWindowRows a valid wire-schema batch built directly (no bridge). The
// stored window batch decodes back to the scanned rows with Snapshot set.
func TestChunkExecutorEmitsWireBatch(t *testing.T) {
	w := New(Config{})
	fc := &fakeCommitter{}
	regTable(t, w, "dst.t", fc, 0)
	w.SetKnownSchema("dst.t", core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	})

	var sent []*pb.WorkerMessage
	x := &chunkExecutor{
		chunkSz:  10,
		bySource: map[string]*pb.TableAssignment{"src.t": {SourceTable: "src.t", TargetTable: "dst.t", PrimaryKey: []string{"id"}}},
		w:        w,
		send:     func(m *pb.WorkerMessage) error { sent = append(sent, m); return nil },
		qsrc: &fakeQuerySource{cs: &fakeChunkSource{
			pk: []string{"id"},
			rows: []map[string]any{
				{"id": int64(1), "v": "a"},
				{"id": int64(2), "v": "b"},
			},
		}},
	}

	if err := x.run(context.Background(), &pb.ChunkRequest{Table: "src.t", ChunkId: 3}); err != nil {
		t.Fatalf("run: %v", err)
	}

	if len(sent) != 1 || sent[0].GetChunkReady() == nil {
		t.Fatalf("expected one ChunkReady, got %+v", sent)
	}
	if got := sent[0].GetChunkReady().Rows; got != 2 {
		t.Fatalf("ChunkReady.Rows = %d, want 2", got)
	}

	p := w.tables["dst.t"]
	p.winMu.Lock()
	win := p.windows[3]
	p.winMu.Unlock()
	if win == nil {
		t.Fatal("chunk 3 window not stored")
	}
	t.Cleanup(func() { win.batch.Release() })
	br, err := transport.NewBatchReader(win.batch.Record, []string{"id"})
	if err != nil {
		t.Fatalf("stored window batch is not wire schema: %v", err)
	}
	if br.NumRows() != 2 {
		t.Fatalf("window rows = %d, want 2", br.NumRows())
	}
	for i := 0; i < br.NumRows(); i++ {
		if !br.Snapshot(i) {
			t.Fatalf("row %d: Snapshot not set on a chunk row", i)
		}
	}
	if v, _ := br.Value("v", 0); v != "a" {
		t.Fatalf("row 0 v = %v, want a", v)
	}
}

func TestSpecTablesFromAssignment(t *testing.T) {
	filterJSON := []byte(`{"where":{"col":"active","op":"eq","value":true}}`)
	bySource := map[string]*pb.TableAssignment{
		"public.orders": {
			SourceTable:  "public.orders",
			ColumnFilter: []string{"id", "v"},
			Filter:       filterJSON,
		},
		"public.plain": {SourceTable: "public.plain"},
	}
	tables, err := specTablesFromAssignment(bySource)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 {
		t.Fatalf("got %d tables, want 2", len(tables))
	}
	var orders *spec.Table
	for i := range tables {
		if tables[i].Source == "public.orders" {
			orders = &tables[i]
		}
	}
	if orders == nil {
		t.Fatal("public.orders not reconstructed")
	}
	if len(orders.ColumnFilter) != 2 || orders.ColumnFilter[0] != "id" {
		t.Fatalf("ColumnFilter = %v", orders.ColumnFilter)
	}
	if orders.Filter == nil || orders.Filter.Predicate == nil || orders.Filter.Predicate.Column != "active" {
		t.Fatalf("Filter = %+v", orders.Filter)
	}
}

func TestSpecTablesFromAssignmentBadFilter(t *testing.T) {
	_, err := specTablesFromAssignment(map[string]*pb.TableAssignment{
		"public.orders": {SourceTable: "public.orders", Filter: []byte("not json")},
	})
	if err == nil {
		t.Fatal("want an error for malformed filter JSON")
	}
}
