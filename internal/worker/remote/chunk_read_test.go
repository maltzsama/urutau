package remote

import (
	"context"
	"testing"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/rowchange"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/internal/worker"
	"github.com/maltzsama/urutau/source"
)

// arrowSource reads straight into Arrow unless failArrow, and records which
// path served the chunk.
type arrowSource struct {
	fakeChunkSource
	failArrow   bool
	arrow, maps int
}

func (a *arrowSource) Scan(ctx context.Context, ch source.Chunk, fn func(map[string]any) error) error {
	a.maps++
	return a.fakeChunkSource.Scan(ctx, ch, fn)
}

func (a *arrowSource) ScanArrow(_ context.Context, _ source.Chunk, enc *transport.RowEncoder, _ int) (int, error) {
	if a.failArrow {
		return 0, transport.ErrColumnNotInSchema
	}
	a.arrow++
	for _, r := range a.rows {
		id, _ := enc.Column("id")
		v, _ := enc.Column("v")
		if err := enc.AppendValue(id, r["id"]); err != nil {
			return 0, err
		}
		enc.AppendBytes(v, []byte(r["v"].(string)))
		enc.EndRow(transport.RowMeta{Op: rowchange.OpInsert, Snapshot: true})
	}
	return len(a.rows), nil
}

func readHarness(t *testing.T, src *arrowSource) *chunkExecutor {
	t.Helper()
	w := worker.New(worker.Config{})
	regTable(t, w, "dst.t", &fakeCommitter{}, 0)
	w.SetKnownSchema("dst.t", core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "v", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	})
	src.pk = []string{"id"}
	src.rows = []map[string]any{{"id": int64(1), "v": "aaaa"}, {"id": int64(2), "v": "bbbb"}}
	return &chunkExecutor{chunkSz: 2, w: w}
}

// A source that reads straight into Arrow serves the chunk that way, and the
// table's bytes per row are kept to size its next chunk.
func TestReadChunkUsesTheArrowPath(t *testing.T) {
	src := &arrowSource{}
	x := readHarness(t, src)
	ta := &pb.TableAssignment{SourceTable: "src.t", TargetTable: "dst.t", PrimaryKey: []string{"id"}}
	rec, n, err := x.readChunk(context.Background(), src, source.Chunk{}, ta)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if src.arrow != 1 || src.maps != 0 || n != 2 || rec.NumRows() != 2 {
		t.Fatalf("arrow=%d maps=%d n=%d rows=%d; want the Arrow path, 2 rows", src.arrow, src.maps, n, rec.NumRows())
	}
	if per := x.perRow["dst.t"]; len(per) != 2 || per[1] != 4 {
		t.Fatalf("perRow = %v, want v at 4 bytes per row", per)
	}
}

// A chunk with a column the known schema lacks goes through row maps, which
// widen the schema.
func TestReadChunkFallsBackToRowsForAnUnknownColumn(t *testing.T) {
	src := &arrowSource{failArrow: true}
	x := readHarness(t, src)
	ta := &pb.TableAssignment{SourceTable: "src.t", TargetTable: "dst.t", PrimaryKey: []string{"id"}}
	rec, n, err := x.readChunk(context.Background(), src, source.Chunk{}, ta)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if src.maps != 1 || n != 2 {
		t.Fatalf("maps=%d n=%d; want the row path, 2 rows", src.maps, n)
	}
}
