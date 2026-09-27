package worker

import (
	"context"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// payloadChunkSource yields n rows with a fresh payload of size bytes each, as
// a SQL driver does: every row's cells are new allocations.
type payloadChunkSource struct{ n, size int }

func (p *payloadChunkSource) PK() []string                            { return []string{"id"} }
func (p *payloadChunkSource) Bounds(context.Context) ([][]any, error) { return nil, nil }
func (p *payloadChunkSource) Scan(_ context.Context, _ source.Chunk, fn func(map[string]any) error) error {
	for i := 0; i < p.n; i++ {
		if err := fn(map[string]any{"id": int64(i), "payload": strings.Repeat("x", p.size)}); err != nil {
			return err
		}
	}
	return nil
}

// liveHeap is the heap's live bytes as of the last GC.
func liveHeap() uint64 {
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}

// A chunk of the full profile's events holds 65 MB of payload, yet the worker's
// heap swung to ~0.9 GB reading it: the whole chunk sat in memory as row maps
// while the Arrow builders grew by doubling over all of it. Reading a chunk
// must hold little more than the chunk's own bytes at any point: the rows are
// encoded in parts as they arrive, and the parts concatenated once — 2.0x at
// peak, from 3.1x when the whole chunk was held as row maps first.
func TestChunkReadHoldsLittleMoreThanTheChunk(t *testing.T) {
	const rows, size = 4000, 6 << 10 // ~24 MiB of payload
	defer debug.SetGCPercent(debug.SetGCPercent(5))

	w := New(Config{})
	regTable(t, w, "dst.t", &fakeCommitter{}, 0)
	w.SetKnownSchema("dst.t", core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "payload", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	})
	x := &chunkExecutor{
		chunkSz:  rows,
		bySource: map[string]*pb.TableAssignment{"src.t": {SourceTable: "src.t", TargetTable: "dst.t", PrimaryKey: []string{"id"}}},
		w:        w,
		send:     func(*pb.WorkerMessage) error { return nil },
		qsrc:     payloadQuerySource{&payloadChunkSource{n: rows, size: size}},
	}

	runtime.GC()
	base := liveHeap()
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if l := liveHeap(); l > peak.Load() {
				peak.Store(l)
			}
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()
	if err := x.run(context.Background(), &pb.ChunkRequest{Table: "src.t", ChunkId: 1}); err != nil {
		t.Fatal(err)
	}
	close(stop)
	<-done

	chunk := uint64(rows * size)
	held := peak.Load() - base
	t.Logf("chunk %d MiB, peak live heap above baseline %d MiB (%.1fx)", chunk>>20, held>>20, float64(held)/float64(chunk))
	if held > chunk*9/4 {
		t.Fatalf("reading a %d MiB chunk held %d MiB live (%.1fx); want at most 2.25x", chunk>>20, held>>20, float64(held)/float64(chunk))
	}
}

type payloadQuerySource struct{ cs source.ChunkSource }

func (q payloadQuerySource) NewChunker(_, _ string, _ int) (source.ChunkSource, error) {
	return q.cs, nil
}
func (q payloadQuerySource) CloseQuery() error { return nil }

// A chunk encoded in several parts is one record with every row, in scan order.
func TestChunkReadInPartsKeepsEveryRowInOrder(t *testing.T) {
	const rows, size = 3000, 4 << 10 // ~12 MiB: three parts
	src := &payloadChunkSource{n: rows, size: size}
	ta := &pb.TableAssignment{SourceTable: "src.t", TargetTable: "dst.t", PrimaryKey: []string{"id"}}
	known := core.Schema{
		Columns: []core.Column{
			{Name: "id", Type: core.ColumnType{Kind: core.KindInt64}},
			{Name: "payload", Type: core.ColumnType{Kind: core.KindString, Nullable: true}},
		},
		PrimaryKey: []string{"id"},
	}
	rec, n, err := scanChunkRecord(context.Background(), src, source.Chunk{}, ta, known)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if n != rows || rec.NumRows() != rows {
		t.Fatalf("rows: counted %d, record %d; want %d", n, rec.NumRows(), rows)
	}
	r, err := transport.NewBatchReader(rec, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range r.NumRows() {
		if got := r.Key(i)[0]; got != int64(i) {
			t.Fatalf("row %d has id %v: out of scan order", i, got)
		}
	}
}
