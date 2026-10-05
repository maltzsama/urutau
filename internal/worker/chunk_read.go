package worker

import (
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/maltzsama/urutau/core"
	"github.com/maltzsama/urutau/internal/transport"
	pb "github.com/maltzsama/urutau/internal/transport/pb/urutau/v1"
	"github.com/maltzsama/urutau/source"
)

// arrowChunkScanner is a chunk source that reads a chunk straight into
// Arrow (the MySQL chunker): no row maps, the chunk held once (#448).
type arrowChunkScanner interface {
	ScanArrow(ctx context.Context, ch source.Chunk, enc *transport.RowEncoder, expected int) (int, error)
}

// byteCapScanner is a chunk source that reads a chunk in byte-capped pages
// (the MySQL and Postgres chunkers): each page is at most maxBytes, cut at the
// row that crosses the cap. emit is called per page with the page's record and
// row count; the caller owns each record.
type byteCapScanner interface {
	ScanArrowPages(ctx context.Context, ch source.Chunk, enc *transport.RowEncoder, maxBytes int, emit func(rec arrow.RecordBatch, n int) error) (int, error)
}

// readChunkPages reads one chunk in byte-capped pages, calling emit per page.
func (x *chunkExecutor) readChunkPages(ctx context.Context, s byteCapScanner, ch source.Chunk, ta *pb.TableAssignment, emit func(rec arrow.RecordBatch, n int) error) (int, error) {
	enc, err := transport.NewRowEncoder(x.w.KnownSchema(ta.TargetTable), nil)
	if err != nil {
		return 0, err
	}
	defer enc.Release()
	return s.ScanArrowPages(ctx, ch, enc, snapshotWindowBytes(), emit)
}

// readChunk reads one chunk into a record and returns it with its row count.
// A source that reads straight into Arrow does, into buffers sized from the
// table's previous chunk; any other source, or a chunk with a column the
// known schema lacks, goes through row maps (scanChunkRecord).
func (x *chunkExecutor) readChunk(ctx context.Context, chunker source.ChunkSource, ch source.Chunk, ta *pb.TableAssignment) (arrow.RecordBatch, int, error) {
	known := x.w.KnownSchema(ta.TargetTable)
	if s, ok := chunker.(arrowChunkScanner); ok && len(known.Columns) > 0 {
		rec, n, err := x.scanArrow(ctx, s, ch, ta.TargetTable, known)
		if !errors.Is(err, transport.ErrColumnNotInSchema) {
			return rec, n, err
		}
	}
	return scanChunkRecord(ctx, chunker, ch, ta, known)
}

func (x *chunkExecutor) scanArrow(ctx context.Context, s arrowChunkScanner, ch source.Chunk, target string, known core.Schema) (arrow.RecordBatch, int, error) {
	enc, err := transport.NewRowEncoder(known, nil)
	if err != nil {
		return nil, 0, err
	}
	defer enc.Release()
	enc.ReservePerRow(x.chunkSz, x.perRow[target])
	n, err := s.ScanArrow(ctx, ch, enc, x.chunkSz)
	if err != nil {
		return nil, 0, err
	}
	if x.perRow == nil {
		x.perRow = map[string][]int{}
	}
	x.perRow[target] = enc.PerRow()
	return enc.NewRecord(), n, nil
}
