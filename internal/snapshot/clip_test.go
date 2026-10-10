package snapshot

import (
	"testing"

	"github.com/maltzsama/urutau/internal/partition"
	"github.com/maltzsama/urutau/source"
)

func mustComparePK(t *testing.T, a, b []any) int {
	t.Helper()
	c, err := partition.ComparePK(a, b)
	if err != nil {
		t.Fatalf("ComparePK(%v, %v): %v", a, b, err)
	}
	return c
}

func mustClip(t *testing.T, chunks []source.Chunk, r source.Chunk) []source.Chunk {
	t.Helper()
	out, err := ClipChunksToRange(chunks, r)
	if err != nil {
		t.Fatalf("ClipChunksToRange: %v", err)
	}
	return out
}

func TestClipChunksToRangeUnpartitionedPassesThrough(t *testing.T) {
	chunks := []source.Chunk{{Low: []any{int64(0)}, High: []any{int64(10)}}}
	got := mustClip(t, chunks, source.Chunk{})
	if len(got) != 1 || got[0].Low[0] != int64(0) {
		t.Fatalf("ClipChunksToRange with an empty range should pass chunks through unchanged: %+v", got)
	}
}

func TestClipChunksToRangeDropsOutsideChunks(t *testing.T) {
	chunks := []source.Chunk{
		{Low: nil, High: []any{int64(50)}},
		{Low: []any{int64(50)}, High: []any{int64(100)}},
		{Low: []any{int64(100)}, High: nil},
	}
	// Partition range [50, 100) should keep only the middle chunk.
	got := mustClip(t, chunks, source.Chunk{Low: []any{int64(50)}, High: []any{int64(100)}})
	if len(got) != 1 {
		t.Fatalf("ClipChunksToRange = %+v, want exactly 1 chunk", got)
	}
	if mustComparePK(t, got[0].Low, []any{int64(50)}) != 0 || mustComparePK(t, got[0].High, []any{int64(100)}) != 0 {
		t.Fatalf("clipped chunk = %+v, want [50,100)", got[0])
	}
}

func TestClipChunksToRangeClampsStraddlingChunk(t *testing.T) {
	// One big chunk [0, 1000) straddles a [200, 400) partition range —
	// the clipped chunk must not leak rows outside [200,400).
	chunks := []source.Chunk{{Low: []any{int64(0)}, High: []any{int64(1000)}}}
	got := mustClip(t, chunks, source.Chunk{Low: []any{int64(200)}, High: []any{int64(400)}})
	if len(got) != 1 {
		t.Fatalf("ClipChunksToRange = %+v, want 1 clamped chunk", got)
	}
	if mustComparePK(t, got[0].Low, []any{int64(200)}) != 0 || mustComparePK(t, got[0].High, []any{int64(400)}) != 0 {
		t.Fatalf("clamped chunk = %+v, want [200,400)", got[0])
	}
}

func TestClipChunksToRangeRejectsUnorderedBounds(t *testing.T) {
	chunks := []source.Chunk{{Low: []any{"a"}, High: []any{"z"}}}
	if _, err := ClipChunksToRange(chunks, source.Chunk{Low: []any{int64(50)}}); err == nil {
		t.Fatal("ClipChunksToRange: want an error for string chunk bounds against an int64 partition range")
	}
}
